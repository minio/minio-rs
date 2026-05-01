// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, KeyInit};
use argon2::{Argon2, Params, Version};
use rand::RngCore;

use crate::s3::error::{Error, ValidationErr};

const ARGON2ID_AES_GCM: u8 = 0x00;

pub fn decrypt_data(password: &str, encrypted_data: &[u8]) -> Result<Vec<u8>, Error> {
    // sio-go format: [32 bytes salt][1 byte algorithm][8 bytes nonce][encrypted fragments...]
    // Each fragment: [ciphertext + 16 byte auth tag]
    // Nonce for each fragment: base_nonce with last 4 bytes = sequence number (little-endian)
    // Associated data: [0x00] for regular fragments, [0x80] for final fragment
    // Default buffer size: 16384 bytes plaintext per fragment

    if encrypted_data.len() < 41 {
        return Err(Error::Validation(ValidationErr::StrError {
            message: "Encrypted data too short".to_string(),
            source: None,
        }));
    }

    // Extract salt (first 32 bytes)
    let salt = &encrypted_data[0..32];

    // Check algorithm byte (byte 32)
    if encrypted_data[32] != ARGON2ID_AES_GCM {
        return Err(Error::Validation(ValidationErr::StrError {
            message: format!("Unsupported encryption algorithm: {}", encrypted_data[32]),
            source: None,
        }));
    }

    // Extract base nonce (bytes 33-40)
    let nonce_8 = &encrypted_data[33..41];

    // Derive key from password using Argon2
    let params = Params::new(65536, 1, 4, Some(32)).map_err(|e| {
        Error::Validation(ValidationErr::StrError {
            message: format!("Failed to create Argon2 params: {e}"),
            source: None,
        })
    })?;

    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, Version::V0x13, params);

    let mut key_bytes = [0u8; 32];
    argon2
        .hash_password_into(password.as_bytes(), salt, &mut key_bytes)
        .map_err(|e| {
            Error::Validation(ValidationErr::StrError {
                message: format!("Failed to derive key: {e}"),
                source: None,
            })
        })?;

    let cipher = Aes256Gcm::new_from_slice(&key_bytes).map_err(|e| {
        Error::Validation(ValidationErr::StrError {
            message: format!("Failed to create cipher: {e}"),
            source: None,
        })
    })?;

    // Process encrypted fragments (starting at byte 41)
    // Each fragment is up to BUF_SIZE (16384) bytes plaintext + 16 bytes tag
    const BUF_SIZE: usize = 16384;
    const TAG_SIZE: usize = 16;
    const MAX_FRAGMENT_SIZE: usize = BUF_SIZE + TAG_SIZE;

    let encrypted_fragments = &encrypted_data[41..];
    let mut decrypted = Vec::new();
    let mut offset = 0;

    // Initialize AAD: sio-go creates a 17-byte buffer with first byte 0x00 and
    // bytes 1-16 filled with the result of encrypting nil with nil AAD using nonce with seqNum=0
    let mut aad_buffer = vec![0u8; 1 + TAG_SIZE];
    aad_buffer[0] = 0x00;

    // Compute the tag for AAD initialization: encrypt empty data with empty AAD using first nonce (seqNum=0)
    let mut init_nonce = [0u8; 12];
    init_nonce[0..8].copy_from_slice(nonce_8);
    init_nonce[8..12].copy_from_slice(&0u32.to_le_bytes());
    let init_nonce_ref = &init_nonce.into();

    use aes_gcm::aead::Payload;
    let init_tag = cipher
        .encrypt(init_nonce_ref, Payload { msg: &[], aad: &[] })
        .map_err(|e| {
            Error::Validation(ValidationErr::StrError {
                message: format!("Failed to initialize AAD: {e}"),
                source: None,
            })
        })?;
    aad_buffer[1..17].copy_from_slice(&init_tag);

    // After initialization, sio-go sets seqNum = 1, so first data fragment uses seqNum=1
    let mut sequence_num: u32 = 1;

    while offset < encrypted_fragments.len() {
        // Determine fragment size (last fragment may be smaller)
        let remaining = encrypted_fragments.len() - offset;
        let fragment_size = remaining.min(MAX_FRAGMENT_SIZE);

        if fragment_size < TAG_SIZE {
            return Err(Error::Validation(ValidationErr::StrError {
                message: format!("Fragment too small: {fragment_size} bytes"),
                source: None,
            }));
        }

        let ciphertext_and_tag = &encrypted_fragments[offset..offset + fragment_size];
        let is_final = offset + fragment_size >= encrypted_fragments.len();

        // Construct nonce: base_nonce (8 bytes) + 4 zero bytes + sequence (4 bytes little-endian)
        let mut gcm_nonce_12 = [0u8; 12];
        gcm_nonce_12[0..8].copy_from_slice(nonce_8);
        gcm_nonce_12[8..12].copy_from_slice(&sequence_num.to_le_bytes());

        let nonce = &gcm_nonce_12.into();

        // Update AAD for final fragment
        if is_final {
            aad_buffer[0] = 0x80;
        }

        // Decrypt with AAD
        let payload = Payload {
            msg: ciphertext_and_tag,
            aad: &aad_buffer,
        };

        let plaintext = cipher.decrypt(nonce, payload).map_err(|e| {
            Error::Validation(ValidationErr::StrError {
                message: format!("Failed to decrypt fragment {sequence_num}: {e}"),
                source: None,
            })
        })?;

        decrypted.extend_from_slice(&plaintext);

        offset += fragment_size;
        sequence_num += 1;
    }

    Ok(decrypted)
}

pub fn encrypt_data(password: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
    // sio-go format: [32 bytes salt][1 byte algorithm][8 bytes nonce][encrypted fragments...]
    // Each fragment: [ciphertext + 16 byte auth tag]
    const BUF_SIZE: usize = 16384;

    let mut salt = [0u8; 32];
    rand::rng().fill_bytes(&mut salt);

    let params = Params::new(65536, 1, 4, Some(32)).map_err(|e| {
        Error::Validation(ValidationErr::StrError {
            message: format!("Failed to create Argon2 params: {e}"),
            source: None,
        })
    })?;

    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, Version::V0x13, params);

    let mut key_bytes = [0u8; 32];
    argon2
        .hash_password_into(password.as_bytes(), &salt, &mut key_bytes)
        .map_err(|e| {
            Error::Validation(ValidationErr::StrError {
                message: format!("Failed to derive key: {e}"),
                source: None,
            })
        })?;

    let cipher = Aes256Gcm::new_from_slice(&key_bytes).map_err(|e| {
        Error::Validation(ValidationErr::StrError {
            message: format!("Failed to create cipher: {e}"),
            source: None,
        })
    })?;

    let mut nonce_8 = [0u8; 8];
    rand::rng().fill_bytes(&mut nonce_8);

    // Initialize AAD buffer like sio-go does
    const TAG_SIZE: usize = 16;
    let mut aad_buffer = vec![0u8; 1 + TAG_SIZE];
    aad_buffer[0] = 0x00;

    // Compute init tag with seqNum=0
    let mut init_nonce = [0u8; 12];
    init_nonce[0..8].copy_from_slice(&nonce_8);
    init_nonce[8..12].copy_from_slice(&0u32.to_le_bytes());
    let init_nonce_ref = &init_nonce.into();

    use aes_gcm::aead::Payload;
    let init_tag = cipher
        .encrypt(init_nonce_ref, Payload { msg: &[], aad: &[] })
        .map_err(|e| {
            Error::Validation(ValidationErr::StrError {
                message: format!("Failed to initialize AAD: {e}"),
                source: None,
            })
        })?;
    aad_buffer[1..17].copy_from_slice(&init_tag);

    let mut encrypted_fragments = Vec::new();
    let mut sequence_num: u32 = 1; // Start at 1 like sio-go

    // Split data into chunks and encrypt each
    let chunks: Vec<&[u8]> = data.chunks(BUF_SIZE).collect();
    let total_chunks = chunks.len();

    for (i, chunk) in chunks.into_iter().enumerate() {
        let is_final = i == total_chunks - 1;

        // Construct nonce: base_nonce (8 bytes) + 4 zero bytes + sequence (4 bytes little-endian)
        let mut gcm_nonce_12 = [0u8; 12];
        gcm_nonce_12[0..8].copy_from_slice(&nonce_8);
        gcm_nonce_12[8..12].copy_from_slice(&sequence_num.to_le_bytes());

        let nonce = &gcm_nonce_12.into();

        // Update AAD for final fragment
        if is_final {
            aad_buffer[0] = 0x80;
        }

        let payload = Payload {
            msg: chunk,
            aad: &aad_buffer,
        };

        let ciphertext_and_tag = cipher.encrypt(nonce, payload).map_err(|e| {
            Error::Validation(ValidationErr::StrError {
                message: format!("Failed to encrypt fragment {sequence_num}: {e}"),
                source: None,
            })
        })?;

        encrypted_fragments.extend_from_slice(&ciphertext_and_tag);
        sequence_num += 1;
    }

    let mut result = Vec::with_capacity(32 + 1 + 8 + encrypted_fragments.len());
    result.extend_from_slice(&salt);
    result.push(ARGON2ID_AES_GCM);
    result.extend_from_slice(&nonce_8);
    result.extend_from_slice(&encrypted_fragments);

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_data_format() {
        let password = "test-password";
        let data = b"Hello, MinIO!";

        let encrypted = encrypt_data(password, data).unwrap();

        assert!(encrypted.len() > 41);
        assert_eq!(encrypted[32], ARGON2ID_AES_GCM);

        let salt = &encrypted[0..32];
        assert_eq!(salt.len(), 32);

        let nonce = &encrypted[33..41];
        assert_eq!(nonce.len(), 8);
    }

    #[test]
    fn test_encrypt_data_different_salts() {
        let password = "test-password";
        let data = b"test data";

        let encrypted1 = encrypt_data(password, data).unwrap();
        let encrypted2 = encrypt_data(password, data).unwrap();

        assert_ne!(encrypted1, encrypted2);

        let salt1 = &encrypted1[0..32];
        let salt2 = &encrypted2[0..32];
        assert_ne!(salt1, salt2);
    }

    #[test]
    fn test_encrypt_empty_data() {
        let password = "test-password";
        let data = b"";

        let encrypted = encrypt_data(password, data).unwrap();
        // Empty data has no DARE packets, just: 32 (salt) + 1 (version) + 8 (nonce) = 41 bytes
        assert_eq!(encrypted.len(), 41);
        assert_eq!(encrypted[32], ARGON2ID_AES_GCM);
    }

    #[test]
    fn test_decrypt_data() {
        let password = "test-password";
        let original_data = b"Hello, MinIO Admin API!";

        let encrypted = encrypt_data(password, original_data).unwrap();
        let decrypted = decrypt_data(password, &encrypted).unwrap();

        assert_eq!(decrypted, original_data);
    }

    #[test]
    fn test_decrypt_wrong_password() {
        let password = "correct-password";
        let wrong_password = "wrong-password";
        let data = b"Secret data";

        let encrypted = encrypt_data(password, data).unwrap();
        let result = decrypt_data(wrong_password, &encrypted);

        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_empty_data() {
        let password = "test-password";
        let data = b"";

        let encrypted = encrypt_data(password, data).unwrap();
        let decrypted = decrypt_data(password, &encrypted).unwrap();

        assert_eq!(decrypted, data);
    }

    #[test]
    fn test_decrypt_large_data() {
        let password = "test-password";
        let large_data = vec![b'X'; 100000];

        let encrypted = encrypt_data(password, &large_data).unwrap();
        let decrypted = decrypt_data(password, &encrypted).unwrap();

        assert_eq!(decrypted, large_data);
    }

    #[test]
    fn test_decrypt_invalid_data() {
        let password = "test-password";
        let invalid_data = b"too short";

        let result = decrypt_data(password, invalid_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_data_minimum_length() {
        let password = "test-password";
        let mut data = vec![0u8; 41];
        data[32] = ARGON2ID_AES_GCM;

        let result = decrypt_data(password, &data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_decrypt_data_less_than_minimum() {
        let password = "test-password";
        let data = vec![0u8; 40];

        let result = decrypt_data(password, &data);
        assert!(result.is_err());
        if let Err(Error::Validation(ValidationErr::StrError { message, .. })) = result {
            assert!(message.contains("too short"));
        } else {
            panic!("Expected validation error");
        }
    }

    #[test]
    fn test_decrypt_unsupported_algorithm() {
        let password = "test-password";
        let mut data = vec![0u8; 41];
        data[32] = 0xFF;

        let result = decrypt_data(password, &data);
        assert!(result.is_err());
        if let Err(Error::Validation(ValidationErr::StrError { message, .. })) = result {
            assert!(message.contains("Unsupported encryption algorithm"));
        } else {
            panic!("Expected validation error");
        }
    }

    #[test]
    fn test_decrypt_corrupted_fragment() {
        let password = "test-password";
        let data = b"Test data for corruption";

        let mut encrypted = encrypt_data(password, data).unwrap();

        if encrypted.len() > 50 {
            encrypted[50] ^= 0xFF;
        }

        let result = decrypt_data(password, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_encrypt_decrypt_boundary_sizes() {
        let password = "test-password";

        let sizes = vec![1, 100, 1024, 16383, 16384, 16385, 32768, 49152];

        for size in sizes {
            let data = vec![b'A'; size];
            let encrypted = encrypt_data(password, &data).unwrap();
            let decrypted = decrypt_data(password, &encrypted).unwrap();
            assert_eq!(decrypted, data, "Failed for size {}", size);
        }
    }

    #[test]
    fn test_decrypt_fragment_too_small() {
        let password = "test-password";
        let mut data = vec![0u8; 56];
        data[32] = ARGON2ID_AES_GCM;

        let result = decrypt_data(password, &data);
        assert!(result.is_err());
        if let Err(Error::Validation(ValidationErr::StrError { message, .. })) = result {
            assert!(message.contains("Fragment too small"));
        } else {
            panic!("Expected fragment too small error");
        }
    }

    #[test]
    fn test_encrypt_different_passwords() {
        let password1 = "password1";
        let password2 = "password2";
        let data = b"test data";

        let encrypted1 = encrypt_data(password1, data).unwrap();
        let encrypted2 = encrypt_data(password2, data).unwrap();

        assert_ne!(encrypted1, encrypted2);

        let decrypted1 = decrypt_data(password1, &encrypted1).unwrap();
        let decrypted2 = decrypt_data(password2, &encrypted2).unwrap();

        assert_eq!(decrypted1, data);
        assert_eq!(decrypted2, data);
    }

    #[test]
    fn test_encrypt_special_characters() {
        let password = "test!@#$%^&*()";
        let data = "Special chars: 你好世界 🌍 emoji".as_bytes();

        let encrypted = encrypt_data(password, data).unwrap();
        let decrypted = decrypt_data(password, &encrypted).unwrap();

        assert_eq!(decrypted, data);
    }
}
