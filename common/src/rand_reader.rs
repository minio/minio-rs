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

use std::io;

pub struct RandReader {
    size: u64,
}

impl RandReader {
    #[allow(dead_code)]
    pub fn new(size: u64) -> RandReader {
        RandReader { size }
    }
}

impl io::Read for RandReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let bytes_read: usize = match (self.size as usize) > buf.len() {
            true => buf.len(),
            false => self.size as usize,
        };

        if bytes_read > 0 {
            let random: &mut dyn rand::RngCore = &mut rand::thread_rng();
            random.fill_bytes(&mut buf[0..bytes_read]);
        }

        self.size -= bytes_read as u64;

        Ok(bytes_read)
    }
}
