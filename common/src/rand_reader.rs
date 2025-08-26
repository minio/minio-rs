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

use futures_io::AsyncRead;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

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
        let bytes_read = buf.len().min(self.size as usize);

        if bytes_read > 0 {
            let random: &mut dyn rand::RngCore = &mut rand::rng();
            random.fill_bytes(&mut buf[0..bytes_read]);
        }

        self.size -= bytes_read as u64;

        Ok(bytes_read)
    }
}

impl AsyncRead for RandReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let bytes_read = buf.len().min(self.size as usize);

        if bytes_read > 0 {
            let random: &mut dyn rand::RngCore = &mut rand::rng();
            random.fill_bytes(&mut buf[0..bytes_read]);
        }

        self.get_mut().size -= bytes_read as u64;

        Poll::Ready(Ok(bytes_read))
    }
}
