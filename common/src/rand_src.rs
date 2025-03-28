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

use async_std::task;
use bytes::Bytes;
use rand::SeedableRng;
use rand::prelude::SmallRng;
use std::io;
use tokio::io::AsyncRead;
use tokio_stream::Stream;

pub struct RandSrc {
    size: u64,
    rng: SmallRng,
}

impl RandSrc {
    #[allow(dead_code)]
    pub fn new(size: u64) -> RandSrc {
        let rng = SmallRng::from_entropy();
        RandSrc { size, rng }
    }
}

impl Stream for RandSrc {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        if self.size == 0 {
            return task::Poll::Ready(None);
        }

        let bytes_read = match self.size > 64 * 1024 {
            true => 64 * 1024,
            false => self.size as usize,
        };

        let this = self.get_mut();

        let mut buf = vec![0; bytes_read];
        let random: &mut dyn rand::RngCore = &mut this.rng;
        random.fill_bytes(&mut buf);

        this.size -= bytes_read as u64;

        task::Poll::Ready(Some(Ok(Bytes::from(buf))))
    }
}

impl AsyncRead for RandSrc {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
        read_buf: &mut tokio::io::ReadBuf<'_>,
    ) -> task::Poll<io::Result<()>> {
        let buf = read_buf.initialize_unfilled();
        let bytes_read = match self.size > (buf.len() as u64) {
            true => buf.len(),
            false => self.size as usize,
        };

        let this = self.get_mut();

        if bytes_read > 0 {
            let random: &mut dyn rand::RngCore = &mut this.rng;
            random.fill_bytes(&mut buf[0..bytes_read]);
        }

        this.size -= bytes_read as u64;

        read_buf.advance(bytes_read);
        task::Poll::Ready(Ok(()))
    }
}
