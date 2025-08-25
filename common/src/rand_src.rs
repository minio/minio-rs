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

use async_std::stream::Stream;
use bytes::Bytes;
use futures_io::AsyncRead;
use rand::prelude::SmallRng;
use rand::{RngCore, SeedableRng};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct RandSrc {
    size: u64,
    rng: SmallRng,
}

impl RandSrc {
    #[allow(dead_code)]
    pub fn new(size: u64) -> RandSrc {
        let rng: SmallRng = SmallRng::from_os_rng();
        RandSrc { size, rng }
    }
}

impl Stream for RandSrc {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.size == 0 {
            return Poll::Ready(None);
        }
        // Limit to 8 KiB per read
        let bytes_read = self.size.min(8 * 1024) as usize;

        let this = self.get_mut();

        let mut buf = vec![0; bytes_read];
        let random: &mut dyn rand::RngCore = &mut this.rng;
        random.fill_bytes(&mut buf);
        this.size -= bytes_read as u64;
        Poll::Ready(Some(Ok(Bytes::from(buf))))
    }
}

impl AsyncRead for RandSrc {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();

        if this.size == 0 {
            return Poll::Ready(Ok(0)); // EOF
        }

        let to_read = std::cmp::min(this.size as usize, buf.len());

        this.rng.fill_bytes(&mut buf[..to_read]);
        this.size -= to_read as u64;

        Poll::Ready(Ok(to_read))
    }
}
