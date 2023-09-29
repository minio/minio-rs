use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use futures_util::stream;
use http::Method;

use crate::s3::{
    args::ListenBucketNotificationArgs,
    error::Error,
    response::ListenBucketNotificationResponse,
    types::NotificationRecords,
    utils::{merge, Multimap},
};

use super::Client;

impl Client {
    /// Listens for bucket notifications. This is MinIO extension API. This
    /// function returns a tuple of `ListenBucketNotificationResponse` and a
    /// stream of `NotificationRecords`. The former contains the HTTP headers
    /// returned by the server and the latter is a stream of notification
    /// records. In normal operation (when there are no errors), the stream
    /// never ends.
    pub async fn listen_bucket_notification(
        &self,
        args: ListenBucketNotificationArgs,
    ) -> Result<
        (
            ListenBucketNotificationResponse,
            impl Stream<Item = Result<NotificationRecords, Error>>,
        ),
        Error,
    > {
        if self.base_url.is_aws_host() {
            return Err(Error::UnsupportedApi(String::from(
                "ListenBucketNotification",
            )));
        }

        let region = self
            .get_region(&args.bucket, args.region.as_deref())
            .await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = args.prefix {
            query_params.insert(String::from("prefix"), v.to_string());
        }
        if let Some(v) = args.suffix {
            query_params.insert(String::from("suffix"), v.to_string());
        }
        if let Some(v) = &args.events {
            for e in v.iter() {
                query_params.insert(String::from("events"), e.to_string());
            }
        } else {
            query_params.insert(String::from("events"), String::from("s3:ObjectCreated:*"));
            query_params.insert(String::from("events"), String::from("s3:ObjectRemoved:*"));
            query_params.insert(String::from("events"), String::from("s3:ObjectAccessed:*"));
        }

        let resp = self
            .execute(
                Method::GET,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                None,
            )
            .await?;

        let header_map = resp.headers().clone();

        let line = BytesMut::with_capacity(16 * 1024);
        let lines: VecDeque<Bytes> = VecDeque::new();

        // We use a stream::unfold to process the response body. The unfold
        // state consists of the current (possibly incomplete) line , a deque of
        // (complete) lines extracted from the response body and the response
        // itself wrapped in an Option (the Option is to indicate if the
        // response body has been fully consumed). The unfold operation here
        // generates a stream of notification records.
        let record_stream = Box::pin(stream::unfold(
            (line, lines, Some(resp)),
            move |(mut line, mut lines, mut resp_opt)| async move {
                loop {
                    // 1. If we have some lines in the deque, deserialize and return them.
                    while let Some(v) = lines.pop_front() {
                        let s = match String::from_utf8((&v).to_vec()) {
                            Err(e) => return Some((Err(e.into()), (line, lines, resp_opt))),
                            Ok(s) => {
                                let s = s.trim().to_string();
                                // Skip empty strings.
                                if s.is_empty() {
                                    continue;
                                }
                                s
                            }
                        };
                        let records_res: Result<NotificationRecords, Error> =
                            serde_json::from_str(&s).map_err(|e| e.into());
                        return Some((records_res, (line, lines, resp_opt)));
                    }

                    // At this point `lines` is empty. We may have a partial line in
                    // `line`. We now process the next chunk in the response body.

                    if resp_opt.is_none() {
                        if line.len() > 0 {
                            // Since we have no more chunks to process, we
                            // consider this as a complete line and deserialize
                            // it in the next loop iteration.
                            lines.push_back(line.freeze());
                            line = BytesMut::with_capacity(16 * 1024);
                            continue;
                        }
                        // We have no more chunks to process, no partial line
                        // and no more lines to return. So we are done.
                        return None;
                    }

                    // Attempt to read the next chunk of the response.
                    let next_chunk_res = resp_opt.as_mut().map(|r| r.chunk()).unwrap().await;
                    let mut done = false;
                    let chunk = match next_chunk_res {
                        Err(e) => return Some((Err(e.into()), (line, lines, None))),
                        Ok(Some(chunk)) => chunk,
                        Ok(None) => {
                            done = true;
                            Bytes::new()
                        }
                    };

                    // Now we process the chunk. The `.split()` splits the chunk
                    // around each newline character.
                    //
                    // For e.g. "\nab\nc\n\n" becomes ["", "ab", "c", "", ""].
                    //
                    // This means that a newline was found in the chunk only
                    // when `.split()` returns at least 2 elements. The main
                    // tricky situation is when a line is split across chunks.
                    // We use the length of `lines_in_chunk` to determine if
                    // this is the case.
                    let lines_in_chunk = chunk.split(|&v| v == b'\n').collect::<Vec<_>>();

                    if lines_in_chunk.len() == 1 {
                        // No newline found in the chunk. So we just append the
                        // chunk to the current line and continue to the next
                        // chunk.
                        line.extend_from_slice(&chunk);
                        continue;
                    }

                    // At least one newline was found in the chunk.
                    for (i, chunk_line) in lines_in_chunk.iter().enumerate() {
                        if i == 0 {
                            // The first split component in the chunk completes
                            // the line.
                            line.extend_from_slice(chunk_line);
                            lines.push_back(line.freeze());
                            line = BytesMut::with_capacity(16 * 1024);
                            continue;
                        }
                        if i == lines_in_chunk.len() - 1 {
                            // The last split component in the chunk is a
                            // partial line. We append it to the current line
                            // (which will be empty because we just re-created
                            // it).
                            line.extend_from_slice(chunk_line);
                            continue;
                        }

                        lines.push_back(Bytes::copy_from_slice(chunk_line));
                    }

                    if done {
                        lines.push_back(line.freeze());
                        line = BytesMut::with_capacity(16 * 1024);
                        resp_opt = None;
                    }
                }
            },
        ));

        Ok((
            ListenBucketNotificationResponse::new(header_map, &region, &args.bucket),
            record_stream,
        ))
    }
}
