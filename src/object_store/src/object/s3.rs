// Copyright 2024 RisingWave Labs
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

use std::cmp;
use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError};
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, CompletedMultipartUpload,
    CompletedPart, Delete, ExpirationStatus, LifecycleRule, LifecycleRuleFilter, ObjectIdentifier,
};
use aws_sdk_s3::Client;
use aws_smithy_http::futures_stream_adapter::FuturesStreamCompatByteStream;
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use aws_smithy_runtime_api::client::http::HttpClient;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use aws_smithy_types::retry::RetryConfig;
use either::Either;
use fail::fail_point;
use futures::future::{try_join_all, BoxFuture, FutureExt};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use hyper::Body;
use itertools::Itertools;
use risingwave_common::config::{ObjectStoreConfig, S3ObjectStoreConfig};
use risingwave_common::monitor::connection::monitor_connector;
use risingwave_common::range::RangeBoundsExt;
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use super::object_metrics::ObjectStoreMetrics;
use super::{
    BoxedStreamingUploader, Bytes, ObjectError, ObjectMetadata, ObjectRangeBounds, ObjectResult,
    ObjectStore, StreamingUploader,
};
use crate::object::{try_update_failure_metric, ObjectDataStream, ObjectMetadataIter};

type PartId = i32;

/// MinIO and S3 share the same minimum part ID and part size.
const MIN_PART_ID: PartId = 1;
/// The minimum number of bytes that is buffered before they are uploaded as a part.
/// Its value must be greater than the minimum part size of 5MiB.
///
/// Reference: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
const S3_PART_SIZE: usize = 16 * 1024 * 1024;
// TODO: we should do some benchmark to determine the proper part size for MinIO
const MINIO_PART_SIZE: usize = 16 * 1024 * 1024;
/// The number of S3/MinIO bucket prefixes
const NUM_BUCKET_PREFIXES: u32 = 256;
/// Stop multipart uploads that don't complete within a specified number of days after being
/// initiated. (Day is the smallest granularity)
const S3_INCOMPLETE_MULTIPART_UPLOAD_RETENTION_DAYS: i32 = 1;

/// S3 multipart upload handle. The multipart upload is not initiated until the first part is
/// available for upload.
///
/// Reference: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html>
pub struct S3StreamingUploader {
    client: Client,
    part_size: usize,
    bucket: String,
    /// The key of the object.
    key: String,
    /// The identifier of multipart upload task for S3.
    upload_id: Option<String>,
    /// Next part ID.
    next_part_id: PartId,
    /// Join handles for part uploads.
    join_handles: Vec<JoinHandle<ObjectResult<(PartId, UploadPartOutput)>>>,
    /// Buffer for data. It will store at least `part_size` bytes of data before wrapping itself
    /// into a stream and upload to object store as a part.
    buf: Vec<Bytes>,
    /// Length of the data that have not been uploaded to S3.
    not_uploaded_len: usize,
    /// To record metrics for uploading part.
    metrics: Arc<ObjectStoreMetrics>,
}

impl S3StreamingUploader {
    pub fn new(
        client: Client,
        bucket: String,
        part_size: usize,
        key: String,
        metrics: Arc<ObjectStoreMetrics>,
    ) -> S3StreamingUploader {
        Self {
            client,
            bucket,
            part_size,
            key,
            upload_id: None,
            next_part_id: MIN_PART_ID,
            join_handles: Default::default(),
            buf: Default::default(),
            not_uploaded_len: 0,
            metrics,
        }
    }

    async fn upload_next_part(&mut self) -> ObjectResult<()> {
        let operation_type = "s3_upload_part";

        // Lazily create multipart upload.
        if self.upload_id.is_none() {
            let resp = self
                .client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .send()
                .await?;
            self.upload_id = Some(resp.upload_id.unwrap());
        }

        // Get the data to upload for the next part.
        let data = self.buf.drain(..).collect_vec();
        let len = self.not_uploaded_len;
        debug_assert_eq!(
            data.iter().map(|b| b.len()).sum::<usize>(),
            self.not_uploaded_len
        );

        // Update part id.
        let part_id = self.next_part_id;
        self.next_part_id += 1;

        // Clone the variables to be passed into the upload join handle.
        let client_cloned = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone().unwrap();

        let metrics = self.metrics.clone();
        metrics
            .operation_size
            .with_label_values(&[operation_type])
            .observe(len as f64);

        self.join_handles.push(tokio::spawn(async move {
            let _timer = metrics
                .operation_latency
                .with_label_values(&["s3", operation_type])
                .start_timer();
            let upload_output_res = client_cloned
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_id)
                .body(get_upload_body(data))
                .content_length(len as i64)
                .send()
                .await
                .map_err(Into::into);
            try_update_failure_metric(&metrics, &upload_output_res, operation_type);
            Ok((part_id, upload_output_res?))
        }));

        Ok(())
    }

    async fn flush_multipart_and_complete(&mut self) -> ObjectResult<()> {
        if !self.buf.is_empty() {
            self.upload_next_part().await?;
        }

        // If any part fails to upload, abort the upload.
        let join_handles = self.join_handles.drain(..).collect_vec();

        let mut uploaded_parts = Vec::with_capacity(join_handles.len());
        for result in try_join_all(join_handles)
            .await
            .map_err(ObjectError::internal)?
        {
            uploaded_parts.push(result?);
        }

        let completed_parts = Some(
            uploaded_parts
                .iter()
                .map(|(part_id, output)| {
                    CompletedPart::builder()
                        .set_e_tag(output.e_tag.clone())
                        .set_part_number(Some(*part_id))
                        .build()
                })
                .collect_vec(),
        );

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(self.upload_id.as_ref().unwrap())
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(completed_parts)
                    .build(),
            )
            .send()
            .await?;

        Ok(())
    }

    async fn abort_multipart_upload(&self) -> ObjectResult<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(self.upload_id.as_ref().unwrap())
            .send()
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl StreamingUploader for S3StreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        fail_point!("s3_write_bytes_err", |_| Err(ObjectError::internal(
            "s3 write bytes error"
        )));
        let data_len = data.len();
        self.not_uploaded_len += data_len;
        self.buf.push(data);

        if self.not_uploaded_len >= self.part_size {
            self.upload_next_part().await?;
            self.not_uploaded_len = 0;
        }
        Ok(())
    }

    /// If the multipart upload has not been initiated, we can use `PutObject` instead to save the
    /// `CreateMultipartUpload` and `CompleteMultipartUpload` requests. Otherwise flush the
    /// remaining data of the buffer to S3 as a new part.
    async fn finish(mut self: Box<Self>) -> ObjectResult<()> {
        fail_point!("s3_finish_streaming_upload_err", |_| Err(
            ObjectError::internal("s3 finish streaming upload error")
        ));

        if self.upload_id.is_none() {
            debug_assert!(self.join_handles.is_empty());
            if self.buf.is_empty() {
                debug_assert_eq!(self.not_uploaded_len, 0);
                Err(ObjectError::internal("upload empty object"))
            } else {
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .body(get_upload_body(self.buf))
                    .content_length(self.not_uploaded_len as i64)
                    .key(&self.key)
                    .send()
                    .await?;
                Ok(())
            }
        } else if let Err(e) = self.flush_multipart_and_complete().await {
            tracing::warn!(key = self.key, error = %e.as_report(), "Failed to upload object");
            self.abort_multipart_upload().await?;
            Err(e)
        } else {
            Ok(())
        }
    }

    fn get_memory_usage(&self) -> u64 {
        self.part_size as u64
    }
}

fn get_upload_body(data: Vec<Bytes>) -> ByteStream {
    SdkBody::retryable(move || {
        Body::wrap_stream(stream::iter(data.clone().into_iter().map(ObjectResult::Ok))).into()
    })
    .into()
}

/// Object store with S3 backend
/// The full path to a file on S3 would be `s3://bucket/<data_directory>/prefix/file`
pub struct S3ObjectStore {
    client: Client,
    bucket: String,
    part_size: usize,
    /// For S3 specific metrics.
    metrics: Arc<ObjectStoreMetrics>,

    config: ObjectStoreConfig,
}

#[async_trait::async_trait]
impl ObjectStore for S3ObjectStore {
    fn get_object_prefix(&self, obj_id: u64) -> String {
        // Delegate to static method to avoid creating an `S3ObjectStore` in unit test.
        Self::get_object_prefix(obj_id)
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        fail_point!("s3_upload_err", |_| Err(ObjectError::internal(
            "s3 upload error"
        )));
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .body(ByteStream::from(obj))
                .key(path)
                .send()
                .await?;
            Ok(())
        }
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<BoxedStreamingUploader> {
        fail_point!("s3_streaming_upload_err", |_| Err(ObjectError::internal(
            "s3 streaming upload error"
        )));
        Ok(Box::new(S3StreamingUploader::new(
            self.client.clone(),
            self.bucket.clone(),
            self.part_size,
            path.to_string(),
            self.metrics.clone(),
        )))
    }

    /// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
    async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        fail_point!("s3_read_err", |_| Err(ObjectError::internal(
            "s3 read error"
        )));

        // retry if occurs AWS EC2 HTTP timeout error.
        let val = tokio_retry::RetryIf::spawn(
            self.get_retry_strategy(),
            || async {
                match self.obj_store_request(path, range.clone()).send().await {
                    Ok(resp) => {
                        let val = resp
                            .body
                            .collect()
                            .await
                            .map_err(either::Right)?
                            .into_bytes();
                        Ok(val)
                    }
                    Err(err) => {
                        if let SdkError::DispatchFailure(e) = &err
                            && e.is_timeout()
                        {
                            self.metrics
                                .request_retry_count
                                .with_label_values(&["read"])
                                .inc();
                        }

                        Err(either::Left(err))
                    }
                }
            },
            RetryCondition::new(&self.config.s3),
        )
        .await?;

        if let Some(len) = range.len()
            && len != val.len()
        {
            return Err(ObjectError::internal(format!(
                "mismatched size: expected {}, found {} when reading {} at {:?}",
                len,
                val.len(),
                path,
                range,
            )));
        }

        Ok(val)
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        fail_point!("s3_metadata_err", |_| Err(ObjectError::internal(
            "s3 metadata error"
        )));
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        Ok(ObjectMetadata {
            key: path.to_owned(),
            last_modified: resp
                .last_modified()
                .expect("last_modified required")
                .as_secs_f64(),
            total_size: resp.content_length.unwrap_or_default() as usize,
        })
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        range: Range<usize>,
    ) -> ObjectResult<ObjectDataStream> {
        fail_point!("s3_streaming_read_err", |_| Err(ObjectError::internal(
            "s3 streaming read error"
        )));

        // retry if occurs AWS EC2 HTTP timeout error.
        let resp = tokio_retry::RetryIf::spawn(
            self.get_retry_strategy(),
            || async {
                match self.obj_store_request(path, range.clone()).send().await {
                    Ok(resp) => Ok(resp),
                    Err(err) => {
                        if let SdkError::DispatchFailure(e) = &err
                            && e.is_timeout()
                        {
                            self.metrics
                                .request_retry_count
                                .with_label_values(&["streaming_read"])
                                .inc();
                        }

                        Err(either::Left(err))
                    }
                }
            },
            RetryCondition::new(&self.config.s3),
        )
        .await?;
        let reader = FuturesStreamCompatByteStream::new(resp.body);

        Ok(Box::pin(
            reader
                .into_stream()
                .map(|item| item.map_err(ObjectError::from)),
        ))
    }

    /// Permanently deletes the whole object.
    /// According to Amazon S3, this will simply return Ok if the object does not exist.
    async fn delete(&self, path: &str) -> ObjectResult<()> {
        fail_point!("s3_delete_err", |_| Err(ObjectError::internal(
            "s3 delete error"
        )));
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        Ok(())
    }

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    ///
    /// Uses AWS' DeleteObjects API. See [AWS Docs](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html) for more details.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        // AWS restricts the number of objects per request to 1000.
        const MAX_LEN: usize = 1000;

        // If needed, split given set into subsets of size with no more than `MAX_LEN` objects.
        for start_idx /* inclusive */ in (0..paths.len()).step_by(MAX_LEN) {
            let end_idx /* exclusive */ = cmp::min(paths.len(), start_idx + MAX_LEN);
            let slice = &paths[start_idx..end_idx];
            // Create identifiers from paths.
            let mut obj_ids = Vec::with_capacity(slice.len());
            for path in slice {
                obj_ids.push(ObjectIdentifier::builder().key(path).build().unwrap());
            }

            // Build and submit request to delete objects.
            let delete_builder = Delete::builder().set_objects(Some(obj_ids));
            let delete_output = self
                .client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete_builder.build().unwrap()).send()
                .await?;

            // Check if there were errors.
            if !delete_output.errors().is_empty() {
                return Err(ObjectError::internal(format!("DeleteObjects request returned exception for some objects: {:?}", delete_output.errors())));
            }
        }

        Ok(())
    }

    async fn list(&self, prefix: &str) -> ObjectResult<ObjectMetadataIter> {
        Ok(Box::pin(S3ObjectIter::new(
            self.client.clone(),
            self.bucket.clone(),
            prefix.to_string(),
        )))
    }

    fn store_media_type(&self) -> &'static str {
        "s3"
    }

    fn recv_buffer_size(&self) -> usize {
        self.config
            .s3
            .object_store_recv_buffer_size
            .unwrap_or(1 << 21)
    }

    fn config(&self) -> Option<&ObjectStoreConfig> {
        Some(&self.config)
    }
}

impl S3ObjectStore {
    pub fn new_http_client(config: &ObjectStoreConfig) -> impl HttpClient {
        let mut http = hyper::client::HttpConnector::new();

        // connection config
        if let Some(keepalive_ms) = config.s3.object_store_keepalive_ms.as_ref() {
            http.set_keepalive(Some(Duration::from_millis(*keepalive_ms)));
        }

        if let Some(nodelay) = config.s3.object_store_nodelay.as_ref() {
            http.set_nodelay(*nodelay);
        }

        if let Some(recv_buffer_size) = config.s3.object_store_recv_buffer_size.as_ref() {
            http.set_recv_buffer_size(Some(*recv_buffer_size));
        }

        if let Some(send_buffer_size) = config.s3.object_store_send_buffer_size.as_ref() {
            http.set_send_buffer_size(Some(*send_buffer_size));
        }

        http.enforce_http(false);

        let conn = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_all_versions()
            .wrap_connector(http);

        let conn = monitor_connector(conn, "S3");

        HyperClientBuilder::new().build(conn)
    }

    /// Creates an S3 object store from environment variable.
    ///
    /// See [AWS Docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) on how to provide credentials and region from env variable. If you are running compute-node on EC2, no configuration is required.
    pub async fn new_with_config(
        bucket: String,
        metrics: Arc<ObjectStoreMetrics>,
        config: ObjectStoreConfig,
    ) -> Self {
        let sdk_config_loader = aws_config::from_env()
            .retry_config(RetryConfig::standard().with_max_attempts(4))
            .http_client(Self::new_http_client(&config));

        // Retry 3 times if we get server-side errors or throttling errors
        let client = match std::env::var("RW_S3_ENDPOINT") {
            Ok(endpoint) => {
                // s3 compatible storage
                let is_force_path_style = match std::env::var("RW_IS_FORCE_PATH_STYLE") {
                    Ok(value) => value == "true",
                    Err(_) => false,
                };

                let sdk_config = sdk_config_loader.load().await;
                #[cfg(madsim)]
                let client = Client::new(&sdk_config);
                #[cfg(not(madsim))]
                let client = Client::from_conf(
                    aws_sdk_s3::config::Builder::from(&sdk_config)
                        .endpoint_url(endpoint)
                        .force_path_style(is_force_path_style)
                        .build(),
                );
                client
            }
            Err(_) => {
                // s3

                let sdk_config = sdk_config_loader.load().await;
                Client::new(&sdk_config)
            }
        };

        Self {
            client,
            bucket,
            part_size: S3_PART_SIZE,
            metrics,
            config,
        }
    }

    /// Creates a minio client. The server should be like `minio://key:secret@address:port/bucket`.
    pub async fn with_minio(
        server: &str,
        metrics: Arc<ObjectStoreMetrics>,
        s3_object_store_config: ObjectStoreConfig,
    ) -> Self {
        let server = server.strip_prefix("minio://").unwrap();
        let (access_key_id, rest) = server.split_once(':').unwrap();
        let (secret_access_key, mut rest) = rest.split_once('@').unwrap();
        let endpoint_prefix = if let Some(rest_stripped) = rest.strip_prefix("https://") {
            rest = rest_stripped;
            "https://"
        } else if let Some(rest_stripped) = rest.strip_prefix("http://") {
            rest = rest_stripped;
            "http://"
        } else {
            "http://"
        };
        let (address, bucket) = rest.split_once('/').unwrap();

        #[cfg(madsim)]
        let builder = aws_sdk_s3::config::Builder::new().credentials_provider(
            Credentials::from_keys(access_key_id, secret_access_key, None),
        );
        #[cfg(not(madsim))]
        let builder = aws_sdk_s3::config::Builder::from(
            &aws_config::ConfigLoader::default()
                // FIXME: https://github.com/awslabs/aws-sdk-rust/issues/973
                .credentials_provider(Credentials::from_keys(
                    access_key_id,
                    secret_access_key,
                    None,
                ))
                .load()
                .await,
        )
        .force_path_style(true)
        .http_client(Self::new_http_client(&s3_object_store_config))
        .behavior_version_latest();
        let config = builder
            .region(Region::new("custom"))
            .endpoint_url(format!("{}{}", endpoint_prefix, address))
            .build();
        let client = Client::from_conf(config);

        Self {
            client,
            bucket: bucket.to_string(),
            part_size: MINIO_PART_SIZE,
            metrics,
            config: s3_object_store_config,
        }
    }

    fn get_object_prefix(obj_id: u64) -> String {
        let prefix = crc32fast::hash(&obj_id.to_be_bytes()) % NUM_BUCKET_PREFIXES;
        let mut obj_prefix = prefix.to_string();
        obj_prefix.push('/');
        obj_prefix
    }

    /// Generates an HTTP GET request to download the object specified in `path`. If given,
    /// `start_pos` and `end_pos` specify the first and last byte to download, respectively. Both
    /// are inclusive and 0-based. For example, set `start_pos = 0` and `end_pos = 7` to download
    /// the first 8 bytes. If neither is given, the request will download the whole object.
    fn obj_store_request(
        &self,
        path: &str,
        range: impl ObjectRangeBounds,
    ) -> GetObjectFluentBuilder {
        let req = self.client.get_object().bucket(&self.bucket).key(path);
        if range.is_full() {
            return req;
        }

        let start = range.start().map(|v| v.to_string()).unwrap_or_default();
        let end = range.end().map(|v| (v - 1).to_string()).unwrap_or_default(); // included

        req.range(format!("bytes={}-{}", start, end))
    }

    // When multipart upload is aborted, if any part uploads are in progress, those part uploads
    // might or might not succeed. As a result, these parts will remain in the bucket and be
    // charged for part storage. Therefore, we need to configure the bucket to purge stale
    // parts.
    //
    /// Note: This configuration only works for S3. MinIO automatically enables this feature, and it
    /// is not configurable with S3 sdk. To verify that this feature is enabled, use `mc admin
    /// config get <alias> api`.
    ///
    /// Reference:
    /// - S3
    ///   - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html>
    ///   - <https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html>
    /// - MinIO
    ///   - <https://github.com/minio/minio/issues/15681#issuecomment-1245126561>
    pub async fn configure_bucket_lifecycle(&self, data_directory: &str) -> bool {
        // Check if lifecycle is already configured to avoid overriding existing configuration.
        let bucket = self.bucket.as_str();
        let mut configured_rules = vec![];
        let get_config_result = self
            .client
            .get_bucket_lifecycle_configuration()
            .bucket(bucket)
            .send()
            .await;
        let mut is_expiration_configured = false;

        if let Ok(config) = &get_config_result {
            for rule in config.rules() {
                if rule.expiration().is_some() {
                    // When both of the conditions are met, it is considered that there is a risk of data deletion.
                    //
                    // 1. expiration status rule is enabled
                    // 2. (a) prefix filter is not set
                    // or (b) prefix filter is set to the data directory of RisingWave.
                    //
                    // P.S. 1 && (2a || 2b)
                    is_expiration_configured |= rule.status == ExpirationStatus::Enabled // 1
                    && match rule.filter().as_ref() {
                        // 2a
                        None => true,
                        // 2b
                        Some(LifecycleRuleFilter::Prefix(prefix))
                            if data_directory.starts_with(prefix) =>
                        {
                            true
                        }
                        _ => false,
                    };

                    if matches!(rule.status(), ExpirationStatus::Enabled)
                        && rule.abort_incomplete_multipart_upload().is_some()
                    {
                        configured_rules.push(rule);
                    }
                }
            }
        }

        if !configured_rules.is_empty() {
            tracing::info!(
                "S3 bucket {} has already configured AbortIncompleteMultipartUpload: {:?}",
                bucket,
                configured_rules,
            );
        } else {
            let bucket_lifecycle_rule = LifecycleRule::builder()
                .id("abort-incomplete-multipart-upload")
                .status(ExpirationStatus::Enabled)
                .filter(LifecycleRuleFilter::Prefix(String::new()))
                .abort_incomplete_multipart_upload(
                    AbortIncompleteMultipartUpload::builder()
                        .days_after_initiation(S3_INCOMPLETE_MULTIPART_UPLOAD_RETENTION_DAYS)
                        .build(),
                )
                .build()
                .unwrap();
            let bucket_lifecycle_config = BucketLifecycleConfiguration::builder()
                .rules(bucket_lifecycle_rule)
                .build()
                .unwrap();
            if self
                .client
                .put_bucket_lifecycle_configuration()
                .bucket(bucket)
                .lifecycle_configuration(bucket_lifecycle_config)
                .send()
                .await
                .is_ok()
            {
                tracing::info!(
                    "S3 bucket {:?} is configured to automatically purge abandoned MultipartUploads after {} days",
                    bucket,
                    S3_INCOMPLETE_MULTIPART_UPLOAD_RETENTION_DAYS,
                );
            } else {
                tracing::warn!("Failed to configure life cycle rule for S3 bucket: {:?}. It is recommended to configure it manually to avoid unnecessary storage cost.", bucket);
            }
        }
        if is_expiration_configured {
            tracing::info!(
                "S3 bucket {} has already configured the expiration for the lifecycle.",
                bucket,
            );
        }
        is_expiration_configured
    }

    #[inline(always)]
    fn get_retry_strategy(&self) -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(self.config.s3.object_store_req_retry_interval_ms)
            .max_delay(Duration::from_millis(
                self.config.s3.object_store_req_retry_max_delay_ms,
            ))
            .take(self.config.s3.object_store_req_retry_max_attempts)
            .map(jitter)
    }
}

struct S3ObjectIter {
    buffer: VecDeque<ObjectMetadata>,
    client: Client,
    bucket: String,
    prefix: String,
    next_continuation_token: Option<String>,
    is_truncated: Option<bool>,
    #[allow(clippy::type_complexity)]
    send_future: Option<
        BoxFuture<
            'static,
            Result<
                (Vec<ObjectMetadata>, Option<String>, Option<bool>),
                SdkError<ListObjectsV2Error, aws_smithy_runtime_api::http::Response<SdkBody>>,
            >,
        >,
    >,
}

impl S3ObjectIter {
    fn new(client: Client, bucket: String, prefix: String) -> Self {
        Self {
            buffer: VecDeque::default(),
            client,
            bucket,
            prefix,
            next_continuation_token: None,
            is_truncated: Some(true),
            send_future: None,
        }
    }
}

impl Stream for S3ObjectIter {
    type Item = ObjectResult<ObjectMetadata>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(e) = self.buffer.pop_front() {
            return Poll::Ready(Some(Ok(e)));
        }
        if let Some(f) = self.send_future.as_mut() {
            return match ready!(f.poll_unpin(cx)) {
                Ok((more, next_continuation_token, is_truncated)) => {
                    self.next_continuation_token = next_continuation_token;
                    self.is_truncated = is_truncated;
                    self.buffer.extend(more);
                    self.send_future = None;
                    self.poll_next(cx)
                }
                Err(e) => {
                    self.send_future = None;
                    Poll::Ready(Some(Err(e.into())))
                }
            };
        }
        if !self.is_truncated.unwrap_or_default() {
            return Poll::Ready(None);
        }
        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix);
        if let Some(continuation_token) = self.next_continuation_token.as_ref() {
            request = request.continuation_token(continuation_token);
        }
        let f = async move {
            match request.send().await {
                Ok(r) => {
                    let more = r
                        .contents()
                        .iter()
                        .map(|obj| ObjectMetadata {
                            key: obj.key().expect("key required").to_owned(),
                            last_modified: obj
                                .last_modified()
                                .map(|l| l.as_secs_f64())
                                .unwrap_or(0f64),
                            total_size: obj.size().unwrap_or_default() as usize,
                        })
                        .collect_vec();
                    let is_truncated = r.is_truncated;
                    let next_continuation_token = r.next_continuation_token;
                    Ok((more, next_continuation_token, is_truncated))
                }
                Err(e) => Err(e),
            }
        };
        self.send_future = Some(Box::pin(f));
        self.poll_next(cx)
    }
}

type RetryError = Either<
    SdkError<GetObjectError, aws_smithy_runtime_api::http::Response<SdkBody>>,
    ByteStreamError,
>;

impl From<RetryError> for ObjectError {
    fn from(e: RetryError) -> Self {
        match e {
            Either::Left(e) => e.into(),
            Either::Right(e) => e.into(),
        }
    }
}

struct RetryCondition {
    retry_unknown_service_error: bool,
    retryable_service_error_codes: Vec<String>,
}

impl RetryCondition {
    fn new(config: &S3ObjectStoreConfig) -> Self {
        Self {
            retry_unknown_service_error: config.developer.object_store_retry_unknown_service_error
                || config.retry_unknown_service_error,
            retryable_service_error_codes: config
                .developer
                .object_store_retryable_service_error_codes
                .clone(),
        }
    }
}

impl tokio_retry::Condition<RetryError> for RetryCondition {
    fn should_retry(&mut self, err: &RetryError) -> bool {
        match err {
            Either::Left(err) => match err {
                SdkError::DispatchFailure(e) => {
                    if e.is_timeout() {
                        tracing::warn!(target: "http_timeout_retry", "{e:?} occurs, retry S3 get_object request.");
                        return true;
                    }
                }
                SdkError::ServiceError(e) => match e.err().code() {
                    None => {
                        if self.retry_unknown_service_error {
                            tracing::warn!(target: "unknown_service_error", "{e:?} occurs, retry S3 get_object request.");
                            return true;
                        }
                    }
                    Some(code) => {
                        if self
                            .retryable_service_error_codes
                            .iter()
                            .any(|s| s.as_str().eq_ignore_ascii_case(code))
                        {
                            tracing::warn!(target: "retryable_service_error", "{e:?} occurs, retry S3 get_object request.");
                            return true;
                        }
                    }
                },
                _ => {}
            },
            Either::Right(_) => {
                // Unfortunately `ErrorKind` of `ByteStreamError` is not accessible.
                // Always returns true and relies on req_retry_max_attempts to avoid infinite loop.
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use crate::object::s3::NUM_BUCKET_PREFIXES;
    use crate::object::S3ObjectStore;

    fn get_hash_of_object(obj_id: u64) -> u32 {
        let crc_hash = crc32fast::hash(&obj_id.to_be_bytes());
        crc_hash % NUM_BUCKET_PREFIXES
    }

    #[tokio::test]
    async fn test_get_object_prefix() {
        for obj_id in 0..99999 {
            let hash = get_hash_of_object(obj_id);
            let prefix = S3ObjectStore::get_object_prefix(obj_id);
            assert_eq!(format!("{}/", hash), prefix);
        }

        let obj_prefix = String::default();
        let path = format!("{}/{}{}.data", "hummock_001", obj_prefix, 101);
        assert_eq!("hummock_001/101.data", path);
    }
}
