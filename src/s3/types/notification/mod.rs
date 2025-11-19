// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Event notification configuration types for S3 bucket notifications

pub mod and_operator;
pub mod cloud_func_config;
pub mod directive;
pub mod filter;
pub mod notification_common;
pub mod notification_config;
pub mod notification_record;
pub mod notification_records;
pub mod prefix_filter_rule;
pub mod queue_config;
pub mod request_parameters;
pub mod response_elements;
pub mod source;
pub mod suffix_filter_rule;
pub mod topic_config;
pub mod user_identity;

pub use and_operator::AndOperator;
pub use cloud_func_config::CloudFuncConfig;
pub use directive::Directive;
pub use filter::Filter;
pub use notification_config::NotificationConfig;
pub use notification_record::NotificationRecord;
pub use notification_records::NotificationRecords;
pub use prefix_filter_rule::PrefixFilterRule;
pub use queue_config::QueueConfig;
pub use request_parameters::RequestParameters;
pub use response_elements::ResponseElements;
pub use source::Source;
pub use suffix_filter_rule::SuffixFilterRule;
pub use topic_config::TopicConfig;
pub use user_identity::UserIdentity;
