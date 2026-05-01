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

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::{
    AddUserResponse, GetGroupDescriptionResponse, ListGroupsResponse, RemoveUserResponse,
    SetGroupStatusResponse, UpdateGroupMembersResponse,
};
use minio::madmin::types::MadminApi;
use minio::madmin::types::group::{GroupAddRemove, GroupStatus};
use minio::madmin::types::typed_parameters::{AccessKey, GroupName};
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_list_groups() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let response: ListGroupsResponse = madmin_client.list_groups().build().send().await.unwrap();
    let groups = response.groups().unwrap();

    // Groups list is retrievable if we got here without error
    for group_name in &groups {
        assert!(!group_name.is_empty(), "Group name should not be empty");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_group_lifecycle() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let group_name = GroupName::from("test-group-rust");
    let test_user1 = AccessKey::from("test-group-user1-rust");
    let test_user2 = AccessKey::from("test-group-user2-rust");

    // Create test users
    println!("Creating test users");
    let _add_user1_resp: AddUserResponse = madmin_client
        .add_user()
        .access_key(&test_user1)
        .secret_key("TestPassword123!")
        .build()
        .send()
        .await
        .unwrap();

    let _add_user2_resp: AddUserResponse = madmin_client
        .add_user()
        .access_key(&test_user2)
        .secret_key("TestPassword123!")
        .build()
        .send()
        .await
        .unwrap();

    // Add members to group
    let add_req = GroupAddRemove::add_members(
        group_name.to_string(),
        vec![test_user1.to_string(), test_user2.to_string()],
    );

    println!("Adding members to group");
    let _update_resp: UpdateGroupMembersResponse = madmin_client
        .update_group_members()
        .request(add_req)
        .build()
        .send()
        .await
        .unwrap();
    // UpdateGroupMembersResponse is an empty struct, success is indicated by no error

    // List groups - should include our new group
    let response: ListGroupsResponse = madmin_client.list_groups().build().send().await.unwrap();
    let groups = response.groups().unwrap();
    println!("Groups after adding members: {:?}", groups);
    assert!(groups.contains(&group_name.to_string()));

    // Get group description
    println!("Getting group description");
    let response: GetGroupDescriptionResponse = madmin_client
        .get_group_description()
        .group(&group_name)
        .build()
        .send()
        .await
        .unwrap();
    let group_desc = response.description().unwrap();

    assert!(
        !group_desc.name.is_empty(),
        "Group name should not be empty"
    );
    assert!(
        !group_desc.status.is_empty(),
        "Group status should not be empty"
    );
    for member in &group_desc.members {
        assert!(!member.is_empty(), "Member name should not be empty");
    }

    println!("Group description: {:?}", group_desc);
    assert_eq!(group_desc.members.len(), 2);
    assert!(group_desc.members.contains(&test_user1.to_string()));
    assert!(group_desc.members.contains(&test_user2.to_string()));

    // Disable group
    println!("Disabling group");
    let _disable_resp: SetGroupStatusResponse = madmin_client
        .set_group_status()
        .group(&group_name)
        .status(GroupStatus::Disabled)
        .build()
        .send()
        .await
        .unwrap();
    // SetGroupStatusResponse is an empty struct, success is indicated by no error

    // Verify group is disabled
    let response: GetGroupDescriptionResponse = madmin_client
        .get_group_description()
        .group(&group_name)
        .build()
        .send()
        .await
        .unwrap();
    let group_desc = response.description().unwrap();

    assert_eq!(group_desc.status, "disabled");

    // Re-enable group
    println!("Enabling group");
    let _enable_resp: SetGroupStatusResponse = madmin_client
        .set_group_status()
        .group(&group_name)
        .status(GroupStatus::Enabled)
        .build()
        .send()
        .await
        .unwrap();
    // SetGroupStatusResponse is an empty struct, success is indicated by no error

    // Remove one member
    let remove_req =
        GroupAddRemove::remove_members(group_name.to_string(), vec![test_user2.to_string()]);

    println!("Removing member from group");
    let _remove_member_resp: UpdateGroupMembersResponse = madmin_client
        .update_group_members()
        .request(remove_req)
        .build()
        .send()
        .await
        .unwrap();
    // UpdateGroupMembersResponse is an empty struct, success is indicated by no error

    // Verify member removed
    let response: GetGroupDescriptionResponse = madmin_client
        .get_group_description()
        .group(&group_name)
        .build()
        .send()
        .await
        .unwrap();
    let group_desc = response.description().unwrap();

    assert_eq!(group_desc.members.len(), 1);
    assert!(group_desc.members.contains(&test_user1.to_string()));

    // Remove all members to delete group
    let remove_all_req =
        GroupAddRemove::remove_members(group_name.to_string(), vec![test_user1.to_string()]);

    println!("Removing all members (deletes group)");
    let _remove_all_resp: UpdateGroupMembersResponse = madmin_client
        .update_group_members()
        .request(remove_all_req)
        .build()
        .send()
        .await
        .unwrap();
    // UpdateGroupMembersResponse is an empty struct, success is indicated by no error

    // Cleanup test users
    println!("Removing test users");
    let _remove_user1_resp: RemoveUserResponse = madmin_client
        .remove_user()
        .access_key(&test_user1)
        .build()
        .send()
        .await
        .unwrap();

    let _remove_user2_resp: RemoveUserResponse = madmin_client
        .remove_user()
        .access_key(&test_user2)
        .build()
        .send()
        .await
        .unwrap();

    println!("Group lifecycle test completed");
}
