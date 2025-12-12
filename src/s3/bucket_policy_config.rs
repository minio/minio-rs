use crate::s3::error::{Error, ValidationErr};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use xmltree::{Element, XMLNode};
//use std::io::Cursor;

/*
// Define custom Error type
#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid bucket policy: {0}")]
    InvalidPolicy(String),
    #[error("XML parsing error: {0}")]
    XmlError(#[from] xmltree::ParseError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
}
*/

// Equivalent to Action in Go
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    // Object operations
    #[serde(rename = "s3:GetObject")]
    GetObject,
    #[serde(rename = "s3:PutObject")]
    PutObject,
    #[serde(rename = "s3:DeleteObject")]
    DeleteObject,
    #[serde(rename = "s3:GetObjectVersion")]
    GetObjectVersion,
    #[serde(rename = "s3:GetObjectVersionTagging")]
    GetObjectVersionTagging,
    #[serde(rename = "s3:PutObjectRetention")]
    PutObjectRetention,
    #[serde(rename = "s3:PutObjectLegalHold")]
    PutObjectLegalHold,

    // Bucket operations
    #[serde(rename = "s3:ListBucket")]
    ListBucket,
    #[serde(rename = "s3:ListBucketMultipartUploads")]
    ListBucketMultipartUploads,
    #[serde(rename = "s3:GetBucketLocation")]
    GetBucketLocation,
    #[serde(rename = "s3:GetBucketVersioning")]
    GetBucketVersioning,
    #[serde(rename = "s3:GetBucketObjectLockConfiguration")]
    GetBucketObjectLockConfiguration,
    #[serde(rename = "s3:PutBucketObjectLockConfiguration")]
    PutBucketObjectLockConfiguration,

    // Replication operations
    #[serde(rename = "s3:GetReplicationConfiguration")]
    GetReplicationConfiguration,
    #[serde(rename = "s3:ReplicateTags")]
    ReplicateTags,
    #[serde(rename = "s3:ReplicateObject")]
    ReplicateObject,
    #[serde(rename = "s3:ReplicateDelete")]
    ReplicateDelete,

    // Multipart operations
    #[serde(rename = "s3:AbortMultipartUpload")]
    AbortMultipartUpload,

    // Encryption operations
    #[serde(rename = "s3:GetEncryptionConfiguration")]
    GetEncryptionConfiguration,
}

// Equivalent to Effect in Go
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    Allow,
    Deny,
}

// Equivalent to ID in Go
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ID(pub String);

// Equivalent to Principal in Go
// Principal can be either "*" (string) or {"AWS": ["..."]} (object)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Principal {
    Wildcard(String), // "*"
    Aws {
        #[serde(rename = "AWS")]
        aws: Vec<String>,
    },
}

// Equivalent to BPStatement in Go
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BPStatement {
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Principal")]
    pub principal: Principal,
    #[serde(rename = "Action")]
    pub actions: Vec<Action>,
    #[serde(rename = "Resource")]
    pub resources: Vec<String>,
    #[serde(
        rename = "Condition",
        default,
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub conditions: HashMap<String, HashMap<String, Vec<String>>>,
}

impl BPStatement {
    pub fn is_allowed(&self, args: &BucketPolicyArgs) -> bool {
        // Implementation similar to Go's IsAllowed for statements
        // This would check if the statement allows the given action for the given resource
        // with the given conditions

        // For this example, we'll just provide a simplified placeholder
        // In a real implementation, you'd need to check conditions, resources, etc.
        self.actions.contains(&args.action)
            && self.resources.iter().any(|r| r.contains(&args.bucket_name))
    }

    pub fn equals(&self, other: &Self) -> bool {
        self.effect == other.effect
            && self.principal == other.principal
            && self.actions == other.actions
            && self.resources == other.resources
            && self.conditions == other.conditions
    }

    pub fn validate(&self, bucket_name: &str) -> Result<(), Error> {
        // Check if all resources in this statement belong to the given bucket
        for resource in &self.resources {
            if !resource.contains(bucket_name) {
                return Err(ValidationErr::InvalidBucketPolicy(format!(
                    "resource '{}' does not belong to bucket '{}'",
                    resource, bucket_name
                ))
                .into());
            }
        }
        Ok(())
    }
}

// Equivalent to BucketPolicyArgs in Go
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketPolicyArgs {
    pub account_name: String,
    pub groups: Vec<String>,
    pub action: Action,
    pub bucket_name: String,
    pub condition_values: HashMap<String, Vec<String>>,
    pub is_owner: bool,
    pub object_name: String,
}

// Equivalent to BucketPolicy in Go
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BucketPolicy {
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<ID>,
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Statement")]
    pub statements: Vec<BPStatement>,
}

impl BucketPolicy {
    pub const DEFAULT_VERSION: &'static str = "2012-10-17";

    pub fn is_allowed(&self, args: &BucketPolicyArgs) -> bool {
        // Check all deny statements first
        for statement in &self.statements {
            if statement.effect == Effect::Deny && !statement.is_allowed(args) {
                return false;
            }
        }

        // For owner, it's allowed by default
        if args.is_owner {
            return true;
        }

        // Check all allow statements
        for statement in &self.statements {
            if statement.effect == Effect::Allow && statement.is_allowed(args) {
                return true;
            }
        }

        false
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    pub fn is_valid(&self) -> Result<(), Error> {
        if !self.version.is_empty() && self.version != Self::DEFAULT_VERSION {
            return Err(ValidationErr::InvalidBucketPolicy(format!(
                "invalid version '{}'",
                self.version
            ))
            .into());
        }

        for _statement in &self.statements {
            // Assuming there's an implementation for statement.is_valid()
            // For this example, we'll skip detailed validation
        }

        Ok(())
    }

    pub fn drop_duplicate_statements(&mut self) {
        let mut unique_statements = Vec::new();

        for statement in self.statements.drain(..) {
            if !unique_statements
                .iter()
                .any(|s: &BPStatement| s.equals(&statement))
            {
                unique_statements.push(statement);
            }
        }

        self.statements = unique_statements;
    }

    pub fn validate(&self, bucket_name: &str) -> Result<(), Error> {
        self.is_valid()?;

        for statement in &self.statements {
            statement.validate(bucket_name)?;
        }

        Ok(())
    }

    pub fn equals(&self, other: &Self) -> bool {
        if self.id != other.id || self.version != other.version {
            return false;
        }

        if self.statements.len() != other.statements.len() {
            return false;
        }

        for (i, statement) in self.statements.iter().enumerate() {
            if !statement.equals(&other.statements[i]) {
                return false;
            }
        }

        true
    }

    pub fn parse_from_json<R: Read>(reader: R, bucket_name: &str) -> Result<Self, Error> {
        let mut policy: BucketPolicy =
            serde_json::from_reader(reader).map_err(ValidationErr::from)?;
        policy.validate(bucket_name)?;
        policy.drop_duplicate_statements();
        Ok(policy)
    }
}

// This is the struct you want to implement
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BucketPolicyConfig {
    pub rules: Vec<BucketPolicy>,
}

impl BucketPolicyConfig {
    pub fn from_xml(root: &Element) -> Result<BucketPolicyConfig, Error> {
        // Parse XML into BucketPolicyConfig
        let mut config = BucketPolicyConfig { rules: Vec::new() };

        // Process the XML element to extract policy information
        // For example, if your XML structure has policy elements as children:
        for child in &root.children {
            if let XMLNode::Element(policy_elem) = child
                && policy_elem.name == "Policy"
            {
                // Extract policy data from the element
                let mut policy = BucketPolicy {
                    id: None,
                    version: policy_elem
                        .attributes
                        .get("Version")
                        .map_or_else(String::new, |v| v.clone()),
                    statements: Vec::new(),
                };

                // Extract ID if present
                if let Some(id_value) = policy_elem.attributes.get("ID") {
                    policy.id = Some(ID(id_value.clone()));
                }

                // Extract statements
                for stmt_node in &policy_elem.children {
                    if let XMLNode::Element(stmt_elem) = stmt_node
                        && stmt_elem.name == "Statement"
                    {
                        // You would need to implement the statement parsing logic here
                        // based on your specific XML schema

                        // This is a placeholder - you'll need to extract:
                        // - Effect (Allow/Deny)
                        // - Principal
                        // - Actions
                        // - Resources
                        // - Conditions

                        // Example of extracting an effect:
                        let effect = stmt_elem
                            .get_child("Effect")
                            .and_then(|e| e.get_text())
                            .map(|t| match t.to_string().as_str() {
                                "Allow" => Effect::Allow,
                                "Deny" => Effect::Deny,
                                _ => Effect::Deny, // Default to Deny for safety
                            })
                            .unwrap_or(Effect::Deny);

                        // You would continue with parsing the rest of the statement...

                        // For this example, we'll just create a minimal statement
                        let statement = BPStatement {
                            effect,
                            principal: Principal::Wildcard("*".to_string()),
                            actions: Vec::new(),
                            resources: Vec::new(),
                            conditions: HashMap::new(),
                        };

                        policy.statements.push(statement);
                    }
                }

                config.rules.push(policy);
            }
        }

        Ok(config)
    }

    pub fn validate(&self) -> Result<(), Error> {
        // Validate all bucket policies in the configuration
        for rule in &self.rules {
            rule.is_valid()?;
        }
        Ok(())
    }

    pub fn empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn to_xml(&self) -> String {
        // Create the root element
        let mut root = Element::new("BucketPolicyConfig");

        // Add each policy as a child element
        for rule in &self.rules {
            let mut policy_elem = Element::new("Policy");

            // Set attributes
            policy_elem
                .attributes
                .insert("Version".to_string(), rule.version.clone());
            if let Some(id) = &rule.id {
                policy_elem
                    .attributes
                    .insert("ID".to_string(), id.0.clone());
            }

            // Add statements
            for statement in &rule.statements {
                let mut stmt_elem = Element::new("Statement");

                // Add Effect
                let mut effect_elem = Element::new("Effect");
                let effect_text = match statement.effect {
                    Effect::Allow => "Allow",
                    Effect::Deny => "Deny",
                };
                effect_elem
                    .children
                    .push(XMLNode::Text(effect_text.to_string()));
                stmt_elem.children.push(XMLNode::Element(effect_elem));

                // Add Principal
                {
                    let mut principal_elem = Element::new("Principal");
                    let principal_text = match &statement.principal {
                        Principal::Wildcard(s) => s.clone(),
                        Principal::Aws { aws } => format!("AWS: {}", aws.join(", ")),
                    };
                    principal_elem.children.push(XMLNode::Text(principal_text));
                    stmt_elem.children.push(XMLNode::Element(principal_elem));
                }

                // Add Actions
                if !statement.actions.is_empty() {
                    let mut actions_elem = Element::new("Action");
                    for action in &statement.actions {
                        let action_text = format!("{:?}", action); // This is simplistic - improve as needed
                        let mut action_elem = Element::new("Action");
                        action_elem.children.push(XMLNode::Text(action_text));
                        actions_elem.children.push(XMLNode::Element(action_elem));
                    }
                    stmt_elem.children.push(XMLNode::Element(actions_elem));
                }

                // Add Resources
                if !statement.resources.is_empty() {
                    let mut resources_elem = Element::new("Resource");
                    for resource in &statement.resources {
                        let mut resource_elem = Element::new("Resource");
                        resource_elem.children.push(XMLNode::Text(resource.clone()));
                        resources_elem
                            .children
                            .push(XMLNode::Element(resource_elem));
                    }
                    stmt_elem.children.push(XMLNode::Element(resources_elem));
                }

                // Add Conditions
                // This would be more complex and depend on your specific condition format
                if !statement.conditions.is_empty() {
                    let conditions_elem = Element::new("Condition");
                    // Implementation would depend on your condition structure
                    stmt_elem.children.push(XMLNode::Element(conditions_elem));
                }

                policy_elem.children.push(XMLNode::Element(stmt_elem));
            }

            root.children.push(XMLNode::Element(policy_elem));
        }

        // Convert to string
        let mut writer = Vec::new();
        root.write(&mut writer).unwrap_or(());
        String::from_utf8(writer).unwrap_or_default()
    }

    pub fn to_json(&self) -> Result<String, Error> {
        // For bucket policy, we serialize the first policy rule as JSON
        // S3 API expects a single BucketPolicy, not a BucketPolicyConfig
        if self.rules.is_empty() {
            return Ok(String::new());
        }

        serde_json::to_string(&self.rules[0])
            .map_err(ValidationErr::from)
            .map_err(Error::from)
    }
}

impl TryFrom<&str> for BucketPolicyConfig {
    type Error = ValidationErr;

    fn try_from(json: &str) -> Result<Self, Self::Error> {
        let policy: BucketPolicy = serde_json::from_str(json).map_err(ValidationErr::from)?;
        Ok(BucketPolicyConfig {
            rules: vec![policy],
        })
    }
}

impl TryFrom<String> for BucketPolicyConfig {
    type Error = ValidationErr;

    fn try_from(json: String) -> Result<Self, Self::Error> {
        BucketPolicyConfig::try_from(json.as_str())
    }
}

impl TryFrom<&String> for BucketPolicyConfig {
    type Error = ValidationErr;

    fn try_from(json: &String) -> Result<Self, Self::Error> {
        BucketPolicyConfig::try_from(json.as_str())
    }
}
