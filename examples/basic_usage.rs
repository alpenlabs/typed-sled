//! Basic usage example for typed-sled.

use borsh::{BorshDeserialize, BorshSerialize};
use dashmap as _;
use thiserror as _;
use typed_sled::{CodecError, Schema, SledDb, SledTree, TreeName, ValueCodec, error::Result};

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
struct User {
    id: u32,
    name: String,
    email: String,
}

#[derive(Debug)]
struct UserSchema;

impl ValueCodec<UserSchema> for User {
    fn encode_value(&self) -> typed_sled::CodecResult<Vec<u8>> {
        borsh::to_vec(self).map_err(|e| CodecError::SerializationFailed {
            schema: UserSchema::TREE_NAME.0,
            source: e.into(),
        })
    }
    fn decode_value(buf: &[u8]) -> typed_sled::CodecResult<Self> {
        borsh::from_slice(buf).map_err(|e| CodecError::DeserializationFailed {
            schema: UserSchema::TREE_NAME.0,
            source: e.into(),
        })
    }
}

impl Schema for UserSchema {
    const TREE_NAME: TreeName = TreeName("users");
    type Key = u32;
    type Value = User;
}

fn main() -> Result<()> {
    // Open the database
    let sled_db = sled::open("example_db").unwrap();
    let db = SledDb::new(sled_db)?;

    // Get typed trees for each schema
    let users: SledTree<UserSchema> = db.get_tree()?;

    // Create some data
    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    // Insert data using typed trees
    println!("Inserting user: {user:?}");
    users.insert(&user.id, &user)?;

    // Retrieve data
    println!("\nRetrieving user with id 1:");
    if let Some(retrieved_user) = users.get(&1)? {
        println!("Found user: {retrieved_user:?}");
    } else {
        println!("User not found");
    }

    // Try to get non-existent data
    println!("\nTrying to retrieve user with id 999:");
    if let Some(user) = users.get(&999)? {
        println!("Found user: {user:?}");
    } else {
        println!("User not found (as expected)");
    }

    // Remove data
    println!("\nRemoving user 1");
    users.remove(&1)?;

    // Verify removal
    if users.get(&1)?.is_some() {
        println!("User still exists (unexpected)");
    } else {
        println!("User successfully removed");
    }

    println!("\nExample completed successfully!");
    Ok(())
}
