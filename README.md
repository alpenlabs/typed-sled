# typed-sled

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache-blue.svg)](https://opensource.org/licenses/apache-2-0)
[![ci](https://github.com/alpenlabs/typed-sled/actions/workflows/lint.yml/badge.svg?event=push)](https://github.com/alpenlabs/typed-sled/actions)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://alpenlabs.github.io/typed-sled/)

A type-safe wrapper around the [sled](https://github.com/spacejam/sled) embedded database.

This library provides a schema-based approach to working with sled, ensuring compile-time type safety for keys and values while leveraging efficient binary serialization.

## Features

- **Type Safety**: Schema-based table definitions with associated key/value types
- **Serialization**: Flexible codec system for efficient binary encoding  
- **Transactions**: Multi-table atomic operations with retry mechanisms
- **Error Handling**: Comprehensive error types with proper error chaining
- **Concurrent Access**: Thread-safe operations with internal caching
- **Range Queries**: Type-safe iteration and range operations

## Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
typed-sled = { git = "https://github.com/alpenlabs/typed-sled" }
# Optionally for serialization
rkyv = { version = "0.8", features = ["bytecheck"] }
```

## Usage

### Basic Example

```rust
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize, from_bytes, to_bytes};
use typed_sled::{CodecError, Schema, SledDb, TreeName, ValueCodec, error::Result};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
struct User {
    id: u32,
    name: String,
    email: String,
}

// Define a schema for your data
#[derive(Debug)]
struct UserSchema;

impl Schema for UserSchema {
    const TREE_NAME: TreeName = TreeName("users");
    type Key = u32;
    type Value = User;
}

// Implement serialization for your value type
impl ValueCodec<UserSchema> for User {
    fn encode_value(&self) -> typed_sled::CodecResult<Vec<u8>> {
        to_bytes::<RkyvError>(self)
            .map(|bytes| bytes.into_vec())
            .map_err(|e| CodecError::SerializationFailed {
                schema: UserSchema::TREE_NAME.0,
                source: e.into(),
            })
    }

    fn decode_value(buf: &[u8]) -> typed_sled::CodecResult<Self> {
        let mut aligned = AlignedVec::<16>::with_capacity(buf.len());
        aligned.extend_from_slice(buf);
        from_bytes::<User, RkyvError>(&aligned).map_err(|e| CodecError::DeserializationFailed {
            schema: UserSchema::TREE_NAME.0,
            source: e.into(),
        })
    }
}

fn main() -> Result<()> {
    // Open the database
    let sled_db = sled::open("mydb")?;
    let db = SledDb::new(sled_db)?;
    let users = db.get_tree::<UserSchema>()?;

    // Insert data
    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    users.insert(&user.id, &user)?;

    // Retrieve data
    if let Some(retrieved) = users.get(&1)? {
        println!("Found user: {:?}", retrieved);
    }

    Ok(())
}
```

### Transactions

```rust
use typed_sled::{CodecError, Schema, TreeName, ValueCodec};
use typed_sled::transaction::{SledTransactional, ExponentialBackoff};

// Define another schema for settings
#[derive(Debug)]
struct SettingsSchema;

impl Schema for SettingsSchema {
    const TREE_NAME: TreeName = TreeName("settings");
    type Key = String;
    type Value = String;
}

impl ValueCodec<SettingsSchema> for String {
    fn encode_value(&self) -> typed_sled::CodecResult<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }

    fn decode_value(buf: &[u8]) -> typed_sled::CodecResult<Self> {
        String::from_utf8(buf.to_vec()).map_err(|e| CodecError::DeserializationFailed {
            schema: SettingsSchema::TREE_NAME.0,
            source: e.into(),
        })
    }
}

// Get trees and perform transaction
let users = db.get_tree::<UserSchema>()?;
let settings = db.get_tree::<SettingsSchema>()?;

let user1 = User {
    id: 1,
    name: "Alice".to_string(),
    email: "alice@example.com".to_string(),
};

(&users, &settings).transaction_with_retry(
    ExponentialBackoff::new(10, 2.0, 1000),
    5,
    |(tx_users, tx_settings)| {
        // Atomic operations across multiple trees
        tx_users.insert(&1, &user1)?;
        tx_settings.insert(&"theme".to_string(), &"dark".to_string())?;
        
        Ok(())
    }
)?;
```

### Range Queries

```rust
// Iterate over all entries
for result in users.iter() {
    let (key, value) = result?;
    println!("User {}: {:?}", key, value);
}

// Range queries
for result in users.range(1..=100) {
    let (key, value) = result?;
    println!("User {}: {:?}", key, value);
}
```

### Batch Operations

```rust
use typed_sled::batch::SledBatch;

let user1 = User {
    id: 1,
    name: "Alice".to_string(),
    email: "alice@example.com".to_string(),
};
let user2 = User {
    id: 2,
    name: "Bob".to_string(),
    email: "bob@example.com".to_string(),
};

let mut batch = SledBatch::<UserSchema>::default();
batch.insert(1, user1)?;
batch.insert(2, user2)?;
batch.remove(3)?;

users.apply_batch(batch)?;
```

## Key Concepts

### Schemas

Schemas define the structure of your data by specifying:
- **Tree name**: A unique identifier for the logical table
- **Key type**: The type used for keys (must implement `KeyCodec`)
- **Value type**: The type used for values (must implement `ValueCodec`)

### Codecs

Codecs handle serialization/deserialization:
- **KeyCodec**: Built-in implementations for all integer types (`u8`, `u16`, `u32`, `u64`, `u128`, `i8`, `i16`, `i32`, `i64`, `i128`), strings, and byte arrays
- **ValueCodec**: You implement this for your custom types

### Error Handling

All operations return `typed_sled::error::Result<T>` which provides:
- Detailed error context with schema information
- Proper error chaining from underlying sled operations
- Specific error types for different failure modes

## Examples

See the [examples/](examples/) directory for more comprehensive examples including:
- Multi-table transactions
- Custom serialization strategies
- Error handling patterns

## Contributing

Contributions are generally welcome. If you intend to make larger changes please discuss them in an issue before opening a PR to avoid duplicate work and architectural mismatches.

For more information please see [`CONTRIBUTING.md`](/CONTRIBUTING.md).

## License

This work is dual-licensed under MIT and Apache 2.0. You can choose between one of them if you use this work.
