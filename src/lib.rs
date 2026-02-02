//! # typed-sled
//!
//! A type-safe wrapper around the [`sled`] embedded database.
//!
//! This library provides a schema-based approach to working with [`sled`],
//! ensuring compile-time type safety for keys and values while leveraging
//! efficient binary serialization.
//!
//! ## Features
//!
//! - **Type Safety**: Schema-based table definitions with associated key/value types
//! - **Serialization**: Flexible codec system for efficient binary encoding
//! - **Transactions**: Multi-table atomic operations
//! - **Error Handling**: Comprehensive error types with proper error chaining
//!
//! ## Example
//!
//! ```rust,no_run
//! use rkyv::rancor::Error as RkyvError;
//! use rkyv::util::AlignedVec;
//! use rkyv::{Archive, Deserialize, Serialize, from_bytes, to_bytes};
//! use typed_sled::{CodecError, Schema, SledDb, TreeName, ValueCodec, error::Result};
//!
//! #[derive(Archive, Serialize, Deserialize, Debug)]
//! struct User {
//!     id: u32,
//!     name: String,
//! }
//!
//! #[derive(Debug)]
//! struct UserSchema;
//!
//! impl Schema for UserSchema {
//!     const TREE_NAME: TreeName = TreeName("users");
//!     type Key = u32;
//!     type Value = User;
//! }
//!
//! impl ValueCodec<UserSchema> for User {
//!     fn encode_value(&self) -> typed_sled::CodecResult<Vec<u8>> {
//!         to_bytes::<RkyvError>(self)
//!             .map(|bytes| bytes.into_vec())
//!             .map_err(|e| CodecError::SerializationFailed {
//!                 schema: UserSchema::TREE_NAME.0,
//!                 source: e.into(),
//!             })
//!     }
//!     fn decode_value(buf: &[u8]) -> typed_sled::CodecResult<Self> {
//!         let mut aligned = AlignedVec::<16>::with_capacity(buf.len());
//!         aligned.extend_from_slice(buf);
//!         from_bytes::<User, RkyvError>(&aligned).map_err(|e| CodecError::DeserializationFailed {
//!             schema: UserSchema::TREE_NAME.0,
//!             source: e.into(),
//!         })
//!     }
//! }
//!
//! fn main() -> Result<()> {
//!     let sled_db = sled::open("mydb").unwrap();
//!     let db = SledDb::new(sled_db)?;
//!     let tree = db.get_tree::<UserSchema>()?;
//!
//!     let user = User {
//!         id: 1,
//!         name: "Alice".to_string(),
//!     };
//!     tree.insert(&1, &user)?;
//!
//!     let retrieved = tree.get(&1)?;
//!     println!("{:?}", retrieved);
//!
//!     Ok(())
//! }
//! ```

/// Batch operations for multiple key-value pairs.
pub mod batch;
/// Codec traits and errors for serialization/deserialization.
pub mod codec;
/// Database wrapper around sled with type safety.
pub mod db;
/// Error types and utilities.
pub mod error;
/// Schema trait and tree name definitions.
pub mod schema;
/// Transaction support with retry policies.
pub mod transaction;
/// Type-safe tree operations.
pub mod tree;

#[cfg(test)]
mod test_utils;

// Re-export main types
pub use codec::{CodecError, CodecResult, KeyCodec, ValueCodec};
pub use db::SledDb;
pub use schema::{Schema, TreeName};
pub use tree::SledTree;
