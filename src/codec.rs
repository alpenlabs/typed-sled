use std::{borrow::Borrow, marker::PhantomData, ops::Deref};

use rkyv::{Portable, api::high::HighValidator, bytecheck::CheckBytes, rancor::Error as RkyvError};
use sled::IVec;
use thiserror::Error;

use crate::schema::Schema;

/// Errors that can occur during key/value encoding or decoding.
#[derive(Debug, Error)]
pub enum CodecError {
    /// Key has invalid length for the expected type.
    #[error("invalid key length in '{schema}' (expected {expected} bytes, got {actual})")]
    InvalidKeyLength {
        /// The schema name where the error occurred.
        schema: &'static str,
        /// The expected key length in bytes.
        expected: usize,
        /// The actual key length in bytes.
        actual: usize,
    },
    /// Value serialization failed.
    #[error("failed to serialize schema '{schema}' value")]
    SerializationFailed {
        /// The schema name where the error occurred.
        schema: &'static str,
        /// The underlying serialization error.
        #[source]
        source: Box<dyn std::error::Error>,
    },
    /// Value deserialization failed.
    #[error("failed to deserialize schema '{schema}' value")]
    DeserializationFailed {
        /// The schema name where the error occurred.
        schema: &'static str,
        /// The underlying deserialization error.
        #[source]
        source: Box<dyn std::error::Error>,
    },
    /// I/O error during codec operations.
    #[error("io: {0}")]
    IO(#[from] std::io::Error),

    /// Other
    #[error("other: {0}")]
    Other(String),
}

/// Result type for codec operations.
pub type CodecResult<T> = Result<T, CodecError>;

/// Zero-copy view over an archived `rkyv` value backed by an owned byte buffer.
///
/// The buffer must satisfy `rkyv`'s alignment requirements. When bytes come
/// from an unaligned source such as `sled::IVec`, copy them into an
/// [`rkyv::util::AlignedVec`] before constructing the view.
#[derive(Clone)]
pub struct RkyvView<B, P> {
    buf: B,
    // Keep `P` covariant without implying that the view owns a `P`.
    _phantom: PhantomData<fn() -> P>,
}

impl<B, P> RkyvView<B, P>
where
    B: AsRef<[u8]>,
    P: Portable + for<'a> CheckBytes<HighValidator<'a, RkyvError>>,
{
    /// Validates the archived bytes and creates a new zero-copy view.
    pub fn try_new(buf: B) -> Result<Self, RkyvError> {
        rkyv::access::<P, RkyvError>(buf.as_ref())?;
        Ok(Self {
            buf,
            _phantom: PhantomData,
        })
    }

    /// Returns the owned buffer backing this archived view.
    pub fn into_inner(self) -> B {
        self.buf
    }
}

impl<B, P> AsRef<P> for RkyvView<B, P>
where
    B: AsRef<[u8]>,
    P: Portable + for<'a> CheckBytes<HighValidator<'a, RkyvError>>,
{
    fn as_ref(&self) -> &P {
        rkyv::access::<P, RkyvError>(self.buf.as_ref())
            .expect("RkyvView validates archived bytes at construction")
    }
}

impl<B, P> Borrow<P> for RkyvView<B, P>
where
    B: AsRef<[u8]>,
    P: Portable + for<'a> CheckBytes<HighValidator<'a, RkyvError>>,
{
    fn borrow(&self) -> &P {
        self.as_ref()
    }
}

impl<B, P> Deref for RkyvView<B, P>
where
    B: AsRef<[u8]>,
    P: Portable + for<'a> CheckBytes<HighValidator<'a, RkyvError>>,
{
    type Target = P;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<B, P> std::fmt::Debug for RkyvView<B, P>
where
    B: AsRef<[u8]>,
    P: Portable + std::fmt::Debug + for<'a> CheckBytes<HighValidator<'a, RkyvError>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RkyvView").field(self.as_ref()).finish()
    }
}

/// Trait for encoding and decoding keys for a specific schema.
pub trait KeyCodec<S: Schema>: Sized {
    /// Encodes the key into bytes.
    fn encode_key(&self) -> CodecResult<Vec<u8>>;
    /// Decodes the key from bytes.
    fn decode_key(buf: &[u8]) -> CodecResult<Self>;
}

/// Trait for encoding and decoding values for a specific schema.
pub trait ValueCodec<S: Schema>: Sized {
    /// The value representation returned by [`ValueCodec::decode_value`].
    type Decoded;

    /// Encodes the value into bytes.
    fn encode_value(&self) -> CodecResult<Vec<u8>>;

    /// Decodes the value from the raw bytes stored in sled.
    fn decode_value(buf: IVec) -> CodecResult<Self::Decoded>;
}

macro_rules! derive_key_codec_for_integers {
    ($($int:ty), *) => {
        $(impl<T: Schema> KeyCodec<T> for $int {
            fn encode_key(&self) -> CodecResult<Vec<u8>> {
                Ok(self.to_be_bytes().into())
            }

            fn decode_key(buf: &[u8]) -> CodecResult<Self> {
                const SIZE: usize = std::mem::size_of::<$int>();
                if buf.len() != SIZE {
                    return Err(CodecError::InvalidKeyLength {
                        schema: T::TREE_NAME.0,
                        expected: SIZE,
                        actual: buf.len(),
                    });
                }
                let mut bytes = [0u8; SIZE];
                bytes.copy_from_slice(buf);
                Ok(<$int>::from_be_bytes(bytes))
            }
        })*
    };
}

derive_key_codec_for_integers!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);
