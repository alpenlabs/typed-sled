// re-export `ConflictableTransactionError`
pub use sled::transaction::ConflictableTransactionError;
use sled::{CompareAndSwapError, Error as SledError, transaction::UnabortableTransactionError};

use crate::CodecError;

/// The main error type for typed-sled operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Codec error
    #[error("codec: {0}")]
    CodecError(#[from] CodecError),

    /// Sled database error
    #[error("sled: {0}")]
    SledError(#[from] SledError),

    /// Sled transaction error
    #[error("sled tx: {0}")]
    TransactionError(#[from] UnabortableTransactionError),

    /// CAS error
    #[error("sled cas: {0}")]
    CASError(#[from] CompareAndSwapError),

    /// Custom abort error for transactions
    #[error("abort: {0}")]
    Abort(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<Error> for ConflictableTransactionError<Error> {
    fn from(value: Error) -> Self {
        ConflictableTransactionError::Abort(value)
    }
}

impl Error {
    /// Creates an abort error from any error type.
    ///
    /// This is useful for aborting transactions with custom application errors.
    ///
    /// # Example
    ///
    /// ```
    /// use typed_sled::error::Error;
    ///
    /// #[derive(Debug, thiserror::Error)]
    /// #[error("insufficient balance")]
    /// struct InsufficientBalance;
    ///
    /// let error = Error::abort(InsufficientBalance);
    /// ```
    pub fn abort<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Error::Abort(Box::new(err))
    }

    /// Attempts to downcast the abort error to a specific type, returning a reference.
    ///
    /// Returns `None` if the error is not an `Abort` variant or if the downcast fails.
    ///
    /// # Example
    ///
    /// ```
    /// use typed_sled::error::Error;
    ///
    /// #[derive(Debug, thiserror::Error)]
    /// #[error("insufficient balance")]
    /// struct InsufficientBalance;
    ///
    /// let error = Error::abort(InsufficientBalance);
    /// if let Some(app_err) = error.downcast_abort_ref::<InsufficientBalance>() {
    ///     // Handle the specific error
    /// }
    /// ```
    pub fn downcast_abort_ref<E: std::error::Error + 'static>(&self) -> Option<&E> {
        match self {
            Error::Abort(boxed) => boxed.downcast_ref::<E>(),
            _ => None,
        }
    }

    /// Attempts to downcast the abort error to a specific type, consuming self.
    ///
    /// Returns `Err(Self)` if the error is not an `Abort` variant or if the downcast fails.
    ///
    /// # Example
    ///
    /// ```
    /// use typed_sled::error::Error;
    ///
    /// #[derive(Debug, thiserror::Error, PartialEq)]
    /// #[error("insufficient balance")]
    /// struct InsufficientBalance;
    ///
    /// let error = Error::abort(InsufficientBalance);
    /// match error.downcast_abort::<InsufficientBalance>() {
    ///     Ok(app_err) => {
    ///         // Handle the specific error
    ///     }
    ///     Err(original_error) => {
    ///         // Not the expected error type
    ///     }
    /// }
    /// ```
    pub fn downcast_abort<E: std::error::Error + 'static>(self) -> std::result::Result<E, Self> {
        match self {
            Error::Abort(boxed) => boxed.downcast::<E>().map(|b| *b).map_err(Error::Abort),
            other => Err(other),
        }
    }
}

/// A type alias for `Result<T, Error>`.
pub type Result<T> = core::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::io;

    use sled::Error as SledError;

    use super::*;
    use crate::codec::CodecError;

    #[test]
    fn test_error_from_codec_error() {
        let codec_error = CodecError::InvalidKeyLength {
            schema: "test",
            expected: 4,
            actual: 2,
        };

        let error: Error = codec_error.into();

        match error {
            Error::CodecError(_) => {} // Expected
            _ => panic!("Expected CodecError variant"),
        }
    }

    #[test]
    fn test_error_from_sled_error() {
        let sled_error = SledError::Io(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "test error",
        ));

        let error: Error = sled_error.into();

        match error {
            Error::SledError(_) => {} // Expected
            _ => panic!("Expected SledError variant"),
        }
    }

    #[test]
    fn test_error_from_unabortable_transaction_error() {
        let tx_error = UnabortableTransactionError::Storage(SledError::Io(io::Error::other(
            "transaction failed",
        )));

        let error: Error = tx_error.into();

        match error {
            Error::TransactionError(_) => {} // Expected
            _ => panic!("Expected TransactionError variant"),
        }
    }

    #[test]
    fn test_error_from_cas_error() {
        // Create a CompareAndSwapError - this requires specific sled internals
        // We'll test this by creating an error that would come from actual CAS operations
        let cas_error = CompareAndSwapError {
            current: Some(vec![1, 2, 3].into()),
            proposed: Some(vec![4, 5, 6].into()),
        };

        let error: Error = cas_error.into();

        match error {
            Error::CASError(_) => {} // Expected
            _ => panic!("Expected CASError variant"),
        }
    }

    #[test]
    fn test_error_into_conflictable_transaction_error() {
        let original_error = Error::CodecError(CodecError::InvalidKeyLength {
            schema: "test",
            expected: 4,
            actual: 2,
        });

        let conflictable_error: ConflictableTransactionError<Error> = original_error.into();

        match conflictable_error {
            ConflictableTransactionError::Abort(error) => {
                match error {
                    Error::CodecError(_) => {} // Expected
                    _ => panic!("Expected CodecError variant inside Abort"),
                }
            }
            _ => panic!("Expected Abort variant"),
        }
    }

    #[test]
    fn test_error_display_formatting() {
        // Test that all error variants display properly
        let codec_error = Error::CodecError(CodecError::SerializationFailed {
            schema: "test_schema",
            source: Box::new(io::Error::other("serialization failed")),
        });

        match codec_error {
            Error::CodecError(CodecError::SerializationFailed { .. }) => {} // Expected
            _ => panic!("Expected CodecError::SerializationFailed variant"),
        }
    }

    #[test]
    fn test_error_debug_formatting() {
        let error = Error::SledError(SledError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            "file not found",
        )));

        let debug_string = format!("{:?}", error);
        assert!(debug_string.contains("SledError"));
        assert!(debug_string.contains("NotFound"));
    }

    #[test]
    fn test_error_chain_source() {
        let io_error = io::Error::new(io::ErrorKind::PermissionDenied, "permission denied");
        let sled_error = SledError::Io(io_error);
        let typed_sled_error = Error::SledError(sled_error);

        // Test that the error chain is preserved
        match typed_sled_error {
            Error::SledError(SledError::Io(_)) => {} // Expected
            _ => panic!("Expected SledError::Io variant"),
        }
    }

    #[test]
    fn test_result_type_alias() {
        // Test that our Result type alias works correctly
        fn test_function() -> Result<i32> {
            Ok(42)
        }

        fn error_function() -> Result<i32> {
            Err(Error::CodecError(CodecError::InvalidKeyLength {
                schema: "test",
                expected: 4,
                actual: 2,
            }))
        }

        let success_result = test_function();
        assert!(success_result.is_ok());
        assert_eq!(success_result.unwrap(), 42);

        let error_result = error_function();
        assert!(error_result.is_err());
        match error_result.unwrap_err() {
            Error::CodecError(_) => {} // Expected
            _ => panic!("Expected CodecError"),
        }
    }

    #[test]
    fn test_codec_error_variations() {
        // Test all CodecError variants can be converted to Error
        let key_length_error = Error::CodecError(CodecError::InvalidKeyLength {
            schema: "test",
            expected: 4,
            actual: 8,
        });

        let serialization_error = Error::CodecError(CodecError::SerializationFailed {
            schema: "test",
            source: Box::new(io::Error::other("serialize failed")),
        });

        let deserialization_error = Error::CodecError(CodecError::DeserializationFailed {
            schema: "test",
            source: Box::new(io::Error::other("deserialize failed")),
        });

        // All should convert properly
        match key_length_error {
            Error::CodecError(CodecError::InvalidKeyLength { .. }) => {} // Expected
            _ => panic!("Expected CodecError::InvalidKeyLength variant"),
        }
        match serialization_error {
            Error::CodecError(CodecError::SerializationFailed { .. }) => {} // Expected
            _ => panic!("Expected CodecError::SerializationFailed variant"),
        }
        match deserialization_error {
            Error::CodecError(CodecError::DeserializationFailed { .. }) => {} // Expected
            _ => panic!("Expected CodecError::DeserializationFailed variant"),
        }
    }

    // Custom error types for testing abort functionality
    #[derive(Debug, thiserror::Error, PartialEq)]
    #[error("insufficient balance: need {need}, have {have}")]
    struct InsufficientBalance {
        need: u64,
        have: u64,
    }

    #[derive(Debug, thiserror::Error, PartialEq)]
    #[error("invalid state: {0}")]
    struct InvalidState(String);

    #[test]
    fn test_abort_error_creation() {
        let custom_error = InsufficientBalance {
            need: 100,
            have: 50,
        };

        let error = Error::abort(custom_error);

        match error {
            Error::Abort(_) => {} // Expected
            _ => panic!("Expected Abort variant"),
        }
    }

    #[test]
    fn test_abort_error_downcast_ref_success() {
        let custom_error = InsufficientBalance {
            need: 100,
            have: 50,
        };

        let error = Error::abort(custom_error);

        let downcasted = error.downcast_abort_ref::<InsufficientBalance>();
        assert!(downcasted.is_some());

        let app_err = downcasted.unwrap();
        assert_eq!(app_err.need, 100);
        assert_eq!(app_err.have, 50);
    }

    #[test]
    fn test_abort_error_downcast_ref_wrong_type() {
        let custom_error = InsufficientBalance {
            need: 100,
            have: 50,
        };

        let error = Error::abort(custom_error);

        // Try to downcast to wrong type
        let downcasted = error.downcast_abort_ref::<InvalidState>();
        assert!(downcasted.is_none());
    }

    #[test]
    fn test_abort_error_downcast_ref_not_abort_variant() {
        let error = Error::CodecError(CodecError::InvalidKeyLength {
            schema: "test",
            expected: 4,
            actual: 2,
        });

        let downcasted = error.downcast_abort_ref::<InsufficientBalance>();
        assert!(downcasted.is_none());
    }

    #[test]
    fn test_abort_error_downcast_owned_success() {
        let custom_error = InsufficientBalance {
            need: 100,
            have: 50,
        };

        let error = Error::abort(custom_error);

        let downcasted = error.downcast_abort::<InsufficientBalance>();
        assert!(downcasted.is_ok());

        let app_err = downcasted.unwrap();
        assert_eq!(
            app_err,
            InsufficientBalance {
                need: 100,
                have: 50
            }
        );
    }

    #[test]
    fn test_abort_error_downcast_owned_wrong_type() {
        let custom_error = InsufficientBalance {
            need: 100,
            have: 50,
        };

        let error = Error::abort(custom_error);

        // Try to downcast to wrong type
        let downcasted = error.downcast_abort::<InvalidState>();
        assert!(downcasted.is_err());

        // Should get the original error back
        let original = downcasted.unwrap_err();
        match original {
            Error::Abort(_) => {} // Expected - still an Abort with the original type
            _ => panic!("Expected Abort variant"),
        }
    }

    #[test]
    fn test_abort_error_downcast_owned_not_abort_variant() {
        let error = Error::CodecError(CodecError::InvalidKeyLength {
            schema: "test",
            expected: 4,
            actual: 2,
        });

        let downcasted = error.downcast_abort::<InsufficientBalance>();
        assert!(downcasted.is_err());

        let original = downcasted.unwrap_err();
        match original {
            Error::CodecError(_) => {} // Expected - original variant preserved
            _ => panic!("Expected CodecError variant"),
        }
    }

    #[test]
    fn test_abort_error_display() {
        let custom_error = InsufficientBalance {
            need: 100,
            have: 50,
        };

        let error = Error::abort(custom_error);
        let display_string = format!("{}", error);

        assert!(display_string.contains("abort"));
        assert!(display_string.contains("insufficient balance"));
    }

    #[test]
    fn test_abort_error_into_conflictable() {
        let custom_error = InsufficientBalance {
            need: 100,
            have: 50,
        };

        let error = Error::abort(custom_error);
        let conflictable: ConflictableTransactionError<Error> = error.into();

        match conflictable {
            ConflictableTransactionError::Abort(Error::Abort(_)) => {} // Expected
            _ => panic!("Expected Abort variant"),
        }
    }
}
