//! # bigquery-storage
//! A thin wrapper around Google's [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage).
//! The BigQuery Storage API allows reading BigQuery tables into efficient, concurrent streams.
//! The upstream API supports both serialized Arrow and AVRO formats. This crate supports only supports outputting Arrow at the moment.

pub use yup_oauth2;

pub mod googleapis {
    tonic::include_proto!("google.cloud.bigquery.storage.v1");
}

pub mod client;
pub use client::*;

pub mod read;
pub use read::*;

macro_rules! errors {
    { $(
        $(#[$m:meta])*
        $id:ident($p:path),
    )* } => {
        /// Encompassing error enum for this crate.
        #[derive(Debug)]
        pub enum Error {
            $($(#[$m])* $id($p),)*
        }

        impl std::fmt::Display for Error {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        $(#[$m])*
                        Self::$id(inner) => {
                            write!(f, "{}: {}", stringify!($id), inner)
                        },
                    )*
                }
            }
        }

        impl std::error::Error for Error { }

        $(
            $(#[$m])*
            impl From<$p> for Error {
                fn from(inner: $p) -> Self {
                    Self::$id(inner)
                }
            }
        )*
    };
}

errors! {
    Transport(tonic::transport::Error),
    Status(tonic::Status),
    MetadataEncoding(tonic::metadata::errors::InvalidMetadataValue),
    Auth(yup_oauth2::Error),
    InvalidResponse(String),
    Io(std::io::Error),
    #[cfg(feature = "arrow")]
    Arrow(arrow::error::ArrowError),
}

impl Error {
    pub(crate) fn invalid<S: AsRef<str>>(s: S) -> Self {
        Self::InvalidResponse(s.as_ref().to_string())
    }
}
