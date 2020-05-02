//! # bigquery-storage
//! A thin wrapper around Google's [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage) that allows reading BigQuery tables efficiently in concurrent streams.
//! This crate supports only supports Arrow transport at the moment.

pub use yup_oauth2;

pub mod googleapis {
    tonic::include_proto!("google.cloud.bigquery.storage.v1");
}

pub mod client;
pub use client::*;

macro_rules! errors {
    { $($id:ident($p:path),)* } => {
        /// Encompassing error enum for this crate.
        #[derive(Debug)]
        pub enum Error {
            $($id($p),)*
        }

        impl std::fmt::Display for Error {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        Self::$id(inner) => {
                            write!(f, "{}: {}", stringify!($id), inner)
                        },
                    )*
                }
            }
        }

        impl std::error::Error for Error { }

        $(
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
}
