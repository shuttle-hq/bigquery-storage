//! # bigquery-storage
//! A small wrapper around the [Google BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage).
//!
//! The BigQuery Storage API allows reading BigQuery tables by serializing their contents into efficient, concurrent streams. The official API supports both binary serialized Arrow and AVRO formats, but this crate only supports outputting Arrow [RecordBatch](arrow::record_batch::RecordBatch) at the moment.
//! # Usage
//! 0. You will need some form of authentication, provided by an [`Authenticator`](yup_oauth2::authenticator::Authenticator).
//! 1. You will first need to create a [`Client`](crate::client::Client), with [`Client::new`](crate::client::Client::new).
//! 2. Reading tables is done in [read sessions](https://cloud.google.com/bigquery/docs/reference/storage#create_a_session). In this crate, this is handled by [`Client::read_session_builder`](crate::client::Client::read_session_builder).
//! 3. After that you will have a [`ReadSession`](crate::client::ReadSession), which is a small wrapper around a collection of [read streams](https://cloud.google.com/bigquery/docs/reference/storage#read_from_a_session_stream). Go through the streams with [`ReadSession::next_stream`](crate::client::ReadSession::next_stream).
//! 4. Each storage stream is wrapped in a [`RowsStreamReader`](crate::read::RowsStreamReader). This will let you consume the stream into an Arrow [`StreamReader`](arrow::ipc::reader::StreamReader), at which point the data will actually be downloaded.
//! # Example
//! ```rust
//! let service_account_key = yup_oauth2::read_service_account_key("clientsecret.json")
//!     .await
//!     .unwrap();
//!
//! let auth = yup_oauth2::ServiceAccountAuthenticator::builder(service_account_key)
//!     .build()
//!     .await
//!     .unwrap();
//! 
//! let mut client = Client::new(auth).await.unwrap();
//! 
//! let test_table = Table::new(
//!     "bigquery-public-data",
//!     "london_bicycles",
//!     "cycle_stations"
//! );
//! 
//! let mut read_session = client
//!     .read_session(test_table)
//!     .parent_project_id("openquery-dev".to_string())
//!     .build()
//!     .await
//!     .unwrap();
//! 
//! let stream_reader = read_session
//!     .next_stream()
//!     .await
//!     .unwrap()
//!     .expect("did not get any stream");
//! 
//! let mut arrow_stream_reader = stream_reader
//!     .into_arrow_reader()
//!     .await
//!     .unwrap();
//! 
//! arrow_stream_reader
//!     .next()
//!     .unwrap()
//!     .expect("no record batch");
//! ```
//! # Authentication
//! For authentication you need an [Authenticator](yup_oauth2::authenticator::Authenticator), which is provided by the [yup_oauth2](yup_oauth2) crate.
pub use yup_oauth2;

pub mod googleapis {
    //! Codegenerated from [`google.cloud.bigquery.storage.v1`](https://github.com/googleapis/googleapis/tree/master/google/cloud/bigquery/storage/v1).
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
