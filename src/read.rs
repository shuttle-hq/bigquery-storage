use tonic::{Status, Streaming};

use futures::future::ready;
use futures::stream::{Stream, StreamExt, TryStream, TryStreamExt};

use std::io::{Cursor, Write};

use crate::googleapis::{
    read_rows_response::Rows, read_session::Schema, ArrowRecordBatch, ArrowSchema, ReadRowsResponse,
};
use crate::{Error, ReadSession};

#[cfg(feature = "arrow")]
use arrow::{ipc::reader::StreamReader as ArrowStreamReader, record_batch::RecordBatch};

/// Remove the continuation bytes segment of a valid Arrow IPC message
#[cfg(feature = "arrow")]
fn strip_continuation_bytes(msg: &[u8]) -> Result<&[u8], Error> {
    let header = msg
        .get(0..4)
        .ok_or(Error::invalid("arrow message of invalid len"))?;
    if header != [255; 4] {
        Err(Error::invalid("invalid arrow message"))
    } else {
        let tail = msg.get(4..).ok_or(Error::invalid("empty arrow message"))?;
        Ok(tail)
    }
}

#[cfg(feature = "arrow")]
pub type DefaultArrowStreamReader = ArrowStreamReader<Cursor<Vec<u8>>>;

/// A wrapper around a [BigQuery Storage stream](https://cloud.google.com/bigquery/docs/reference/storage#read_from_a_session_stream).
pub struct RowsStreamReader {
    schema: Schema,
    upstream: Streaming<ReadRowsResponse>,
}

impl RowsStreamReader {
    pub(crate) fn new(schema: Schema, upstream: Streaming<ReadRowsResponse>) -> Self {
        Self { schema, upstream }
    }

    /// Consume the entire stream into an Arrow [StreamReader](arrow::ipc::reader::StreamReader).
    #[cfg(feature = "arrow")]
    pub async fn into_arrow_reader(self) -> Result<DefaultArrowStreamReader, Error> {
        let mut serialized_arrow_stream = self
            .upstream
            .map_err(|e| e.into())
            .and_then(|resp| {
                let ReadRowsResponse { rows, .. } = resp;
                let out =
                    rows.ok_or(Error::invalid("no rows received"))
                        .and_then(|rows| match rows {
                            Rows::ArrowRecordBatch(ArrowRecordBatch {
                                serialized_record_batch,
                                ..
                            }) => Ok(serialized_record_batch),
                            _ => {
                                let err = Error::invalid("expected arrow record batch");
                                Err(err)
                            }
                        });
                ready(out)
            })
            .boxed();

        let serialized_schema = match self.schema {
            Schema::ArrowSchema(ArrowSchema { serialized_schema }) => serialized_schema,
            _ => return Err(Error::invalid("expected arrow schema")),
        };

        let mut buf = Vec::new();
        buf.extend(strip_continuation_bytes(serialized_schema.as_slice())?);

        while let Some(msg) = serialized_arrow_stream.next().await {
            let msg = msg?;
            let body = strip_continuation_bytes(msg.as_slice())?;
            buf.extend(body);
        }

        // Arrow StreamReader expects a zero message to signal the end
        // of the stream. Gotta give the people what they want.
        buf.extend(&[0u8; 4]);

        let reader = ArrowStreamReader::try_new(Cursor::new(buf))?;

        Ok(reader)
    }
}
