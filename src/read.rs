use tonic::{Status, Streaming};

use futures::stream::{Stream, StreamExt, TryStream, TryStreamExt};
use futures::future::ready;

use std::io::{Cursor, Write};

use crate::{ReadSession, Error};
use crate::googleapis::{ArrowSchema, ArrowRecordBatch, read_rows_response::Rows, read_session::Schema, ReadRowsResponse};

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
        let tail = msg.get(4..)
            .ok_or(Error::invalid("empty arrow message"))?;
        Ok(tail)
    }
}

#[cfg(feature = "arrow")]
pub type DefaultArrowStreamReader = ArrowStreamReader<Cursor<Vec<u8>>>;

pub struct RowsStreamReader {
    schema: Schema,
    upstream: Streaming<ReadRowsResponse>
}

impl RowsStreamReader {
    pub fn new(schema: Schema, upstream: Streaming<ReadRowsResponse>) -> Self {
        Self { schema, upstream }
    }

    #[cfg(feature = "arrow")]
    pub async fn into_arrow_reader(self) -> Result<DefaultArrowStreamReader, Error> {
        let mut serialized_arrow_stream = self.upstream
            .map_err(|e| e.into())
            .and_then(|resp| {
                let ReadRowsResponse { rows, .. } = resp;
                let out = rows
                    .ok_or(Error::invalid("no rows received"))
                    .and_then(|rows| {
                        match rows {
                            Rows::ArrowRecordBatch(ArrowRecordBatch {
                                serialized_record_batch, ..
                            }) => {
                                Ok(serialized_record_batch)
                            },
                            _ => {
                                let err = Error::invalid("expected arrow record batch");
                                Err(err)
                            }
                        }
                    });
                ready(out)
            })
            .boxed();

        let serialized_schema = match self.schema {
            Schema::ArrowSchema(ArrowSchema { serialized_schema }) => {
                serialized_schema
            },
            _ => return Err(Error::invalid("expected arrow schema"))
        };

        let mut buf = Vec::new();
        buf.extend(strip_continuation_bytes(serialized_schema.as_slice())?);

        while let Some(msg) = serialized_arrow_stream.next().await {
            let msg = msg?;
            let body = strip_continuation_bytes(msg.as_slice())?;
            buf.extend(body);
        }

        let reader = ArrowStreamReader::try_new(Cursor::new(buf))?;

        Ok(reader)
    }
}
