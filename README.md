# bigquery-storage
A small wrapper around the [Google BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage).

The BigQuery Storage API allows reading BigQuery tables by serializing their contents into efficient, concurrent streams. The official API supports both binary serialized Arrow and AVRO formats, but this crate only supports outputting Arrow [RecordBatch](arrow::record_batch::RecordBatch) at the moment.

Please refer to the [API docs](https://docs.rs/bigquery-storage) for more information.
