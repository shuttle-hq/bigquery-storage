# bigquery-storage
A small wrapper around the [Google BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage).

The BigQuery Storage API allows reading BigQuery tables by serializing their contents into efficient, concurrent streams. The official API supports both binary serialized Arrow and AVRO formats, but this crate only supports outputting Arrow [RecordBatch](arrow::record_batch::RecordBatch) at the moment.

Please refer to the [documentation](https://docs.rs/bigquery-storage) for more information.

## Example

```rust
use bigquery_storage::{Table, Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load the desired secret (here, a service account key)
    let sa_key = yup_oauth2::read_service_account_key("clientsecret.json")
        .await?;

    // 2. Create an Authenticator
    let auth = yup_oauth2::ServiceAccountAuthenticator::builder(sa_key)
        .build()
        .await?;

    // 3. Create a Client
    let mut client = Client::new(auth).await?;

    // Reading the content of a table `bigquery-public-beta:london_bicycles.cycle_stations`
    let test_table = Table::new(
        "bigquery-public-data",
        "london_bicycles",
        "cycle_stations"
    );

    // Create a new ReadSession; the `parent_project_id` is the ID of the GCP project
    // that owns the read job. This does not download any data.
    let mut read_session = client
        .read_session_builder(test_table)
        .parent_project_id("openquery-dev".to_string())
        .build()
        .await?;

    // Take the first stream in the queue for this ReadSession.
    let stream_reader = read_session
        .next_stream()
        .await?
        .expect("did not get any stream");

    // The stream is consumed to yield an Arrow StreamReader, which does download the
    // data.
    let mut arrow_stream_reader = stream_reader
        .into_arrow_reader()
        .await?;

    let arrow_record_batch = arrow_stream_reader
        .next()?
        .expect("no record batch");

    Ok(())
}
```

## License
This project is licensed under the [Apache-2.0 license](LICENSE).
