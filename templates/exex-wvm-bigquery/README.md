# How to use

This crate exports all the code related to the Bigquery and the exex itself. 

## Create Dependency

In your `Cargo.toml`, add the following dependency:

```
[dependencies]
exex-wvm-bigquery = { git = "https://github.com/weaveVM/exex-templates?b", branch = "main" }
```


### Create a `BigQueryClient`

```rust
use exex_wvm_bigquery::{BigQueryConfig, init_bigquery_db};

/// A path containing the Big Query Configuration File
let config_path: String =
std::env::var("CONFIG").unwrap_or_else(|_| "./bq-config.json".to_string());
/// {
    /// "dropTableBeforeSync": false,
    /// "projectId": "promising-rock-414216",
    /// "datasetId": "wvm",
    /// "credentialsPath": "/execution/key.json"
///}

let config_file =
std::fs::File::open(config_path).expect("bigquery config path exists");
let reader = std::io::BufReader::new(config_file);

/// Parse Big Query Configuration file into ```BigQueryConfig`
let bq_config: BigQueryConfig =
serde_json::from_reader(reader).expect("bigquery config read from file");

/// Init the Big Query Client
let bigquery_client =
init_bigquery_db(&bq_config).await.expect("bigquery client initialized");
```

*Note* 

> `credentialsPath` from the config file mentioned above is a Google Credentials file.


### Create a StateRepository

```rust
use exex_wvm_bigquery::{StateRepository};

let state_repo = StateRepository::new(bigquery_client);
```

### Saving Block

```rust
 exex_wvm_bigquery::save_block(&StateRepository, &SealedBlockWithSenders, BlockNumber, ArweaveId)
```

#### Saving block example:

https://github.com/weaveVM/wvm-reth/blob/8fc69071341e171b1d3c0a9eecd8f9fb0bc75e24/wvm-apps/wvm-exexed/crates/reth-exexed/src/main.rs#L85


