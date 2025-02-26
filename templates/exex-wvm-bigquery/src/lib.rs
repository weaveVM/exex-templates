pub mod repository;
pub mod types;

use indexmap::IndexMap;
use std::collections::HashMap;

use gcp_bigquery_client::{
    error::BQError,
    model::{
        table::Table, table_data_insert_all_request::TableDataInsertAllRequest,
        table_field_schema::TableFieldSchema, table_schema::TableSchema,
    },
    Client,
};

use phf::phf_ordered_map;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{to_string, Value};

use crate::repository::StateRepository;
use crate::types::ExecutionTipState;
use eyre::{Result, WrapErr};
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::ResultSet;
use thiserror::Error;

use gcp_bigquery_client::yup_oauth2::ServiceAccountKey;

/// Query client
/// Impl for this struct is further below
pub struct BigQueryClient {
    pub client: Client,
    pub project_id: String,
    pub dataset_id: String,
    // drop_tables: bool,
    // table_map: HashMap<String, IndexMap<String, String>>,
}

pub static COMMON_COLUMNS: phf::OrderedMap<&'static str, &'static str> = phf_ordered_map! {
    "indexed_id" => "string",  // will need to generate uuid in rust; postgres allows for autogenerate
    "block_number" => "int",
    "sealed_block_with_senders" => "string",
    "arweave_id" => "string",
    "timestamp" => "int"
};

pub fn prepare_blockstate_table_config() -> HashMap<String, IndexMap<String, String>> {
    let mut table_column_definition: HashMap<String, IndexMap<String, String>> = HashMap::new();
    let merged_column_types: IndexMap<String, String> = COMMON_COLUMNS
        .into_iter()
        .map(|it| (it.0.to_string(), it.1.to_string()))
        .collect();

    table_column_definition.insert("state".to_string(), merged_column_types);
    table_column_definition
}

// ---------------------------------------------------------------------------
// Error Handling
// ---------------------------------------------------------------------------
#[derive(Debug, Error)]
pub enum GcpClientError {
    #[error("BigQuery error: {0}")]
    BigQueryError(#[from] BQError),

    #[error("Invalid credentials JSON: {0}")]
    InvalidCredentialsJson(String),

    #[error("Failed to initialize BigQuery client: {0}")]
    ClientInitError(String),

    #[error("Missing credentials in config: both credentials_path and credentials_json are empty")]
    MissingCredentials,
}

// If you want a separate custom error type for init:
#[derive(Debug, Error)]
pub enum BigQueryError {
    #[error("Invalid credentials JSON: {0}")]
    InvalidCredentialsJson(String),

    #[error("Client init error: {0}")]
    ClientInitError(String),

    #[error("No credentials provided (both credentials_path and credentials_json are empty)")]
    MissingCredentials,
}

#[derive(Debug, Deserialize)]
pub struct BigQueryConfig {
    #[serde(rename = "dropTableBeforeSync")]
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub drop_tables: bool,

    #[serde(rename = "projectId")]
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: String,

    #[serde(rename = "datasetId")]
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_id: String,

    #[serde(rename = "credentialsPath")]
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_path: String,

    #[serde(rename = "credentialsJson")]
    // #[serder(skip_serializing_if = "Option::is_none")]
    pub credentials_json: String,
}

pub async fn init_bigquery_db(
    bigquery_config: &BigQueryConfig,
) -> Result<BigQueryClient, GcpClientError> {
    // Create the underlying GCP client
    let client = BigQueryClient::new(bigquery_config)
        .await
        .map_err(|e| GcpClientError::ClientInitError(format!("{:?}", e)))?;

    // (Optional) Drop existing tables if config says so
    // if bigquery_config.drop_tables {
    //     bq_client.delete_tables().await?;
    // }

    // For example, create the "state" table
    client.create_state_table().await?;

    Ok(client)
}

impl BigQueryClient {
    pub async fn new(cfg: &BigQueryConfig) -> Result<Self, BigQueryError> {
        // 1. Check if we have inline JSON credentials
        if !cfg.credentials_json.trim().is_empty() {
            let key: ServiceAccountKey = serde_json::from_str(&cfg.credentials_json)
                .map_err(|e| BigQueryError::InvalidCredentialsJson(e.to_string()))?;

            let client = Client::from_service_account_key(key, true)
                .await
                .map_err(|e| BigQueryError::ClientInitError(e.to_string()))?;

            return Ok(BigQueryClient {
                client,
                project_id: cfg.project_id.to_string(),
                dataset_id: cfg.dataset_id.to_string(),
            });
        }

        // 2. Otherwise, fallback to file-based credentials
        if !cfg.credentials_path.trim().is_empty() {
            let client = Client::from_service_account_key_file(&cfg.credentials_path)
                .await
                .map_err(|e| BigQueryError::ClientInitError(e.to_string()))?;

            return Ok(BigQueryClient {
                client,
                project_id: cfg.project_id.to_string(),
                dataset_id: cfg.dataset_id.to_string(),
            });
        };

        Err(BigQueryError::MissingCredentials)
    }

    ///
    /// Deletes tables from GCP bigquery, if they exist
    /// Tables are only deleted if the configuration has specified drop_table
    // pub async fn delete_tables(&self) -> Result<(), BQError> {
    //     if self.drop_tables {
    //         for table_name in self.table_map.keys() {
    //             let table_ref = self
    //                 .client
    //                 .table()
    //                 .get(
    //                     self.project_id.as_str(),
    //                     self.dataset_id.as_str(),
    //                     table_name.as_str(),
    //                     None,
    //                 )
    //                 .await;
    //
    //             if let Ok(table) = table_ref {
    //                 // Delete table, since it exists
    //                 let res = table.delete(&self.client).await;
    //                 match res {
    //                     Err(err) => {
    //                         return Err(err)
    //                     }
    //                     Ok(_) => println!("Removed table: {}", table_name),
    //                 }
    //             }
    //         }
    //     }
    //
    //     Ok(())
    // }

    ///
    /// Iterates through all defined tables, calls create_table on each table
    // pub async fn create_tables(&self) -> Result<(), BQError> {
    //     for (table_name, column_map) in self.table_map.iter() {
    //         let res = self.create_table(table_name, column_map).await;
    //         match res {
    //             Ok(..) => {}
    //             Err(err) => return Err(err),
    //         }
    //     }
    //     Ok(())
    // }

    pub async fn create_state_table(&self) -> Result<(), BQError> {
        for (table_name, column_map) in prepare_blockstate_table_config().iter() {
            let res = self.create_table(table_name, column_map).await;
            match res {
                Ok(..) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    ///
    /// Create a single table in GCP bigquery, from configured datatypes
    ///
    /// # Arguments
    ///
    /// * `table_name` - name of table
    /// * `column_map` - map of column names to types
    pub async fn create_table(
        &self,
        table_name: &str,
        column_map: &IndexMap<String, String>,
    ) -> Result<(), BQError> {
        let dataset_ref = self
            .client
            .dataset()
            .get(self.project_id.as_str(), self.dataset_id.as_str())
            .await;
        let table_ref = self
            .client
            .table()
            .get(
                self.project_id.as_str(),
                self.dataset_id.as_str(),
                table_name,
                None,
            )
            .await;

        match table_ref {
            Ok(..) => {
                println!("Table {table_name} already exists, skip creation.");
                return Ok(());
            }
            Err(..) => {
                // Table does not exist (err), create
                // Transform config types to GCP api schema types
                let schema_types: Vec<TableFieldSchema> = column_map
                    .iter()
                    .map(|column| {
                        BigQueryClient::db_type_to_table_field_schema(
                            &column.1.to_string(),
                            &column.0.to_string(),
                        )
                    })
                    .collect();

                let dataset = &mut dataset_ref.as_ref().unwrap();
                let res = dataset
                    .create_table(
                        &self.client,
                        Table::new(
                            self.project_id.as_str(),
                            self.dataset_id.as_str(),
                            table_name,
                            TableSchema::new(schema_types),
                        ),
                    )
                    .await;

                match res {
                    Ok(_) => {
                        println!("Created table in gcp: {}", table_name);
                    }
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }

    ///
    /// Constructs GCP bigquery rows for insertion into gcp tables
    /// The writer will write vector of all rows into dataset
    ///
    /// # Arguments
    ///
    /// * `df` - dataframe
    /// * `table_config` - column to type mapping for table being written to
    pub fn build_bigquery_rowmap_vector(
        &self,
        df: &DataFrame,
        table_config: &IndexMap<String, String>,
    ) -> Vec<HashMap<String, Value>> {
        let column_names = df.get_column_names();
        let mut vec_of_maps: Vec<HashMap<String, Value>> = Vec::with_capacity(df.height());

        for idx in 0..df.height() {
            let mut row_map: HashMap<String, Value> = HashMap::new();

            for name in &column_names {
                let series = df.column(name).expect("Column should exist");
                let value = series.get(idx).expect("Value should exist");

                //  Convert from AnyValue (polars) to Value (generic value wrapper)
                //  The converted-to type is already specified in the inbound configuration
                let col_type = table_config
                    .get(&name.to_string())
                    .expect("Column should exist");
                let transformed_value = match col_type.as_str() {
                    "int" => BigQueryClient::bigquery_anyvalue_numeric_type(&value),
                    "string" => Value::String(value.to_string()),
                    _ => Value::Null,
                };

                row_map.insert((*name).into(), transformed_value);
            }

            vec_of_maps.push(row_map);
        }

        vec_of_maps
    }

    ///
    /// Write vector of rowmaps into GCP.  Construct bulk insertion request and
    /// and insert into target remote table
    ///
    /// # Arguments
    ///
    /// * `table_name` - name of table being operated upon / written to
    /// * `vec_of_rowmaps` - vector of rowmap objects to facilitate write
    pub async fn write_rowmaps_to_gcp(
        &self,
        table_name: &str,
        vec_of_rowmaps: &Vec<HashMap<String, Value>>,
    ) {
        let mut insert_request = TableDataInsertAllRequest::new();
        for row_map in vec_of_rowmaps {
            let _ = insert_request.add_row(None, row_map.clone());
        }

        let result = self
            .client
            .tabledata()
            .insert_all(
                self.project_id.as_str(),
                self.dataset_id.as_str(),
                table_name,
                insert_request,
            )
            .await;

        match result {
            Ok(response) => println!("Success response: {:?}", response),
            Err(response) => println!("Failed, reason: {:?}", response),
        }
    }

    ///
    ///  Maps converted column types and constructs bigquery column
    ///
    /// # Arguments
    ///
    /// * `db_type` - stored datatype configured for this column
    /// * `name` - name of column
    pub fn db_type_to_table_field_schema(db_type: &str, name: &str) -> TableFieldSchema {
        match db_type {
            "int" => TableFieldSchema::integer(name),
            "string" => TableFieldSchema::string(name),
            _ => panic!("Unsupported db type: {}", db_type),
        }
    }

    ///
    ///  Converts AnyValue types to numeric types used in GCP api
    ///
    /// # Arguments
    ///
    /// * `value` - value and type
    pub fn bigquery_anyvalue_numeric_type(value: &AnyValue) -> Value {
        match value {
            AnyValue::Int8(val) => Value::Number((*val).into()),
            AnyValue::Int16(val) => Value::Number((*val).into()),
            AnyValue::Int32(val) => Value::Number((*val).into()),
            AnyValue::Int64(val) => Value::Number((*val).into()),
            AnyValue::UInt8(val) => Value::Number((*val).into()),
            AnyValue::UInt16(val) => Value::Number((*val).into()),
            AnyValue::UInt32(val) => Value::Number((*val).into()),
            AnyValue::UInt64(val) => Value::Number((*val).into()),
            _ => Value::Null,
        }
    }

    pub async fn bq_query(&self, query: String) -> std::result::Result<ResultSet, BQError> {
        let query_req = QueryRequest::new(query);
        let mut q = self.client.job().query(&self.project_id, query_req).await;

        q
    }

    pub async fn bq_query_block(&self, block_id: String) -> Option<ResultSet> {
        let query_request = QueryRequest::new(format!(
            "SELECT * FROM `{}.{}.{}` WHERE block_number = {}",
            self.project_id, self.dataset_id, "state", block_id
        ));

        let mut q = self
            .client
            .job()
            .query(&self.project_id, query_request)
            .await;

        match q {
            Ok(mut rs) => {
                let _ = rs.next_row();

                Some(rs)
            }
            Err(e) => {
                println!("{:?}", e);
                None
            }
        }
    }

    pub async fn bq_query_state(&self, block_id: String) -> Option<String> {
        self.bq_query_block(block_id)
            .await
            .map(|d| d.get_string_by_name("sealed_block_with_senders").ok()?)?
    }

    pub async fn insert_generic(
        &self,
        table_name: &str,
        insert_id: Option<String>,
        data: impl Serialize,
    ) -> eyre::Result<()> {
        let mut insert_request = TableDataInsertAllRequest::new();
        insert_request.add_row(insert_id, data)?;
        let result = self
            .client
            .tabledata()
            .insert_all(
                self.project_id.as_str(),
                self.dataset_id.as_str(),
                table_name,
                insert_request,
            )
            .await;

        match result {
            Ok(response) => {
                println!("Success response: {:?}", response);
                Ok(())
            }
            Err(error) => {
                println!("Failed, reason: {:?}", error);
                Err(eyre::Report::new(error)).wrap_err("Failed to insert data into BigQuery")
            }
        }
    }

    pub async fn bq_insert_state(
        &self,
        table_name: &str,
        state: types::ExecutionTipState,
    ) -> eyre::Result<()> {
        #[derive(Serialize)]
        struct StateRow {
            block_number: u64,
            arweave_id: String,
            sealed_block_with_senders: String,
            block_hash: String,
        }

        let mut insert_request = TableDataInsertAllRequest::new();

        insert_request.add_row(
            None,
            StateRow {
                arweave_id: state.arweave_id,
                block_number: state.block_number,
                sealed_block_with_senders: state.sealed_block_with_senders_serialized,
                block_hash: state.block_hash,
            },
        )?;

        let result = self
            .client
            .tabledata()
            .insert_all(
                self.project_id.as_str(),
                self.dataset_id.as_str(),
                table_name,
                insert_request,
            )
            .await;

        match result {
            Ok(response) => {
                println!("Success response: {:?}", response);
                Ok(())
            }
            Err(error) => {
                println!("Failed, reason: {:?}", error);
                Err(eyre::Report::new(error)).wrap_err("Failed to insert data into BigQuery")
            }
        }
    }
}

pub async fn save_block<T>(
    state_repository: Arc<StateRepository>,
    block: &T,
    block_number: u64,
    arweave_id: String,
    block_hash: String,
) -> eyre::Result<()>
where
    T: ?Sized + Serialize,
{
    let block_str = to_string(block).unwrap();
    let _ = state_repository
        .save(ExecutionTipState {
            block_number,
            arweave_id,
            sealed_block_with_senders_serialized: block_str,
            block_hash,
        })
        .await?;

    Ok(())
}
