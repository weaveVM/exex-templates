use crate::types::ExecutionTipState;
use crate::BigQueryClient;
use std::sync::Arc;

pub struct StateRepository {
    pub bq_client: Arc<BigQueryClient>,
}

impl StateRepository {
    pub fn new(bq_client: Arc<BigQueryClient>) -> StateRepository {
        StateRepository { bq_client }
    }

    pub async fn save(&self, state: ExecutionTipState) -> eyre::Result<()> {
        self.bq_client.bq_insert_state("state", state).await
    }

    pub async fn get_by_block_id(&self, block_id: String) -> Option<String> {
        self.bq_client.bq_query_state(block_id).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{BigQueryClient, BigQueryConfig};

    #[tokio::test]
    pub async fn test_get_block_by_id() {
        let curr_dir = std::env::current_dir().unwrap();
        let bq_config_path = curr_dir.join("../../bq-config.json");
        let read_all = std::fs::read(bq_config_path).unwrap();
        let config = serde_json::from_slice::<BigQueryConfig>(&read_all).unwrap();
        let client = BigQueryClient::new(&config).await.unwrap();
        let q = client.bq_query_state("1".to_string()).await;
        assert!(q.is_some());
        println!("{}", q.unwrap());
    }
}
