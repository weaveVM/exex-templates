use crate::BigQueryClient;
use crate::types::ExecutionTipState;

pub struct StateRepository {
    pub bq_client: BigQueryClient,
}

impl StateRepository {
    pub fn new(bq_client: BigQueryClient) -> StateRepository {
        StateRepository { bq_client }
    }

    pub async fn save(&self, state: ExecutionTipState) -> eyre::Result<()> {
        self.bq_client.bq_insert_state("state", state).await
    }
}
