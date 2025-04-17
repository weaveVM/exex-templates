// mod utils;

// use crate::utils::to_brotli;
// use async_trait::async_trait;
// use borsh::BorshSerialize;
// use eyre::Error;
// use wvm_archiver::utils::transaction::send_wvm_calldata;

// pub struct DefaultWvmDataSettler;

// pub enum WvmDataSettlerError {
//     InvalidSendRequest,
// }

// #[async_trait]
// pub trait WvmDataSettler {
//     fn process_block<T: BorshSerialize + ?Sized>(&self, data: &T) -> Result<Vec<u8>, Error> {
//         let borsh_data = borsh::to_vec(&data)?;
//         let brotli_borsh = to_brotli(borsh_data);
//         Ok(brotli_borsh)
//     }

//     async fn send_wvm_calldata(
//         &mut self,
//         block_data: Vec<u8>,
//     ) -> Result<String, WvmDataSettlerError> {
//         send_wvm_calldata(block_data)
//             .await
//             .map_err(|_| WvmDataSettlerError::InvalidSendRequest)
//     }
// }

// impl WvmDataSettler for DefaultWvmDataSettler {}

// #[cfg(test)]
// mod tests {
//     use crate::{WvmDataSettler, WvmDataSettlerError};
//     use async_trait::async_trait;
//     use eyre::Report;
//     use reth::providers::Chain;
//     use reth_exex::ExExNotification;
//     use reth_exex_test_utils::test_exex_context;
//     use std::sync::Arc;
//     use wevm_borsh::block::BorshSealedBlockWithSenders;

//     #[tokio::test]
//     pub async fn test_wvm_da() {
//         struct TestWvmDa {
//             called: bool,
//         }

//         #[async_trait]
//         impl WvmDataSettler for TestWvmDa {
//             async fn send_wvm_calldata(
//                 &mut self,
//                 block_data: Vec<u8>,
//             ) -> Result<String, WvmDataSettlerError> {
//                 self.called = true;
//                 Ok("hello world".to_string())
//             }
//         }

//         let context = test_exex_context().await.unwrap();

//         let chain_def = Chain::from_block(Default::default(), Default::default(), None);

//         context
//             .1
//             .notifications_tx
//             .send(ExExNotification::ChainCommitted {
//                 new: Arc::new(chain_def),
//             })
//             .await
//             .unwrap();

//         let mut wvm_da = TestWvmDa { called: false };

//         drop(context.1);

//         let mut ctx = context.0;

//         while let Some(notification) = ctx.notifications.recv().await {
//             if let Some(committed_chain) = notification.committed_chain() {
//                 let sealed_block_with_senders = committed_chain.tip();
//                 let borsh = BorshSealedBlockWithSenders(sealed_block_with_senders.clone());
//                 let block_data = wvm_da.process_block(&borsh).unwrap();
//                 wvm_da
//                     .send_wvm_calldata(block_data)
//                     .await
//                     .map_err(|e| Report::msg("Invalid Settle Request"))
//                     .unwrap();
//             }
//         }

//         assert!(wvm_da.called);
//     }
// }
