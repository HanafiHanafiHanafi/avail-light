use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use avail_subxt::{primitives::Header, AvailConfig};
use subxt::OnlineClient;
use tokio::sync::mpsc::Sender;
use tracing::{error, info};

pub async fn finalized_headers(
	rpc_client: OnlineClient<AvailConfig>,
	message_tx: Sender<(Header, Instant)>,
	error_sender: Sender<anyhow::Error>,
) {
	async fn subscribe_and_process(
		rpc_client: OnlineClient<AvailConfig>,
		message_tx: Sender<(Header, Instant)>,
	) -> Result<()> {
		let mut new_heads_sub = rpc_client.rpc().subscribe_finalized_blocks().await?;

		while let Some(message) = new_heads_sub.next().await {
			let received_at = Instant::now();
			if let Ok(header) = message {
				info!(header.number, "Received finalized block header");
				let message = (header, received_at);
				if let Err(error) = message_tx.send(message).await.context("Send failed") {
					error!("Fail to process finalized block header: {error}");
				}
			}
		}
		Err(anyhow!("Finalized blocks subscription disconnected"))
	}

	if let Err(error) = subscribe_and_process(rpc_client, message_tx).await {
		error!("{error}");
		if let Err(error) = error_sender.send(error).await {
			error!("Cannot send error to error channel: {error}");
		}
	}
}