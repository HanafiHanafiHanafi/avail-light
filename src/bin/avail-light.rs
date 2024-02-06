#![doc = include_str!("../../README.md")]

use avail_light::{
	network::p2p,
	telemetry::{self, otlp::MetricAttributes},
	types::{CliOpts, IdentityConfig, LibP2PConfig, RuntimeConfig},
};
use clap::Parser;
use color_eyre::{
	eyre::{eyre, WrapErr},
	Result,
};
use libp2p::{multiaddr::Protocol, Multiaddr};
use std::{fs, net::Ipv4Addr, path::Path, sync::Arc};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, metadata::ParseLevelError, warn, Level, Subscriber};
use tracing_subscriber::{fmt::format, EnvFilter, FmtSubscriber};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const CLIENT_ROLE: &str = if cfg!(feature = "crawl") {
	"crawler"
} else {
	"lightnode"
};

/// Light Client for Avail Blockchain

fn json_subscriber(log_level: Level) -> impl Subscriber + Send + Sync {
	FmtSubscriber::builder()
		.with_env_filter(EnvFilter::new(format!("avail_light={log_level}")))
		.event_format(format::json())
		.finish()
}

fn default_subscriber(log_level: Level) -> impl Subscriber + Send + Sync {
	FmtSubscriber::builder()
		.with_env_filter(EnvFilter::new(format!("avail_light={log_level}")))
		.with_span_events(format::FmtSpan::CLOSE)
		.finish()
}

fn parse_log_level(log_level: &str, default: Level) -> (Level, Option<ParseLevelError>) {
	log_level
		.to_uppercase()
		.parse::<Level>()
		.map(|log_level| (log_level, None))
		.unwrap_or_else(|parse_err| (default, Some(parse_err)))
}

async fn run() -> Result<()> {
	let opts = CliOpts::parse();

	let mut cfg: RuntimeConfig = RuntimeConfig::default();
	cfg.load_runtime_config(&opts)?;

	let (log_level, parse_error) = parse_log_level(&cfg.log_level, Level::INFO);

	if cfg.log_format_json {
		tracing::subscriber::set_global_default(json_subscriber(log_level))
			.expect("global json subscriber is set")
	} else {
		tracing::subscriber::set_global_default(default_subscriber(log_level))
			.expect("global default subscriber is set")
	}

	let identity_cfg =
		IdentityConfig::load_or_init(&opts.identity, opts.avail_passphrase.as_deref())?;
	info!("Identity loaded from {}", &opts.identity);

	let client_role = if cfg.is_fat_client() {
		info!("Fat client mode");
		"fatnode"
	} else {
		CLIENT_ROLE
	};

	let version = clap::crate_version!();
	info!("Running Avail light client version: {version}. Role: {client_role}.");
	info!("Using config: {cfg:?}");
	info!("Avail address is: {}", &identity_cfg.avail_address);

	if let Some(error) = parse_error {
		warn!("Using default log level: {}", error);
	}

	if opts.clean && Path::new(&cfg.avail_path).exists() {
		info!("Cleaning up local state directory");
		fs::remove_dir_all(&cfg.avail_path).wrap_err("Failed to remove local state directory")?;
	}

	if cfg.bootstraps.is_empty() {
		Err(eyre!("Bootstrap node list must not be empty. Either use a '--network' flag or add a list of bootstrap nodes in the configuration file"))?
	}

	let cfg_libp2p: LibP2PConfig = (&cfg).into();
	let (id_keys, peer_id) = p2p::keypair(&cfg_libp2p)?;

	let metric_attributes = MetricAttributes {
		role: client_role.into(),
		peer_id,
		ip: RwLock::new("".to_string()),
		multiaddress: RwLock::new("".to_string()), // Default value is empty until first processed block triggers an update,
		origin: cfg.origin.clone(),
		avail_address: identity_cfg.avail_address.clone(),
		operating_mode: cfg.operation_mode.to_string(),
		partition_size: cfg
			.block_matrix_partition
			.map(|_| {
				format!(
					"{}/{}",
					cfg.block_matrix_partition
						.expect("partition doesn't exist")
						.number,
					cfg.block_matrix_partition
						.expect("partition doesn't exist")
						.fraction
				)
			})
			.unwrap_or("n/a".to_string()),
	};

	let ot_metrics = Arc::new(
		telemetry::otlp::initialize(cfg.ot_collector_endpoint.clone(), metric_attributes)
			.wrap_err("Unable to initialize OpenTelemetry service")?,
	);

	// Create sender channel for P2P event loop commands
	let (p2p_event_loop_sender, p2p_event_loop_receiver) = mpsc::unbounded_channel();

	let p2p_event_loop = p2p::EventLoop::new(cfg_libp2p, &id_keys, cfg.is_fat_client());
	let task_event_loop = tokio::spawn(p2p_event_loop.run(ot_metrics, p2p_event_loop_receiver));

	let p2p_client = p2p::Client::new(
		p2p_event_loop_sender,
		cfg.dht_parallelization_limit,
		cfg.kad_record_ttl,
	);

	// Start listening on provided port
	let port = cfg.port;
	p2p_client
		.start_listening(
			Multiaddr::empty()
				.with(Protocol::from(Ipv4Addr::UNSPECIFIED))
				.with(Protocol::Tcp(port)),
		)
		.await
		.wrap_err("Listening on TCP not to fail.")?;
	info!("TCP listener started on port {port}");

	let p2p_clone = p2p_client.to_owned();
	let cfg_clone = cfg.to_owned();
	let task_bootstrap = tokio::spawn(async move {
		info!("Bootstraping the DHT with bootstrap nodes...");
		let bs_result = p2p_clone
			.bootstrap_on_startup(cfg_clone.bootstraps.iter().map(Into::into).collect())
			.await;
		match bs_result {
			Ok(_) => {
				info!("Bootstrap done.");
			},
			Err(e) => {
				error!("Bootstrap process: {e:?}.");
			},
		}
	});

	let (_, _) = tokio::join!(task_event_loop, task_bootstrap);

	Ok(())
}

#[tokio::main]
pub async fn main() -> Result<()> {
	run().await
}
