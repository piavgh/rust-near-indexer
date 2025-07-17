use serde::Deserialize;
use tokio::sync::broadcast;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

#[derive(Debug, Clone, Deserialize)]
pub struct ShutdownConfig {
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: std::time::Duration,
}

pub struct ShutdownCoordinator {
    pub config: ShutdownConfig,
    pub token: CancellationToken,
    pub tracker: TaskTracker,
    shutdown_sender: broadcast::Sender<()>,
}

impl ShutdownCoordinator {
    pub fn new(config: ShutdownConfig) -> Self {
        let (shutdown_sender, _) = broadcast::channel(1);
        Self {
            config,
            token: CancellationToken::new(),
            tracker: TaskTracker::new(),
            shutdown_sender,
        }
    }

    // This method is unused for now, so I comment it to prevent warning
    // pub fn subscribe_shutdown(&self) -> broadcast::Receiver<()> {
    //     self.shutdown_sender.subscribe()
    // }

    pub async fn initiate_shutdown(&self) {
        info!("🛑 Initiating graceful shutdown...");

        // Signal all components to start shutting down
        self.token.cancel();
        let _ = self.shutdown_sender.send(());

        // Close the task tracker to prevent new tasks
        self.tracker.close();

        // Wait for all tasks to complete with timeout
        match timeout(self.config.shutdown_timeout, self.tracker.wait()).await {
            Ok(_) => info!("✅ All tasks completed gracefully"),
            Err(_) => info!("⚠️ Shutdown timeout exceeded, some tasks may not have completed"),
        }
    }
}
