use anyhow::Result;
use simple_redis::{Backend, network::stream_handle};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";

    info!("Simple-redis Listening on {}", addr);
    let listener = TcpListener::bind(&addr).await?;
    let backend = Backend::new();

    loop {
        let (socket, raddr) = listener.accept().await?;
        info!("Accepted connection from {}", raddr);
        let clone_backend = backend.clone();
        // tokio task
        tokio::spawn(async move {
            match stream_handle(socket, clone_backend).await {
                Ok(_) => {
                    info!("Connection from {} closed", raddr);
                }
                Err(e) => {
                    warn!("Connection from {} closed with error: {}", raddr, e);
                }
            }
        });
    }
}
