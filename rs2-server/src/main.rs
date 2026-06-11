//! rs2-server binary: thin wrapper over the [`rs2_server`] library
//! (shared with `rs2 dev`).

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::args().nth(1).unwrap_or_else(|| "serverConfig.json".to_string());
    rs2_server::run(&config_path).await
}
