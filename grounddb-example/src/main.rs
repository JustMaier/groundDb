use actix_web::{web, App, HttpServer, HttpResponse, middleware};
use grounddb::Store;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod handlers;

/// Shared application state
pub struct AppState {
    pub store: Store,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    log::info!("Starting GroundDB example server");

    let data_dir = std::env::var("GROUNDDB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
    let host = std::env::var("GROUNDDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("GROUNDDB_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    log::info!("Opening store at: {data_dir}");
    let store = Store::open(&data_dir).expect("Failed to open GroundDB store");

    let state = web::Data::new(AppState { store });

    log::info!("Listening on {host}:{port}");
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .configure(handlers::configure)
    })
    .bind((host.as_str(), port))?
    .run()
    .await
}
