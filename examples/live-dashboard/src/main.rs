use actix_web::{web, App, HttpServer, HttpRequest, HttpResponse};
use grounddb::Store;
use std::sync::Mutex;
use tokio::sync::broadcast;

/// Shared application state.
pub struct AppState {
    pub store: Mutex<Store>,
    pub tx: broadcast::Sender<SseEvent>,
}

/// A server-sent event payload.
#[derive(Clone, Debug)]
pub struct SseEvent {
    pub view: String,
    pub data: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    log::info!("Starting GroundDB live-dashboard");

    let data_dir = std::env::var("GROUNDDB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
    let host = std::env::var("GROUNDDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("GROUNDDB_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    log::info!("Opening store at: {data_dir}");
    let store = Store::open(&data_dir).expect("Failed to open GroundDB store");

    // Start file watcher
    store.watch().expect("Failed to start file watcher");
    log::info!("File watcher started");

    // Broadcast channel for SSE events (capacity 64)
    let (tx, _rx) = broadcast::channel::<SseEvent>(64);

    // Subscribe to view changes — when process_watcher_events() rebuilds a view,
    // the callback fires with the fresh data and pushes it to the broadcast channel.
    for view_name in &["post_feed", "user_lookup"] {
        let tx = tx.clone();
        let name = view_name.to_string();
        store.on_view_change(
            view_name,
            Box::new(move |rows| {
                let json = serde_json::to_string(rows).unwrap_or_default();
                let _ = tx.send(SseEvent {
                    view: name.clone(),
                    data: json,
                });
            }),
        );
    }

    let state = web::Data::new(AppState {
        store: Mutex::new(store),
        tx: tx.clone(),
    });

    // Spawn a background task to poll watcher events.
    // The on_view_change callbacks push SSE events automatically.
    let poll_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
        loop {
            interval.tick().await;
            let store = poll_state.store.lock().unwrap();
            if let Err(e) = store.process_watcher_events() {
                log::error!("Watcher event processing error: {e}");
            }
        }
    });

    // Resolve static file path relative to the binary's original working directory
    let static_dir = {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").ok();
        match manifest {
            Some(dir) => format!("{}/static", dir),
            None => "static".to_string(),
        }
    };

    log::info!("Listening on {host}:{port}");
    log::info!("Open http://{host}:{port} in your browser");

    let static_dir_clone = static_dir.clone();
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/api/views/{name}", web::get().to(get_view))
            .route("/api/events", web::get().to(sse_handler))
            .route("/api/{collection}", web::post().to(create_document))
            .route("/api/{collection}/{id}", web::delete().to(delete_document))
            .service(actix_files::Files::new("/", &static_dir_clone).index_file("index.html"))
    })
    .bind((host.as_str(), port))?
    .run()
    .await
}

// ── Handlers ─────────────────────────────────────────────────────────

async fn get_view(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> HttpResponse {
    let view_name = path.into_inner();
    let store = state.store.lock().unwrap();
    match store.view_dynamic(&view_name) {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(e) => HttpResponse::NotFound().json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn create_document(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
) -> HttpResponse {
    let collection = path.into_inner();
    let store = state.store.lock().unwrap();
    let mut data = body.into_inner();
    let content = data
        .as_object_mut()
        .and_then(|o| o.remove("content"))
        .and_then(|c| c.as_str().map(|s| s.to_string()));
    match store.insert_dynamic(&collection, data, content.as_deref()) {
        Ok(id) => HttpResponse::Created().json(serde_json::json!({ "id": id })),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn delete_document(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    let (collection, id) = path.into_inner();
    let store = state.store.lock().unwrap();
    match store.delete_dynamic(&collection, &id) {
        Ok(()) => HttpResponse::Ok().json(serde_json::json!({ "ok": true, "deleted": id })),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })),
    }
}

/// SSE endpoint: streams view changes to the browser.
async fn sse_handler(
    _req: HttpRequest,
    state: web::Data<AppState>,
) -> HttpResponse {
    let mut rx = state.tx.subscribe();

    let stream = async_stream::stream! {
        // Send initial data for all views
        {
            let store = state.store.lock().unwrap();
            for view_name in &["post_feed", "user_lookup"] {
                if let Ok(data) = store.view_dynamic(view_name) {
                    let json = serde_json::to_string(&serde_json::json!({
                        "view": view_name,
                        "rows": data
                    })).unwrap_or_default();
                    yield Ok::<_, actix_web::Error>(
                        web::Bytes::from(format!("data: {json}\n\n"))
                    );
                }
            }
        }

        // Stream updates as they arrive from on_view_change callbacks
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let json = serde_json::to_string(&serde_json::json!({
                        "view": event.view,
                        "rows": serde_json::from_str::<serde_json::Value>(&event.data)
                            .unwrap_or(serde_json::Value::Null)
                    })).unwrap_or_default();
                    yield Ok::<_, actix_web::Error>(
                        web::Bytes::from(format!("data: {json}\n\n"))
                    );
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("SSE client lagged by {n} messages");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/event-stream"))
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("X-Accel-Buffering", "no"))
        .streaming(stream)
}
