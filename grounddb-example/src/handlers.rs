use actix_web::{web, HttpResponse};
use serde::Deserialize;
use std::collections::HashMap;

use crate::AppState;

/// Configure all API routes
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api")
            // Status
            .route("/status", web::get().to(status))
            // Users
            .route("/users", web::get().to(list_users))
            .route("/users", web::post().to(create_user))
            .route("/users/{id}", web::get().to(get_user))
            .route("/users/{id}", web::put().to(update_user))
            .route("/users/{id}", web::delete().to(delete_user))
            // Posts
            .route("/posts", web::get().to(list_posts))
            .route("/posts", web::post().to(create_post))
            .route("/posts/{id}", web::get().to(get_post))
            .route("/posts/{id}", web::put().to(update_post))
            .route("/posts/{id}", web::delete().to(delete_post))
            // Comments
            .route("/comments", web::get().to(list_comments))
            .route("/comments", web::post().to(create_comment))
            .route("/comments/{id}", web::get().to(get_comment))
            .route("/comments/{id}", web::put().to(update_comment))
            .route("/comments/{id}", web::delete().to(delete_comment))
            // Events
            .route("/events", web::get().to(list_events))
            .route("/events", web::post().to(create_event))
            .route("/events/{id}", web::get().to(get_event))
            .route("/events/{id}", web::put().to(update_event))
            .route("/events/{id}", web::delete().to(delete_event))
            // Views
            .route("/views/post_feed", web::get().to(view_post_feed))
            .route("/views/user_lookup", web::get().to(view_user_lookup))
            .route("/views/recent_activity", web::get().to(view_recent_activity))
            .route("/views/post_comments", web::get().to(view_post_comments)),
    );
}

// ── Helpers ─────────────────────────────────────────────────────────

fn ok_json(value: serde_json::Value) -> HttpResponse {
    HttpResponse::Ok().json(value)
}

fn created_json(value: serde_json::Value) -> HttpResponse {
    HttpResponse::Created().json(value)
}

fn err_response(e: grounddb::GroundDbError) -> HttpResponse {
    match &e {
        grounddb::GroundDbError::NotFound { .. } => {
            HttpResponse::NotFound().json(serde_json::json!({
                "error": e.to_string()
            }))
        }
        grounddb::GroundDbError::Validation(_) => {
            HttpResponse::BadRequest().json(serde_json::json!({
                "error": e.to_string()
            }))
        }
        grounddb::GroundDbError::PathConflict { .. } => {
            HttpResponse::Conflict().json(serde_json::json!({
                "error": e.to_string()
            }))
        }
        grounddb::GroundDbError::ReferentialIntegrity(_) => {
            HttpResponse::Conflict().json(serde_json::json!({
                "error": e.to_string()
            }))
        }
        _ => {
            log::error!("Internal error: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Internal server error"
            }))
        }
    }
}

// ── Status ──────────────────────────────────────────────────────────

async fn status(state: web::Data<AppState>) -> HttpResponse {
    match state.store.status() {
        Ok(v) => ok_json(v),
        Err(e) => err_response(e),
    }
}

// ── Generic CRUD handlers ───────────────────────────────────────────

async fn list_collection(state: &AppState, collection: &str) -> HttpResponse {
    let filters = HashMap::new();
    match state.store.list_dynamic(collection, &filters) {
        Ok(v) => ok_json(v),
        Err(e) => err_response(e),
    }
}

async fn get_document(state: &AppState, collection: &str, id: &str) -> HttpResponse {
    match state.store.get_dynamic(collection, id) {
        Ok(v) => ok_json(v),
        Err(e) => err_response(e),
    }
}

async fn create_document(
    state: &AppState,
    collection: &str,
    body: serde_json::Value,
) -> HttpResponse {
    let content = body.get("content").and_then(|c| c.as_str()).map(|s| s.to_string());
    let mut data = body;
    // Remove "content" from the data object since it's passed separately
    if let Some(obj) = data.as_object_mut() {
        obj.remove("content");
    }
    match state.store.insert_dynamic(collection, data, content.as_deref()) {
        Ok(id) => created_json(serde_json::json!({ "id": id })),
        Err(e) => err_response(e),
    }
}

async fn update_document(
    state: &AppState,
    collection: &str,
    id: &str,
    body: serde_json::Value,
) -> HttpResponse {
    match state.store.update_dynamic(collection, id, body) {
        Ok(()) => ok_json(serde_json::json!({ "ok": true, "id": id })),
        Err(e) => err_response(e),
    }
}

async fn delete_document(state: &AppState, collection: &str, id: &str) -> HttpResponse {
    match state.store.delete_dynamic(collection, id) {
        Ok(()) => ok_json(serde_json::json!({ "ok": true, "deleted": id })),
        Err(e) => err_response(e),
    }
}

// ── Users ───────────────────────────────────────────────────────────

async fn list_users(state: web::Data<AppState>) -> HttpResponse {
    list_collection(&state, "users").await
}

async fn get_user(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    get_document(&state, "users", &path).await
}

async fn create_user(
    state: web::Data<AppState>,
    body: web::Json<serde_json::Value>,
) -> HttpResponse {
    create_document(&state, "users", body.into_inner()).await
}

async fn update_user(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
) -> HttpResponse {
    update_document(&state, "users", &path, body.into_inner()).await
}

async fn delete_user(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    delete_document(&state, "users", &path).await
}

// ── Posts ───────────────────────────────────────────────────────────

async fn list_posts(state: web::Data<AppState>) -> HttpResponse {
    list_collection(&state, "posts").await
}

async fn get_post(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    get_document(&state, "posts", &path).await
}

async fn create_post(
    state: web::Data<AppState>,
    body: web::Json<serde_json::Value>,
) -> HttpResponse {
    create_document(&state, "posts", body.into_inner()).await
}

async fn update_post(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
) -> HttpResponse {
    update_document(&state, "posts", &path, body.into_inner()).await
}

async fn delete_post(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    delete_document(&state, "posts", &path).await
}

// ── Comments ────────────────────────────────────────────────────────

async fn list_comments(state: web::Data<AppState>) -> HttpResponse {
    list_collection(&state, "comments").await
}

async fn get_comment(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    get_document(&state, "comments", &path).await
}

async fn create_comment(
    state: web::Data<AppState>,
    body: web::Json<serde_json::Value>,
) -> HttpResponse {
    create_document(&state, "comments", body.into_inner()).await
}

async fn update_comment(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
) -> HttpResponse {
    update_document(&state, "comments", &path, body.into_inner()).await
}

async fn delete_comment(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    delete_document(&state, "comments", &path).await
}

// ── Events ──────────────────────────────────────────────────────────

async fn list_events(state: web::Data<AppState>) -> HttpResponse {
    list_collection(&state, "events").await
}

async fn get_event(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    get_document(&state, "events", &path).await
}

async fn create_event(
    state: web::Data<AppState>,
    body: web::Json<serde_json::Value>,
) -> HttpResponse {
    create_document(&state, "events", body.into_inner()).await
}

async fn update_event(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
) -> HttpResponse {
    update_document(&state, "events", &path, body.into_inner()).await
}

async fn delete_event(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    delete_document(&state, "events", &path).await
}

// ── Views ───────────────────────────────────────────────────────────

async fn view_post_feed(state: web::Data<AppState>) -> HttpResponse {
    match state.store.view_dynamic("post_feed") {
        Ok(v) => ok_json(v),
        Err(e) => err_response(e),
    }
}

async fn view_user_lookup(state: web::Data<AppState>) -> HttpResponse {
    match state.store.view_dynamic("user_lookup") {
        Ok(v) => ok_json(v),
        Err(e) => err_response(e),
    }
}

async fn view_recent_activity(state: web::Data<AppState>) -> HttpResponse {
    match state.store.view_dynamic("recent_activity") {
        Ok(v) => ok_json(v),
        Err(e) => err_response(e),
    }
}

#[derive(Deserialize)]
struct PostCommentsQuery {
    post_id: String,
}

async fn view_post_comments(
    state: web::Data<AppState>,
    query: web::Query<PostCommentsQuery>,
) -> HttpResponse {
    let mut params = HashMap::new();
    params.insert("post_id".to_string(), query.post_id.clone());
    match state.store.query_dynamic("post_comments", &params) {
        Ok(v) => ok_json(v),
        Err(e) => err_response(e),
    }
}
