use std::ops::Deref;
use actix_cors::Cors;
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use actix_web::dev::Server;
use actix_web::http::header;
use bevy_reflect::Reflect;
use once_cell::sync::OnceCell;
use rayon::prelude::*;
use rbatis::RBatis;
use redis::Commands;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use Static::{Alexia, Events};
use Static::alex::Overmaster;
use Static::base::FutureEx;
use Error::ThreadEvents;
use crate::io::{Disk, KVStore};
use crate::setting::database_config::{Database, Service};
use crate::setting::local_config::{SUPER_DLR_URL, SUPER_URL};
use crate::sql_url::OrmEX;

///# 全局数据库连接池（单例，应用生命周期内复用）
static GLOBAL_DB: OnceCell<RBatis> = OnceCell::new();

async fn get_global_db() -> Events<&'static RBatis> {
    match GLOBAL_DB.get() {
        Some(rb) => Ok(rb),
        None => {
            let rb = SUPER_URL.load().postgres.connect_bdc().await?;
            GLOBAL_DB.set(rb)
                .map_err(|_| ThreadEvents::UnknownError(anyhow::anyhow!("DB 连接池已初始化")))?;
            Ok(GLOBAL_DB.get().unwrap())
        }
    }
}

///# JSON 统一响应封装
fn json_ok<T: Serialize>(data: T) -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({"success": true, "data": data}))
}

fn json_err(status: actix_web::http::StatusCode, msg: &str) -> HttpResponse {
    HttpResponse::build(status).json(serde_json::json!({
        "success": false,
        "error": msg
    }))
}

// ======================== Websocket 占位 ========================

#[derive(Copy, Clone, Reflect, Debug)]
pub struct Websocket;

impl Alexia<Websocket> for Websocket {
    fn event() -> Vec<FutureEx<'static, Overmaster, Events<Websocket>>> {
        vec![FutureEx::AsyncTraitSimple(Box::pin(async {
            web().await?.await?;
            Ok(Websocket)
        })), FutureEx::AsyncTraitSimple(Box::pin(async {
            Ok(Websocket)
        }))]
    }
}

// ======================== 服务启动 ========================

pub async fn web() -> Events<Server> {
    // 初始化全局连接池
    let rb = get_global_db().await?;
    let conn = rb.acquire().await?;
    let databases = Database::select_by_map(&conn, rbs::value!{}).await?;
    drop(conn);

    let max_payload = 100 * 1024 * 1024; // 100MB

    let mut sup = HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .app_data(web::PayloadConfig::new(max_payload))
            // 页面路由
            .route("/", web::get().to(dashboard))
            .route(SUPER_DLR_URL.load().path.as_str(), web::get().to(dashboard))
            // API v1 — 查询
            .route("/api/v1/databases", web::get().to(api_list_databases))
            .route("/api/v1/databases/{uuid}", web::get().to(api_database_detail))
            // API v1 — 增删改
            .route("/api/v1/databases", web::post().to(api_create_database))
            .route("/api/v1/databases/{uuid}", web::put().to(api_update_database))
            .route("/api/v1/databases/{uuid}", web::delete().to(api_delete_database))
            // API v1 — 文件上传/删除
            .route("/api/v1/upload", web::post().to(api_upload_file))
            .route("/api/v1/files/{uuid}", web::delete().to(api_delete_file))
            // 系统
            .route("/health", web::get().to(health))
            // 下载
            .route("/dl/{filename}", web::get().to(download))
    });

    for db in &databases {
        sup = sup.bind(&db.port)?;
    }
    Ok(sup.bind(SUPER_DLR_URL.load().port)?.run())
}

// ======================== 健康检查 ========================

async fn health() -> impl Responder {
    let db_ok = GLOBAL_DB.get().is_some();
    let status = if db_ok { "healthy" } else { "degraded" };
    json_ok(serde_json::json!({
        "status": status,
        "database": db_ok,
        "version": env!("CARGO_PKG_VERSION"),
        "service": "AtomDB"
    }))
}

// ======================== 管理仪表板 ========================

static DASHBOARD_HTML: &str = include_str!("dashboard.html");

async fn dashboard() -> impl Responder {
    HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, "text/html; charset=utf-8"))
        .body(DASHBOARD_HTML)
}

// ======================== API: 数据库列表 ========================

#[derive(Serialize)]
struct DbSummary {
    uuid: String,
    name: String,
    hash: String,
    port: String,
    mode: Option<String>,
    logs: Option<String>,
    cache: Option<String>,
}

async fn api_list_databases() -> impl Responder {
    let rb = match get_global_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 未就绪: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 连接失败: {}", e)),
    };
    let databases = match Database::select_by_map(&conn, rbs::value!{}).await {
        Ok(d) => d,
        Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("查询失败: {}", e)),
    };
    drop(conn);

    // 尝试从 Redis 读取缓存数据（可选，失败则安静跳过）
    let redis_data: std::collections::HashMap<String, serde_json::Value> = {
        let mut map = std::collections::HashMap::new();
        if let Ok(mut client) = SUPER_URL.deref().load().redis.redis_connection_async().await {
            for db in &databases {
                // redis::Client 直接支持 Commands, get() 为同步调用
                if let Ok(val) = client.get::<_, String>(db.uuid.as_str()) {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&val) {
                        map.insert(db.uuid.clone(), json);
                    }
                }
            }
        }
        map
    };

    let list: Vec<DbSummary> = databases.into_iter().map(|d| {
        let cached = redis_data.get(&d.uuid);
        let (mode_str, logs_opt) = match cached {
            Some(v) => (
                v.get("mode").and_then(|m| m.as_str().map(String::from)).unwrap_or_default(),
                v.get("logs").and_then(|l| l.as_str().map(String::from)),
            ),
            None => (String::new(), None),
        };

        DbSummary {
            uuid: d.uuid,
            name: d.name,
            hash: d.hash,
            port: d.port,
            mode: Some(mode_str),
            logs: logs_opt,
            cache: if cached.is_some() { Some("hit".into()) } else { None },
        }
    }).collect();

    json_ok(list)
}

// ======================== API: 数据库详情 ========================

async fn api_database_detail(path: web::Path<String>) -> impl Responder {
    let uuid = path.into_inner();
    let rb = match get_global_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 未就绪: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 连接失败: {}", e)),
    };

    let databases = match Database::select_by_map(&conn, rbs::value!{"uuid": &uuid}).await {
        Ok(d) => d,
        Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("查询失败: {}", e)),
    };

    let db = match databases.into_iter().next() {
        Some(d) => d,
        None => return json_err(actix_web::http::StatusCode::NOT_FOUND, "数据库未找到"),
    };

    let services = Service::select_by_map(&conn, rbs::value!{"uuid": &uuid}).await.unwrap_or_default();
    drop(conn);

    let svc: Vec<serde_json::Value> = services.into_iter().map(|s| {
        serde_json::json!({
            "name": s.name,
            "mode": s.mode,
            "logs": s.logs,
        })
    }).collect();

    json_ok(serde_json::json!({
        "uuid": db.uuid,
        "name": db.name,
        "hash": db.hash,
        "port": db.port,
        "services": svc
    }))
}

// ======================== 文件下载 ========================

async fn download(filename: web::Path<String>) -> impl Responder {
    let filename = filename.into_inner();
    match download_file(&filename).await {
        Ok(data) => HttpResponse::Ok()
            .insert_header((header::CONTENT_TYPE, "application/octet-stream"))
            .insert_header((header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename)))
            .body(data),
        Err(e) => {
            eprintln!("下载失败 [{}]: {:?}", filename, e);
            json_err(actix_web::http::StatusCode::NOT_FOUND, &format!("文件 \"{}\" 未找到", filename))
        }
    }
}

async fn download_file(filename: &str) -> Events<Vec<u8>> {
    let rb = get_global_db().await?;
    let conn = rb.acquire().await?;
    let databases = Database::select_by_map(&conn, rbs::value!{"name": filename}).await?;
    let services = Service::select_by_map(&conn, rbs::value!{"name": filename}).await?;
    drop(conn);

    for db_rec in &databases {
        if let Some(svc) = services.iter().find(|s| s.uuid == db_rec.uuid) {
            let kv = KVStore {
                hash: None,
                key: Some(svc.uuid.clone()),
                value: String::new(),
            };
            let data = kv.read().await;
            return Ok(data);
        }
    }
    Err(ThreadEvents::UnknownError(anyhow::anyhow!("文件 \"{}\" 数据为空", filename)))
}

// ======================== API: 创建数据库 ========================

#[derive(serde::Deserialize)]
struct CreateDbRequest {
    name: String,
    hash: Option<String>,
    port: String,
}

async fn api_create_database(body: web::Json<CreateDbRequest>) -> impl Responder {
    let rb = match get_global_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 未就绪: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 连接失败: {}", e)),
    };
    let uuid = Uuid::new_v4().to_string();
    let db = Database {
        uuid: uuid.clone(),
        name: body.name.clone(),
        hash: body.hash.clone().unwrap_or_default(),
        port: body.port.clone(),
    };
    match Database::insert(&conn, &db).await {
        Ok(_) => json_ok(serde_json::json!({"uuid": uuid, "name": body.name})),
        Err(e) => json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("创建失败: {}", e)),
    }
}

// ======================== API: 更新数据库 ========================

#[derive(serde::Deserialize)]
struct UpdateDbRequest {
    name: Option<String>,
    hash: Option<String>,
    port: Option<String>,
}

async fn api_update_database(path: web::Path<String>, body: web::Json<UpdateDbRequest>) -> impl Responder {
    let uuid = path.into_inner();
    let rb = match get_global_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 未就绪: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 连接失败: {}", e)),
    };
    // 先用 select_by_map 检查记录是否存在
    let existing = match Database::select_by_map(&conn, rbs::value!{"uuid": &uuid}).await {
        Ok(d) => d,
        Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("查询失败: {}", e)),
    };
    if existing.is_empty() {
        return json_err(actix_web::http::StatusCode::NOT_FOUND, "数据库未找到");
    }
    let current = &existing[0];
    let updated = Database {
        uuid: uuid.clone(),
        name: body.name.clone().unwrap_or_else(|| current.name.clone()),
        hash: body.hash.clone().unwrap_or_else(|| current.hash.clone()),
        port: body.port.clone().unwrap_or_else(|| current.port.clone()),
    };
    // rbatis crud! 没有直接的 update_by_id, 用 delete + insert 模拟
    if let Err(e) = Database::delete_by_map(&conn, rbs::value!{"uuid": &uuid}).await {
        return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("删除旧记录失败: {}", e));
    }
    match Database::insert(&conn, &updated).await {
        Ok(_) => json_ok(serde_json::json!({"uuid": uuid, "name": updated.name})),
        Err(e) => json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("更新失败: {}", e)),
    }
}

// ======================== API: 删除数据库 ========================

async fn api_delete_database(path: web::Path<String>) -> impl Responder {
    let uuid = path.into_inner();
    let rb = match get_global_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 未就绪: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 连接失败: {}", e)),
    };

    // 先尝试从 cacache 删除数据（忽略错误）
    let kv = KVStore {
        hash: None,
        key: Some(uuid.clone()),
        value: String::new(),
    };
    kv.remove().await;

    // 删除 service 记录
    let _ = Service::delete_by_map(&conn, rbs::value!{"uuid": &uuid}).await;
    // 删除 database 记录
    match Database::delete_by_map(&conn, rbs::value!{"uuid": &uuid}).await {
        Ok(r) if r.rows_affected > 0 => json_ok(serde_json::json!({"deleted": uuid})),
        Ok(_) => json_err(actix_web::http::StatusCode::NOT_FOUND, "数据库未找到"),
        Err(e) => json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("删除失败: {}", e)),
    }
}

// ======================== API: 上传文件 ========================

async fn api_upload_file(
    query: web::Query<std::collections::HashMap<String, String>>,
    body: web::Bytes,
) -> impl Responder {
    let name = match query.get("name").filter(|n| !n.is_empty()) {
        Some(n) => n.clone(),
        None => return json_err(actix_web::http::StatusCode::BAD_REQUEST, "缺少 ?name= 参数"),
    };

    if body.is_empty() {
        return json_err(actix_web::http::StatusCode::BAD_REQUEST, "文件内容为空");
    }

    let rb = match get_global_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 未就绪: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 连接失败: {}", e)),
    };

    let uuid = Uuid::new_v4();
    let uuid_str = uuid.to_string();
    // 构造临时 KVStore 并写入 cacache（key 用 uuid_str 满足 AsRef<str>）
    let temp_kv = KVStore {
        hash: None,
        key: Some(uuid_str.clone()),
        value: body.to_vec(),
    };
    let integrity = temp_kv.hash_write().await;
    let hash_str = integrity.to_string();
    let def_port = SUPER_DLR_URL.load().port.to_string();

    // 写入 PgSQL
    let db_rec = Database {
        uuid: uuid_str.clone(),
        name: name.clone(),
        hash: hash_str,
        port: def_port,
    };
    let svc_rec = Service {
        uuid: uuid_str.clone(),
        name: name.clone(),
        logs: Some(format!("uploaded at {}", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"))),
        mode: "HASH".to_string(),
    };

    if let Err(e) = Database::insert(&conn, &db_rec).await {
        return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("数据库记录创建失败: {}", e));
    }
    if let Err(e) = Service::insert(&conn, &svc_rec).await {
        return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("服务记录创建失败: {}", e));
    }

    json_ok(serde_json::json!({
        "uuid": uuid_str,
        "name": name,
        "hash": integrity.to_string(),
        "size": body.len(),
    }))
}

// ======================== API: 删除文件 ========================

async fn api_delete_file(path: web::Path<String>) -> impl Responder {
    let uuid = path.into_inner();
    let rb = match get_global_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 未就绪: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 连接失败: {}", e)),
    };

    // 从 PgSQL 删除
    let _ = Service::delete_by_map(&conn, rbs::value!{"uuid": &uuid}).await;
    match Database::delete_by_map(&conn, rbs::value!{"uuid": &uuid}).await {
        Ok(r) if r.rows_affected > 0 => {
            // 从 cacache 删除数据
            let kv = KVStore {
                hash: None,
                key: Some(uuid.clone()),
                value: String::new(),
            };
            kv.remove().await;
            json_ok(serde_json::json!({"deleted": uuid}))
        }
        Ok(_) => json_err(actix_web::http::StatusCode::NOT_FOUND, "文件未找到"),
        Err(e) => json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("删除失败: {}", e)),
    }
}

// ======================== 旧路由兼容 ========================

// 旧的 /{filename} 路由已被 /dl/{filename} 替代