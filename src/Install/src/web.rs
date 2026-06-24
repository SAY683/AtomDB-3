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
use serde::Serialize;
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

    let mut sup = HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            // 页面路由
            .route("/", web::get().to(dashboard))
            .route(SUPER_DLR_URL.load().path.as_str(), web::get().to(dashboard))
            // API v1
            .route("/api/v1/databases", web::get().to(api_list_databases))
            .route("/api/v1/databases/{uuid}", web::get().to(api_database_detail))
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

// ======================== 旧路由兼容 ========================

// 旧的 /{filename} 路由已被 /dl/{filename} 替代