use std::ops::Deref;
use actix_web::{App, get, HttpResponse, HttpServer, Responder, web};
use actix_web::dev::Server;
use bevy_reflect::Reflect;
use rayon::prelude::*;
use redis::Commands;
use serde::{Deserialize, Serialize};
use Static::{Alexia, Events};
use Static::alex::Overmaster;
use Static::base::FutureEx;
use Error::ThreadEvents;
use crate::io::{Disk, KVStore};
use crate::rei::Response;
use crate::setting::database_config::{Database, Service};
use crate::setting::local_config::{SUPER_DLR_URL, SUPER_URL};
use crate::sql_url::OrmEX;

#[derive(Copy, Clone, Reflect, Debug)]
pub struct Websocket;

///# 链路 【未启用】
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

pub async fn web() -> Events<Server> {
    let mut x = SUPER_URL.load().postgres.connect_bdc().await?;
    let conn = x.acquire().await?;
    let erx = Database::select_by_map(&conn, rbs::value!{}).await?;
    drop(conn);
    let mut sup = HttpServer::new(|| {
        let mut ddc = App::new();
        ddc = ddc.route(SUPER_DLR_URL.load().path.as_str(), web::get().to(index));
        ddc = ddc.service(download);
        ddc
    });
    for e in erx {
        sup = sup.bind(e.port)?;
    }
    Ok(sup.bind(SUPER_DLR_URL.load().port)?.run())
}

#[get("/{filename}")]
async fn download(filename: String) -> impl Responder {
    let result = download_inner(&filename).await;
    match result {
        Ok(data) => HttpResponse::Ok().content_type("application/octet-stream").body(data),
        Err(e) => {
            eprintln!("下载失败: {:?}", e);
            HttpResponse::NotFound().body(format!("文件未找到: {}", filename))
        }
    }
}

async fn download_inner(filename: &str) -> Events<Vec<u8>> {
    let mut eg = SUPER_URL.load().postgres.connect_bdc().await?;
    let conn = eg.acquire().await?;
    let erx = Database::select_by_map(&conn, rbs::value!{"name": filename}).await?;
    let er = Service::select_by_map(&conn, rbs::value!{"name": filename}).await?;
    let mut bit = vec![];
    for db_rec in erx {
        let xd = er.clone().into_par_iter().find_any(|e| {
            db_rec.uuid == e.uuid
        }).map(|e| {
            KVStore {
                hash: None,
                key: Some(e.uuid),
                value: String::new(),
            }
        });
        if let Some(e) = xd {
            bit.push(e.read().await);
        }
    };
    bit.first().cloned().ok_or_else(|| ThreadEvents::UnknownError(anyhow::anyhow!("文件数据为空")))
}

#[derive(Serialize, Deserialize, Debug)]
struct MysqlESR {
    name: String,
    port: String,
    logs: Option<String>,
}

async fn index() -> impl Responder {
    let result = index_inner().await;
    match result {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(e) => {
            eprintln!("列表查询失败: {:?}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "查询失败",
                "detail": format!("{}", e)
            }))
        }
    }
}

async fn index_inner() -> Events<Vec<serde_json::Value>> {
    let mut eg = SUPER_URL.load().postgres.connect_bdc().await?;
    let conn = eg.acquire().await?;
    let xe = Database::select_by_map(&conn, rbs::value!{}).await?;
    drop(conn);

    // Redis 为可选依赖，不可用时返回空列表
    let nc = match SUPER_URL.deref().load().redis.redis_connection_async().await {
        Ok(client) => Some(client),
        Err(_) => {
            eprintln!("Redis 不可用，返回数据库列表作为降级");
            return Ok(xe.into_iter().map(|e| {
                serde_json::json!({
                    "uuid": e.uuid,
                    "name": e.name,
                    "hash": e.hash,
                    "port": e.port,
                    "cache": "unavailable"
                })
            }).collect());
        }
    };

    let mut nc = nc.unwrap();
    let mut med = vec![];
    for e in xe.into_iter() {
        let uuid = e.uuid.clone();
        match nc.get::<_, String>(&uuid).ok() {
            Some(xx) => {
                match serde_json::from_str::<Response>(&xx) {
                    Ok(r) => med.push(serde_json::to_value(r).unwrap_or(serde_json::json!({"error": "parse failed"}))),
                    Err(_) => med.push(serde_json::json!({"uuid": e.uuid, "name": e.name, "error": "cache parse failed"})),
                }
            }
            None => {
                med.push(serde_json::json!({"uuid": e.uuid, "name": e.name, "hash": e.hash, "port": e.port}));
            }
        }
    }
    Ok(med)
}