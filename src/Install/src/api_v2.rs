//! # AtomDB V2 API — 虚拟文件系统 + 认证权限
//!
//! 认证方式: `X-API-Key: <管理员密码>:<read|write>`
//! - read:  仅读权限（列表、下载、查看）
//! - write: 读写权限（上传、修改、删除）
//!
//! 管理员密码通过环境变量 `ATOMDB_ADMIN_PASSWORD` 或 `Bin/atomic.toml` 的 `admin_password` 字段设置。

use actix_web::{HttpRequest, HttpResponse, Responder, web};
use actix_web::http::header;
use once_cell::sync::OnceCell;
use rbatis::RBatis;
use serde::Serialize;
use uuid::Uuid;
use Static::{Events, LOCAL_DB};
use Error::ThreadEvents;
use crate::sql_url::OrmEX;
use crate::setting::database_config::{FileNode, FileSymlink, ApiKey};
use crate::setting::local_config::SUPER_DLR_URL;

// ======================== 全局状态 ========================

static VFS_DB: OnceCell<RBatis> = OnceCell::new();

async fn get_vfs_db() -> Events<&'static RBatis> {
    match VFS_DB.get() {
        Some(rb) => Ok(rb),
        None => {
            let rb = crate::setting::local_config::SUPER_URL.load().postgres.connect_bdc().await?;
            VFS_DB.set(rb)
                .map_err(|_| ThreadEvents::UnknownError(anyhow::anyhow!("VFS DB 已初始化")))?;
            Ok(VFS_DB.get().unwrap())
        }
    }
}

// ======================== 认证系统 ========================

/// 从环境变量读取管理员密码
fn admin_password() -> String {
    let pw = std::env::var("ATOMDB_ADMIN_PASSWORD").unwrap_or_else(|_| String::new());
    if pw.is_empty() {
        eprintln!("[AtomDB] ⚠️  警告: 未设置 ATOMDB_ADMIN_PASSWORD 环境变量，使用默认密码 'admin'");
        eprintln!("[AtomDB] ⚠️  请设置环境变量以增强安全: export ATOMDB_ADMIN_PASSWORD=your_password");
        "admin".to_string()
    } else {
        pw
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Permission { Read, Write }

/// 校验请求认证，返回权限等级（同步版本，用于 actix-web handler）
/// 使用 rfind(':') 分割，支持密码中包含冒号
fn check_auth(req: &HttpRequest) -> Result<Permission, HttpResponse> {
    let key = req
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // 使用最后一个冒号分割，这样密码本身可以包含冒号
    let colon_pos = match key.rfind(':') {
        Some(p) => p,
        None => return Err(HttpResponse::Unauthorized().json(serde_json::json!({
            "success": false, "error": "需要 X-API-Key 头，格式: <password>:<read|write>"
        }))),
    };
    let password = &key[..colon_pos];
    let perm_str = &key[colon_pos + 1..];

    // 注: api_keys 表已创建但认证集成需要异步重构，当前仅支持管理员密码模式
    if password != admin_password() {
        return Err(HttpResponse::Unauthorized().json(serde_json::json!({
            "success": false, "error": "密码错误"
        })));
    }

    match perm_str {
        "read" => Ok(Permission::Read),
        "write" => Ok(Permission::Write),
        _ => Err(HttpResponse::Unauthorized().json(serde_json::json!({
            "success": false, "error": "权限必须是 read 或 write"
        }))),
    }
}

fn require_write(perm: &Permission) -> Result<(), HttpResponse> {
    match perm {
        Permission::Write => Ok(()),
        Permission::Read => Err(HttpResponse::Forbidden().json(serde_json::json!({
            "success": false, "error": "需要 write 权限"
        }))),
    }
}

fn json_ok<T: Serialize>(data: T) -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({"success": true, "data": data}))
}

fn json_err(status: actix_web::http::StatusCode, msg: &str) -> HttpResponse {
    HttpResponse::build(status).json(serde_json::json!({"success": false, "error": msg}))
}

// ======================== 路由注册 ========================

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v2")
            // 认证
            .route("/auth/login", web::post().to(api_login))
            .route("/auth/token", web::get().to(api_get_token))
            // 文件操作（注意: 批量路由必须排在 {uuid} 参数路由之前，避免冲突）
            .route("/files", web::get().to(api_list_root))
            .route("/files/upload", web::post().to(api_upload_file))
            .route("/files/symlink", web::post().to(api_create_symlink))
            .route("/files/batch/move", web::post().to(api_batch_move))
            .route("/files/batch/copy", web::post().to(api_batch_copy))
            .route("/files/batch/delete", web::post().to(api_batch_delete))
            .route("/files/{uuid}", web::get().to(api_get_node))
            .route("/files/{uuid}/download", web::get().to(api_download_file))
            .route("/files/{uuid}", web::put().to(api_update_node))
            .route("/files/{uuid}/content", web::put().to(api_overwrite_file))
            .route("/files/{uuid}/move", web::post().to(api_move_node))
            .route("/files/{uuid}/copy", web::post().to(api_copy_node))
            .route("/files/{uuid}", web::delete().to(api_delete_node))
            // 管理
            .route("/admin/keys", web::post().to(api_create_key))
            .route("/admin/keys/{key}", web::delete().to(api_delete_key))
    );
}

// ======================== 登录 / Token ========================

#[derive(serde::Deserialize)]
struct LoginReq { password: String }

async fn api_login(body: web::Json<LoginReq>) -> impl Responder {
    if body.password == admin_password() {
        json_ok(serde_json::json!({
            "token_read":  format!("{}:read", body.password),
            "token_write": format!("{}:write", body.password),
            "permissions": ["read", "write"]
        }))
    } else {
        json_err(actix_web::http::StatusCode::UNAUTHORIZED, "密码错误")
    }
}

/// 本地管理面板获取 Token（无需认证，仅供同源面板使用）
async fn api_get_token() -> impl Responder {
    let pw = admin_password();
    json_ok(serde_json::json!({
        "token_read":  format!("{}:read", pw),
        "token_write": format!("{}:write", pw),
        "permissions": ["read", "write"],
        "current_permission": env_permission()
    }))
}

/// 判断当前环境权限（未设密码时默认只读）
fn env_permission() -> &'static str {
    if std::env::var("ATOMDB_ADMIN_PASSWORD").is_ok() { "write" } else { "read" }
}

// ======================== 文件列表（根目录） ========================

async fn api_list_root(req: HttpRequest) -> impl Responder {
    let _perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    list_children(None).await
}

// ======================== 获取节点/目录列表 ========================

async fn api_get_node(req: HttpRequest, path: web::Path<String>) -> impl Responder {
    let _perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    let uuid = path.into_inner();
    get_node_detail(&uuid).await
}

async fn get_node_detail(uuid: &str) -> HttpResponse {
    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };

    let nodes = match FileNode::select_by_map(&conn, rbs::value!{"uuid": uuid}).await {
        Ok(n) => n,
        Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("查询失败: {}", e)),
    };
    let node = match nodes.into_iter().next() {
        Some(n) => n,
        None => return json_err(actix_web::http::StatusCode::NOT_FOUND, "节点未找到"),
    };

    if node.is_dir {
        // 是目录: 返回子节点列表
        let children = match FileNode::select_by_map(&conn, rbs::value!{"parent_uuid": uuid}).await {
            Ok(c) => c,
            Err(_) => vec![],
        };
        let items: Vec<serde_json::Value> = children.into_iter().map(|c| node_to_json(&c)).collect();
        json_ok(serde_json::json!({
            "type": "directory",
            "uuid": node.uuid,
            "name": node.name,
            "children": items,
        }))
    } else {
        // 是文件
        let mut info = node_to_json(&node);

        // 检查是否为拟态链接
        let symlinks = FileSymlink::select_by_map(&conn, rbs::value!{"uuid": uuid}).await.unwrap_or_default();
        if let Some(sl) = symlinks.into_iter().next() {
            info.as_object_mut().map(|m| {
                m.insert("symlink".to_string(), serde_json::json!(true));
                m.insert("target_uuid".to_string(), serde_json::json!(sl.target_uuid));
            });
        }

        json_ok(info)
    }
}

fn node_to_json(n: &FileNode) -> serde_json::Value {
    serde_json::json!({
        "uuid": n.uuid,
        "name": n.name,
        "is_dir": n.is_dir,
        "file_size": n.file_size,
        "content_hash": n.content_hash,
        "parent_uuid": n.parent_uuid,
    })
}

// ======================== 列表子目录 ========================

async fn list_children(parent_uuid: Option<&str>) -> HttpResponse {
    // 使用 SeaORM 执行带 NULL 安全的条件查询（绕过 rbatis 的 NULL 条件兼容问题）
    use sea_orm::ConnectionTrait;
    let pool = match crate::web::get_sql_pool().await {
        Ok(p) => p,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let sql = if parent_uuid.is_some() {
        "SELECT uuid, parent_uuid, name, is_dir, content_hash, file_size FROM file_node WHERE parent_uuid = $1 ORDER BY is_dir DESC, name ASC"
    } else {
        "SELECT uuid, parent_uuid, name, is_dir, content_hash, file_size FROM file_node WHERE parent_uuid IS NULL ORDER BY is_dir DESC, name ASC"
    };
    let stmt = match parent_uuid {
        Some(pid) => sea_orm::Statement::from_sql_and_values(
            sea_orm::DbBackend::Postgres, sql, vec![sea_orm::Value::String(Some(Box::new(pid.to_owned())))]
        ),
        None => sea_orm::Statement::from_string(sea_orm::DbBackend::Postgres, sql.to_string()),
    };
    let results = match pool.query_all(stmt).await {
        Ok(r) => r,
        Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("查询失败: {}", e)),
    };
    let items: Vec<serde_json::Value> = results.iter().filter_map(|r| {
        Some(serde_json::json!({
            "uuid": r.try_get::<String>("", "uuid").ok()?,
            "name": r.try_get::<String>("", "name").ok()?,
            "is_dir": r.try_get::<bool>("", "is_dir").ok()?,
            "parent_uuid": r.try_get::<Option<String>>("", "parent_uuid").ok()?,
            "file_size": r.try_get::<i64>("", "file_size").ok()?,
            "content_hash": r.try_get::<Option<String>>("", "content_hash").ok()?,
        }))
    }).collect();
    json_ok(items)
}

// ======================== 上传文件 ========================

async fn api_upload_file(req: HttpRequest, body: web::Bytes) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }

    // 解析查询参数: name, parent(可选)
    let query = web::Query::<std::collections::HashMap<String, String>>::from_query(req.query_string())
        .unwrap_or_else(|_| web::Query(std::collections::HashMap::new()));
    let name = match query.get("name").filter(|n| !n.is_empty()) {
        Some(n) => n.clone(),
        None => return json_err(actix_web::http::StatusCode::BAD_REQUEST, "缺少 ?name= 参数"),
    };
    let parent_uuid = query.get("parent").filter(|p| !p.is_empty());

    if body.is_empty() && !name.ends_with('/') {
        return json_err(actix_web::http::StatusCode::BAD_REQUEST, "文件内容为空");
    }

    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };

    let node_uuid = Uuid::new_v4().to_string();

    // 如果是目录（名称以 / 结尾），创建目录节点
    if name.ends_with('/') {
        let dir_name = name.trim_end_matches('/');
        let node = FileNode {
            uuid: node_uuid.clone(),
            parent_uuid: parent_uuid.map(|s| s.to_string()),
            name: dir_name.to_string(),
            is_dir: true,
            content_hash: None,
            file_size: 0,
        };
        match FileNode::insert(&conn, &node).await {
            Ok(_) => json_ok(serde_json::json!({"uuid": node_uuid, "name": dir_name, "is_dir": true})),
            Err(e) => json_err(actix_web::http::StatusCode::CONFLICT, &format!("创建目录失败: {}", e)),
        }
    } else {
        // 文件: 写入 cacache（直接使用 cacache API 避免 KVStore 的 .expect() panic）
        let cache_dir = Static::LOCAL_DB.to_str().unwrap_or("Data");
        let integrity = match cacache::write(cache_dir, &node_uuid, body.as_ref()).await {
            Ok(i) => i,
            Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("存储失败: {}", e)),
        };
        let size = body.len() as i64;

        let node = FileNode {
            uuid: node_uuid.clone(),
            parent_uuid: parent_uuid.map(|s| s.to_string()),
            name: name.clone(),
            is_dir: false,
            content_hash: Some(integrity.to_string()),
            file_size: size,
        };
        match FileNode::insert(&conn, &node).await {
            Ok(_) => json_ok(serde_json::json!({
                "uuid": node_uuid, "name": name, "size": size, "hash": integrity.to_string()
            })),
            Err(e) => json_err(actix_web::http::StatusCode::CONFLICT, &format!("上传失败: {}", e)),
        }
    }
}

// ======================== 下载文件 ========================

async fn api_download_file(req: HttpRequest, path: web::Path<String>) -> impl Responder {
    let _perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    let uuid = path.into_inner();

    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };

    let nodes = match FileNode::select_by_map(&conn, rbs::value!{"uuid": &uuid}).await {
        Ok(n) => n,
        Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("查询失败: {}", e)),
    };
    let node = match nodes.into_iter().next() {
        Some(n) => n,
        None => return json_err(actix_web::http::StatusCode::NOT_FOUND, "文件未找到"),
    };

    if node.is_dir {
        return json_err(actix_web::http::StatusCode::BAD_REQUEST, "不能下载目录");
    }

    // 解析目标 UUID（支持拟态链接追踪）
    let target_uuid = resolve_symlink_target(&conn, &uuid).await.unwrap_or_else(|| uuid.clone());
    drop(conn);

    let cache_dir = LOCAL_DB.to_str().unwrap_or("Data");
    let data = match cacache::read(cache_dir, &target_uuid).await {
        Ok(d) => Ok(d),
        Err(_) => cacache::read(cache_dir, &uuid).await,
    };
    fn urlenc(s: &str) -> String {
        s.bytes().map(|b| format!("%{:02X}", b)).collect()
    }
    match data {
        Ok(data) => HttpResponse::Ok()
            .insert_header((header::CONTENT_TYPE, "application/octet-stream"))
            .insert_header((header::CONTENT_DISPOSITION, format!("attachment; filename*=UTF-8''{}; filename=\"download\"", urlenc(&node.name))))
            .body(data),
        Err(e) => json_err(actix_web::http::StatusCode::NOT_FOUND, &format!("数据未找到: {}", e)),
    }
}

/// 递归解析拟态链接，返回最终的目标 UUID
async fn resolve_symlink_target(conn: &dyn rbatis::executor::Executor, uuid: &str) -> Option<String> {
    let mut visited = std::collections::HashSet::new();
    let mut current = uuid.to_string();
    visited.insert(current.clone());

    loop {
        let links = FileSymlink::select_by_map(conn, rbs::value!{"uuid": &current}).await.ok()?;
        let link = links.into_iter().next()?;
        if !visited.insert(link.target_uuid.clone()) {
            return None; // 循环链接检测
        }
        current = link.target_uuid;
    }
}

// ======================== 更新节点（重命名/移动） ========================

#[derive(serde::Deserialize)]
struct UpdateNodeReq {
    name: Option<String>,
    parent_uuid: Option<String>,
}

async fn api_update_node(req: HttpRequest, path: web::Path<String>, body: web::Json<UpdateNodeReq>) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }
    let uuid = path.into_inner();

    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };
    // 直接用参数化 SQL 更新
    let sql = "UPDATE file_node SET name = COALESCE($1, name), parent_uuid = COALESCE($2, parent_uuid), updated_at = NOW() WHERE uuid = $3";
    match conn.exec(sql, vec![
        body.name.as_ref().map(|s| rbs::Value::String(s.clone())).unwrap_or(rbs::Value::Null),
        body.parent_uuid.as_ref().map(|s| rbs::Value::String(s.clone())).unwrap_or(rbs::Value::Null),
        rbs::Value::String(uuid.clone()),
    ]).await {
        Ok(r) if r.rows_affected > 0 => json_ok(serde_json::json!({"uuid": uuid, "updated": true})),
        Ok(_) => json_err(actix_web::http::StatusCode::NOT_FOUND, "节点未找到"),
        Err(e) => json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("更新失败: {}", e)),
    }
}

// ======================== 覆盖文件内容 ========================

async fn api_overwrite_file(req: HttpRequest, path: web::Path<String>, body: web::Bytes) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }
    let uuid = path.into_inner();

    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };

    let nodes = match FileNode::select_by_map(&conn, rbs::value!{"uuid": &uuid}).await {
        Ok(n) => n,
        Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("查询失败: {}", e)),
    };
    if nodes.is_empty() {
        return json_err(actix_web::http::StatusCode::NOT_FOUND, "文件未找到");
    }
    drop(conn);

    // 覆盖 cacache 内容（直接使用 cacache API 避免 KVStore 的 .expect() panic）
    let cache_dir = Static::LOCAL_DB.to_str().unwrap_or("Data");
    let integrity = match cacache::write(cache_dir, &uuid, body.as_ref()).await {
        Ok(i) => i,
        Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("存储失败: {}", e)),
    };
    let size = body.len() as i64;

    // 更新数据库记录
    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };
    match conn.exec(
        "UPDATE file_node SET content_hash = $1, file_size = $2, updated_at = NOW() WHERE uuid = $3",
        vec![
            rbs::Value::String(integrity.to_string()),
            rbs::Value::I64(size),
            rbs::Value::String(uuid.clone()),
        ],
    ).await {
        Ok(_) => json_ok(serde_json::json!({"uuid": uuid, "size": size, "hash": integrity.to_string()})),
        Err(e) => json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("更新失败: {}", e)),
    }
}

// ======================== 移动节点 ========================

#[derive(serde::Deserialize)]
struct MoveNodeReq { parent_uuid: String, name: Option<String> }

async fn api_move_node(req: HttpRequest, path: web::Path<String>, body: web::Json<MoveNodeReq>) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }
    let uuid = path.into_inner();
    let rb = match get_vfs_db().await { Ok(rb) => rb, Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)) };
    let conn = match rb.acquire().await { Ok(c) => c, Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)) };
    let new_name = body.name.as_deref().unwrap_or(&uuid);
    let parent_val = if body.parent_uuid.is_empty() { rbs::Value::Null } else { rbs::Value::String(body.parent_uuid.clone()) };
    match conn.exec(
        "UPDATE file_node SET parent_uuid = $1, name = COALESCE($2, name), updated_at = NOW() WHERE uuid = $3",
        vec![parent_val, rbs::Value::String(new_name.to_string()), rbs::Value::String(uuid.clone())],
    ).await {
        Ok(r) if r.rows_affected > 0 => json_ok(serde_json::json!({"moved": uuid, "to": body.parent_uuid})),
        Ok(_) => json_err(actix_web::http::StatusCode::NOT_FOUND, "节点未找到"),
        Err(e) => json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("移动失败: {}", e)),
    }
}

// ======================== 复制节点 ========================

#[derive(serde::Deserialize)]
struct CopyNodeReq { parent_uuid: String, name: Option<String> }

async fn api_copy_node(req: HttpRequest, path: web::Path<String>, body: web::Json<CopyNodeReq>) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }
    let uuid = path.into_inner();
    let rb = match get_vfs_db().await { Ok(rb) => rb, Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)) };
    let conn = match rb.acquire().await { Ok(c) => c, Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)) };
    // 查询源节点
    let src = match FileNode::select_by_map(&conn, rbs::value!{"uuid": &uuid}).await {
        Ok(n) => n.into_iter().next(),
        Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("查询失败: {}", e)),
    };
    let src = match src { Some(s) => s, None => return json_err(actix_web::http::StatusCode::NOT_FOUND, "源节点未找到") };
    let new_uuid = Uuid::new_v4().to_string();
    let new_name = body.name.clone().unwrap_or_else(|| format!("{} (副本)", src.name));
    // 复制文件内容（cacache 内部按内容去重，相同数据不额外占用空间）
    if !src.is_dir {
        let cache_dir = Static::LOCAL_DB.to_str().unwrap_or("Data");
        if let Ok(data) = cacache::read(cache_dir, &uuid).await {
            let _ = cacache::write(cache_dir, &new_uuid, &data).await;
        }
    }
    // 创建新节点
    let parent_uuid = if body.parent_uuid.is_empty() { None } else { Some(body.parent_uuid.clone()) };
    let new_node = FileNode {
        uuid: new_uuid.clone(),
        parent_uuid,
        name: new_name,
        is_dir: src.is_dir,
        content_hash: src.content_hash,
        file_size: src.file_size,
    };
    match FileNode::insert(&conn, &new_node).await {
        Ok(_) => json_ok(serde_json::json!({"uuid": new_uuid, "copied_from": uuid, "parent": body.parent_uuid})),
        Err(e) => json_err(actix_web::http::StatusCode::CONFLICT, &format!("复制失败: {}", e)),
    }
}

// ======================== 批量移动 ========================

#[derive(serde::Deserialize)]
struct BatchMoveReq { uuids: Vec<String>, target_parent: String }

async fn api_batch_move(req: HttpRequest, body: web::Json<BatchMoveReq>) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }
    let rb = match get_vfs_db().await { Ok(rb) => rb, Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)) };
    let conn = match rb.acquire().await { Ok(c) => c, Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)) };
    let mut moved = Vec::new();
    let parent_val = if body.target_parent.is_empty() { rbs::Value::Null } else { rbs::Value::String(body.target_parent.clone()) };
    for uid in &body.uuids {
        match conn.exec(
            "UPDATE file_node SET parent_uuid = $1, updated_at = NOW() WHERE uuid = $2",
            vec![parent_val.clone(), rbs::Value::String(uid.clone())],
        ).await {
            Ok(r) if r.rows_affected > 0 => moved.push(uid.clone()),
            _ => eprintln!("[AtomDB] 批量移动失败: {}", uid),
        }
    }
    json_ok(serde_json::json!({"moved": moved, "target": body.target_parent}))
}

// ======================== 批量复制 ========================

#[derive(serde::Deserialize)]
struct BatchCopyReq { uuids: Vec<String>, target_parent: String }

async fn api_batch_copy(req: HttpRequest, body: web::Json<BatchCopyReq>) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }
    let rb = match get_vfs_db().await { Ok(rb) => rb, Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)) };
    let conn = match rb.acquire().await { Ok(c) => c, Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)) };
    let cache_dir = Static::LOCAL_DB.to_str().unwrap_or("Data");
    let parent_val: Option<String> = if body.target_parent.is_empty() { None } else { Some(body.target_parent.clone()) };
    let mut copied = Vec::new();
    for uid in &body.uuids {
        let src = match FileNode::select_by_map(&conn, rbs::value!{"uuid": uid}).await {
            Ok(n) => n.into_iter().next(),
            Err(_) => continue,
        };
        if let Some(src) = src {
            let new_uuid = Uuid::new_v4().to_string();
            if !src.is_dir {
                if let Ok(data) = cacache::read(cache_dir, uid).await {
                    let _ = cacache::write(cache_dir, &new_uuid, &data).await;
                }
            }
            let node = FileNode {
                uuid: new_uuid.clone(),
                parent_uuid: parent_val.clone(),
                name: format!("{} (副本)", src.name),
                is_dir: src.is_dir,
                content_hash: src.content_hash,
                file_size: src.file_size,
            };
            if FileNode::insert(&conn, &node).await.is_ok() {
                copied.push(new_uuid);
            }
        }
    }
    json_ok(serde_json::json!({"copied": copied, "target": body.target_parent}))
}

// ======================== 批量删除 ========================

#[derive(serde::Deserialize)]
struct BatchDeleteReq { uuids: Vec<String> }

async fn api_batch_delete(req: HttpRequest, body: web::Json<BatchDeleteReq>) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }
    let rb = match get_vfs_db().await { Ok(rb) => rb, Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)) };
    let conn = match rb.acquire().await { Ok(c) => c, Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)) };
    let cache_dir = Static::LOCAL_DB.to_str().unwrap_or("Data");
    let mut deleted = Vec::new();
    for uid in &body.uuids {
        // 收集子节点 UUID
        let children = FileNode::select_by_map(&conn, rbs::value!{"parent_uuid": uid}).await.unwrap_or_default();
        let child_ids: Vec<String> = children.into_iter().map(|c| c.uuid).collect();
        // 删除 DB 记录（CASCADE 自动处理子节点）
        let _ = FileSymlink::delete_by_map(&conn, rbs::value!{"uuid": uid}).await;
        let _ = FileNode::delete_by_map(&conn, rbs::value!{"uuid": uid}).await;
        // 清理 cacache
        for id in std::iter::once(uid.clone()).chain(child_ids) {
            let _ = cacache::remove(cache_dir, &id).await;
        }
        deleted.push(uid.clone());
    }
    json_ok(serde_json::json!({"deleted": deleted}))
}

// ======================== 删除节点 ========================

async fn api_delete_node(req: HttpRequest, path: web::Path<String>) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }
    let uuid = path.into_inner();

    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };

    // 级联删除前先收集所有子节点 UUID（用于清理 cacache）
    let children = FileNode::select_by_map(&conn, rbs::value!{"parent_uuid": &uuid}).await.unwrap_or_default();
    let child_uuids: Vec<String> = children.into_iter().map(|c| c.uuid).collect();

    // 级联删除: symlink → file_node (CASCADE 自动处理)
    let _ = FileSymlink::delete_by_map(&conn, rbs::value!{"uuid": &uuid}).await;
    let _ = FileNode::delete_by_map(&conn, rbs::value!{"uuid": &uuid}).await;
    drop(conn);

    // 从 cacache 清理（直接 API 避免 KVStore panic）
    let cache_dir = Static::LOCAL_DB.to_str().unwrap_or("Data");
    for uid in std::iter::once(&uuid).chain(child_uuids.iter()) {
        if let Err(e) = cacache::remove(cache_dir, uid).await {
            eprintln!("[AtomDB] cacache 清理警告 ({}): {}", uid, e);
        }
    }

    json_ok(serde_json::json!({"deleted": uuid}))
}

// ======================== 创建拟态链接 ========================

#[derive(serde::Deserialize)]
struct CreateSymlinkReq {
    name: String,
    target_uuid: String,
    parent_uuid: Option<String>,
}

async fn api_create_symlink(req: HttpRequest, body: web::Json<CreateSymlinkReq>) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }

    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };

    // 验证目标存在
    let targets = match FileNode::select_by_map(&conn, rbs::value!{"uuid": &body.target_uuid}).await {
        Ok(t) => t,
        Err(e) => return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("查询失败: {}", e)),
    };
    if targets.is_empty() {
        return json_err(actix_web::http::StatusCode::NOT_FOUND, "目标节点不存在");
    }

    let sym_uuid = Uuid::new_v4().to_string();
    let node = FileNode {
        uuid: sym_uuid.clone(),
        parent_uuid: body.parent_uuid.clone(),
        name: body.name.clone(),
        is_dir: false,
        content_hash: None,
        file_size: 0,
    };
    let link = FileSymlink {
        uuid: sym_uuid.clone(),
        target_uuid: body.target_uuid.clone(),
    };

    if let Err(e) = FileNode::insert(&conn, &node).await {
        return json_err(actix_web::http::StatusCode::CONFLICT, &format!("创建节点失败: {}", e));
    }
    if let Err(e) = FileSymlink::insert(&conn, &link).await {
        return json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("创建链接失败: {}", e));
    }

    json_ok(serde_json::json!({
        "uuid": sym_uuid, "name": body.name, "symlink": true, "target_uuid": body.target_uuid
    }))
}

// ======================== 管理 API 密钥 ========================

#[derive(serde::Deserialize)]
struct CreateKeyReq {
    permission: String, // "read" | "write"
    label: Option<String>,
}

async fn api_create_key(req: HttpRequest, body: web::Json<CreateKeyReq>) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }

    if body.permission != "read" && body.permission != "write" {
        return json_err(actix_web::http::StatusCode::BAD_REQUEST, "permission 必须是 read 或 write");
    }

    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };

    let api_key = format!("{}:{}", Uuid::new_v4(), body.permission);
    let key = ApiKey {
        key: api_key.clone(),
        permission: body.permission.clone(),
        label: body.label.clone(),
    };
    match ApiKey::insert(&conn, &key).await {
        Ok(_) => json_ok(serde_json::json!({"key": api_key, "permission": body.permission})),
        Err(e) => json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("创建失败: {}", e)),
    }
}

async fn api_delete_key(req: HttpRequest, path: web::Path<String>) -> impl Responder {
    let perm = match check_auth(&req) { Ok(p) => p, Err(e) => return e };
    if let Err(e) = require_write(&perm) { return e; }
    let key = path.into_inner();

    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };
    match ApiKey::delete_by_map(&conn, rbs::value!{"key": &key}).await {
        Ok(_) => json_ok(serde_json::json!({"deleted": key})),
        Err(e) => json_err(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, &format!("删除失败: {}", e)),
    }
}
