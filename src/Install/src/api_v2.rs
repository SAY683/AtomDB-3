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
use crate::io::{Disk, KVStore};
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
    std::env::var("ATOMDB_ADMIN_PASSWORD").unwrap_or_else(|_| "admin".to_string())
}

#[derive(Debug, Clone, PartialEq)]
enum Permission { Read, Write }

/// 校验请求认证，返回权限等级
fn check_auth(req: &HttpRequest) -> Result<Permission, HttpResponse> {
    let key = req
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let parts: Vec<&str> = key.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err(HttpResponse::Unauthorized().json(serde_json::json!({
            "success": false, "error": "需要 X-API-Key 头，格式: <password>:<read|write>"
        })));
    }

    let (password, perm_str) = (parts[0], parts[1]);
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
            // 文件操作
            .route("/files", web::get().to(api_list_root))
            .route("/files/{uuid}", web::get().to(api_get_node))
            .route("/files/{uuid}/download", web::get().to(api_download_file))
            .route("/files/upload", web::post().to(api_upload_file))
            .route("/files/{uuid}", web::put().to(api_update_node))
            .route("/files/{uuid}/content", web::put().to(api_overwrite_file))
            .route("/files/{uuid}", web::delete().to(api_delete_node))
            .route("/files/symlink", web::post().to(api_create_symlink))
            // 管理
            .route("/admin/keys", web::post().to(api_create_key))
            .route("/admin/keys/{key}", web::delete().to(api_delete_key))
    );
}

// ======================== 登录 ========================

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
            "created_at": node.created_at,
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
    let rb = match get_vfs_db().await {
        Ok(rb) => rb,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("DB 错误: {}", e)),
    };
    let conn = match rb.acquire().await {
        Ok(c) => c,
        Err(e) => return json_err(actix_web::http::StatusCode::SERVICE_UNAVAILABLE, &format!("连接失败: {}", e)),
    };

    // 全量拉取后在 Rust 侧按 parent_uuid 过滤（rbatis 的 Null 条件不可靠）
    let all = FileNode::select_by_map(&conn, rbs::value!{}).await.unwrap_or_default();
    let nodes: Vec<&FileNode> = all.iter().filter(|n| n.parent_uuid.as_deref() == parent_uuid).collect();

    let items: Vec<serde_json::Value> = nodes.into_iter().map(|n| node_to_json(n)).collect();
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
            created_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
        };
        match FileNode::insert(&conn, &node).await {
            Ok(_) => json_ok(serde_json::json!({"uuid": node_uuid, "name": dir_name, "is_dir": true})),
            Err(e) => json_err(actix_web::http::StatusCode::CONFLICT, &format!("创建目录失败: {}", e)),
        }
    } else {
        // 文件: 写入 cacache
        let temp_kv = KVStore {
            hash: None,
            key: Some(node_uuid.clone()),
            value: body.to_vec(),
        };
        let integrity = temp_kv.write().await;
        let size = body.len() as i64;

        let node = FileNode {
            uuid: node_uuid.clone(),
            parent_uuid: parent_uuid.map(|s| s.to_string()),
            name: name.clone(),
            is_dir: false,
            content_hash: Some(integrity.to_string()),
            file_size: size,
            created_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
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
    match data {
        Ok(data) => HttpResponse::Ok()
            .insert_header((header::CONTENT_TYPE, "application/octet-stream"))
            .insert_header((header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", node.name)))
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

    // 覆盖 cacache 内容
    let kv = KVStore {
        hash: None,
        key: Some(uuid.clone()),
        value: body.to_vec(),
    };
    let integrity = kv.write().await;
    let size = body.len() as i64;

    // 更新数据库记录
    let rb = get_vfs_db().await.unwrap();
    let conn = rb.acquire().await.unwrap();
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

    // 级联删除: symlink → file_node (CASCADE 自动处理)
    let _ = FileSymlink::delete_by_map(&conn, rbs::value!{"uuid": &uuid}).await;
    let _ = FileNode::delete_by_map(&conn, rbs::value!{"uuid": &uuid}).await;

    // 尝试从 cacache 清理（忽略错误）
    let kv = KVStore { hash: None, key: Some(uuid.clone()), value: String::new() };
    kv.remove().await;

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
        created_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
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
