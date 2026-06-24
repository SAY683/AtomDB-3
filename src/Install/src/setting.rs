use crate::setting::database_config::{DATABASE_BUILD_DIR, SERVICE_BUILD_DIR, FILE_NODE_BUILD_DIR, FILE_SYMLINK_BUILD_DIR, API_KEYS_BUILD_DIR};

///# 查询数据库
const INQUIRE_BUILD_DIR_SERVER: &str = r#"
select tablename from pg_tables where tablename ='service'"#;
const INQUIRE_BUILD_DIR_DATABASE: &str = r#"
select tablename from pg_tables where tablename='database'"#;
// const INQUIRE_BUILD_DIR_TYPER: &str = r#"
// select typname
// from pg_type
// where typname = 'modes'"#;

const INQUIRE_BUILD_DIR_FILE_NODE: &str = r#"
select tablename from pg_tables where tablename='file_node'"#;
const INQUIRE_BUILD_DIR_SYMLINK: &str = r#"
select tablename from pg_tables where tablename='file_symlink'"#;
const INQUIRE_BUILD_DIR_API_KEYS: &str = r#"
select tablename from pg_tables where tablename='api_keys'"#;

///# 初始数据库必修
pub const JUDGEMENT: [(&str, &str); 5] = [
    (INQUIRE_BUILD_DIR_DATABASE, DATABASE_BUILD_DIR),
    (INQUIRE_BUILD_DIR_SERVER, SERVICE_BUILD_DIR),
    (INQUIRE_BUILD_DIR_FILE_NODE, FILE_NODE_BUILD_DIR),
    (INQUIRE_BUILD_DIR_SYMLINK, FILE_SYMLINK_BUILD_DIR),
    (INQUIRE_BUILD_DIR_API_KEYS, API_KEYS_BUILD_DIR),
];

pub mod database_config {
    use rbatis::crud;
    use serde::{Deserialize, Serialize};

    pub const TYPE_EME: &str = r#"
    create type modes as enum ('Hash', 'Map', 'Cache');"#;

    pub const DATABASE_BUILD_DIR: &str = r#"
    create table database
(
    uuid text not null
        constraint database_pk
            primary key,
    name text not null,
    hash text not null,
    port text not null,
    constraint database_pk2
        unique (name, hash, port)
);"#;
    ///# 创建结构
    pub const SERVICE_BUILD_DIR: &str = r#"
    create table service
(
    uuid text not null
        constraint service_pk
            primary key
        constraint service_database_uuid_fk
            references database,
    name text not null,
    logs text,
    mode text not null
);"#;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
    pub struct Database {
        pub uuid: String,
        pub name: String,
        pub hash: String,
        pub port: String,
    }
    crud!(Database{});

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Service {
        pub uuid: String,
        pub name: String,
        pub logs: Option<String>,
        pub mode: String,
    }

    impl Default for Service {
        fn default() -> Self {
            Service {
                uuid: "Null".to_string(),
                name: "Null".to_string(),
                logs: None,
                mode: "Null".to_string(),
            }
        }
    }

    crud!(Service{});

    ///# 虚拟文件系统: 文件节点表
    pub const FILE_NODE_BUILD_DIR: &str = r#"
    create table file_node (
        uuid text not null constraint file_node_pk primary key,
        parent_uuid text references file_node(uuid) on delete cascade,
        name text not null,
        is_dir boolean not null default false,
        content_hash text,
        file_size bigint default 0,
        created_at timestamp default now(),
        updated_at timestamp default now(),
        constraint file_node_unique_name unique (parent_uuid, name)
    );"#;

    ///# 虚拟文件系统: 拟态链接表
    pub const FILE_SYMLINK_BUILD_DIR: &str = r#"
    create table file_symlink (
        uuid text not null constraint file_symlink_pk primary key
            references file_node(uuid) on delete cascade,
        target_uuid text not null
            references file_node(uuid) on delete cascade
    );"#;

    ///# API 密钥表
    pub const API_KEYS_BUILD_DIR: &str = r#"
    create table api_keys (
        key text not null constraint api_keys_pk primary key,
        permission text not null default 'read',
        label text,
        created_at timestamp default now()
    );"#;

    // ============ VFS 模型 ============

    #[derive(Clone, Debug, Serialize, Deserialize, Default)]
    pub struct FileNode {
        pub uuid: String,
        pub parent_uuid: Option<String>,
        pub name: String,
        pub is_dir: bool,
        pub content_hash: Option<String>,
        pub file_size: i64,
        pub created_at: String,
    }
    crud!(FileNode{});

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct FileSymlink {
        pub uuid: String,
        pub target_uuid: String,
    }
    crud!(FileSymlink{});

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ApiKey {
        pub key: String,
        pub permission: String,
        pub label: Option<String>,
    }
    crud!(ApiKey{});
}

pub mod local_config {
    use std::net::{SocketAddr};
    use std::path::PathBuf;
    use arc_swap::ArcSwap;
    use once_cell::sync::Lazy;
    use serde::{Deserialize, Serialize};

    use crate::{LOCAL_BIN_DIR_FILR, LOCAL_BIN_FILR};
    use crate::sql_url::{MysqlUlr, PostgresUlr, RedisUlr};
    use crate::system::{InstallUtils, Toml};

    pub static SUPER_URL: Lazy<ArcSwap<LocalConfig>> = Lazy::new(|| { ArcSwap::from_pointee(LocalConfig::toml_build(LOCAL_BIN_FILR.as_path()).unwrap()) });

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct LocalConfig {
        pub postgres: PostgresUlr,
        pub mysql: MysqlUlr,
        pub redis: RedisUlr,
        pub mariadb: MysqlUlr,
    }

    impl InstallUtils for LocalConfig {}

    impl Toml for LocalConfig {}

    ///+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    pub static SUPER_DLR_URL: Lazy<ArcSwap<LocalConfigToml>> = Lazy::new(|| { ArcSwap::from_pointee(LocalConfigToml::toml_build(LOCAL_BIN_DIR_FILR.as_path()).unwrap()) });

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct LocalConfigToml {
        pub port: SocketAddr,
        pub view: bool,
        pub apl: PathBuf,
        pub path: String,
        pub auto: bool,
        pub time: usize,
    }

    impl InstallUtils for LocalConfigToml {}

    impl Toml for LocalConfigToml {}
}