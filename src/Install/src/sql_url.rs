use core::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

use deadpool_redis::{Config as Conf, Pool, Runtime};
use rbatis::RBatis;
use rbdc::db::{Connection as Con};
use rbdc_pg::connection::PgConnection;
use rbdc_pg::options::PgConnectOptions;
use rbs::Value;
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, ExecResult, QueryResult, Statement};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, Config, Connection, NoTls, Socket};
use tokio_postgres::tls::NoTlsStream;

use Static::Events;

use crate::setting::local_config::SUPER_URL;

//use crate::entities::prelude::*;
/////# 数据库
//pub enum Table {
//	Server(Service),
//	Settings(Database)
//}

pub trait Url {
    fn build_url(&self) -> String;
    ///# 数据库切换
    fn build_url_database(&self, e: &str) -> String {
        let et = self.build_url();
        let et = et.rsplitn(2, "/").collect::<Vec<_>>();
        let mut xx = String::from(et[1]);
        xx.push_str("/");
        xx.push_str(e);
        xx
    }
}

pub trait OrmEX {
    fn url(&self) -> String;
    //链接
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    async fn default_connect(e: String) -> Events<DatabaseConnection> {
        Ok(Database::connect(e).await?)
    }
    async fn connect(&self) -> Events<DatabaseConnection> {
        Ok(Database::connect(self.url()).await?)
    }
    async fn connect_bdc(&self) -> Events<RBatis> {
        let die = RBatis::new();
        match self.connect().await?.get_database_backend() {
            DbBackend::MySql => { die.link(rbdc_mysql::driver::MysqlDriver {}, self.url().as_str()).await?; }
            DbBackend::Postgres => { die.link(rbdc_pg::driver::PgDriver {}, self.url().as_str()).await?; }
            DbBackend::Sqlite => {}
        }
        Ok(die)
    }
    ///++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    ///# 运行语句
    async fn run_all<const LK: usize>(&self, event: String) -> Events<Vec<QueryResult>> {
        let db = self.connect().await?;
        let xv = db.query_all(Statement::from_string(db.get_database_backend(), event)).await?;
        db.close().await?;
        Ok(xv)
    }
    async fn run_sql(&self, sql: String) -> Events<ExecResult> {
        let ex = self.connect().await?;
        let ev = ex.execute(Statement::from_string(ex.get_database_backend(), sql)).await?;
        ex.close().await?;
        Ok(ev)
    }
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    async fn redis_connection(&self) -> Conf {
        Conf::from_url(self.url())
    }
    async fn redis_connection_async(&self) -> Events<redis::Client> {
        Ok(redis::Client::open(self.url())?)
    }
    async fn redis_pool(&self) -> Events<Pool> {
        Ok(self.redis_connection().await.create_pool(Some(Runtime::Tokio1))?)
    }
}


///# Mysql_Ulr
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MysqlUlr {
    pub name: String,
    pub password: String,
    pub host: String,
    pub port: Option<String>,
    pub database: String,
}

impl Default for MysqlUlr {
    fn default() -> Self {
        MysqlUlr {
            database: DEFAULT_BUILD_DIR_MYSQL.to_string(),
            ..SUPER_URL.deref().load().mysql.clone()
        }
    }
}

impl Display for MysqlUlr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl Url for MysqlUlr {
    fn build_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.name.as_str(),
            self.password.as_str(),
            self.host.as_str(),
            {
                if let Some(ref port) = self.port {
                    port.as_ref()
                } else {
                    "3306"
                }
            },
            self.database
        )
    }
}


///# Redis_Ulr
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct RedisUlr {
    pub name: Option<String>,
    pub password: Option<String>,
    pub host: String,
    pub port: Option<String>,
    pub database: String,
}

impl Url for RedisUlr {
    ///#产生
    ///#redis://[<username>][:<password>@][<hostname>][:port][/<db>]
    fn build_url(&self) -> String {
        if self.name.is_some() || self.password.is_some() {
            format!(
                "redis://{}:{}@{}:{}/{}",
                self.name.as_ref().unwrap().as_str(),
                self.password.as_ref().unwrap().as_str(),
                self.host.as_str(),
                {
                    if let Some(ref port) = self.port {
                        port.as_ref()
                    } else {
                        "6379"
                    }
                },
                self.database.as_str()
            )
        } else {
            format!("redis://{}:{}", self.host.as_str(), {
                if let Some(ref port) = self.port {
                    port.as_ref()
                } else {
                    "6379"
                }
            })
        }
    }
}

impl OrmEX for RedisUlr {
    fn url(&self) -> String {
        self.build_url()
    }
}


///# Postgres_Ulr
///# jdbc:postgresql://root:root@localhost:5432/postgres
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostgresUlr {
    pub name: String,
    pub password: String,
    pub port: Option<String>,
    pub host: String,
    pub database: String,
}

impl PostgresUlr {
    ///# 执行
    pub async fn connect_rab_execute_some(&self, sql: &str, e: Option<Vec<Value>>) -> Events<rbdc::db::ExecResult> {
        let mut x = self.connect_rab().await?;
        let xa = match e {
            None => { x.exec(sql, vec![]).await? }
            Some(e) => { x.exec(sql, e).await? }
        };
        x.close().await?;
        Ok(xa)
    }
}

impl PostgresUlr {
    //# 链接
    async fn connect_rab(&self) -> Events<PgConnection> {
        let x = PgConnectOptions::new().username(self.name.as_str()).password(self.password.as_str()).port(if let Some(ref port) = self.port {
            port.parse().unwrap()
        } else {
            "5432".parse().unwrap()
        }).host(self.host.as_str()).database(self.database.as_str());
        Ok(PgConnection::establish(&x).await?)
    }
    ///# 执行
    pub async fn connect_rab_execute(&self, sql: &str) -> Events<rbdc::db::ExecResult> {
        let mut x = self.connect_rab().await?;
        let xa = x.exec(sql, vec![]).await?;
        x.close().await?;
        Ok(xa)
    }
    ///+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    ///# 链接 tio
    pub async fn config_tok(&self) -> (Client, Connection<Socket, NoTlsStream>) {
        let mut config = Config::new();
        config.host(self.host.as_str());
        config.user(self.name.as_str());
        config.password(self.password.as_str());
        config.dbname(self.database.as_str());
        config.port({
            if let Some(ref port) = self.port {
                port.parse().unwrap()
            } else {
                "5432".parse().unwrap()
            }
        });
        config.connect(NoTls).await.unwrap()
    }
}

impl Default for PostgresUlr {
    fn default() -> Self {
        PostgresUlr {
            database: DEFAULT_BUILD_DIR_POSTGRES.to_string(),
            ..SUPER_URL.deref().load().postgres.clone()
        }
    }
}

impl Display for PostgresUlr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl OrmEX for PostgresUlr {
    fn url(&self) -> String {
        self.build_url()
    }
}

///# 默认数据库
const DEFAULT_BUILD_DIR_POSTGRES: &str = "postgres";
const DEFAULT_BUILD_DIR_MYSQL: &str = "mysql";

impl Url for PostgresUlr {
    fn build_url(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.name.as_str(),
            self.password.as_str(),
            self.host.as_str(),
            {
                if let Some(ref port) = self.port {
                    port.as_ref()
                } else {
                    "5432"
                }
            },
            self.database,
        )
    }
}