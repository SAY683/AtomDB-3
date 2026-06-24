use std::io;
use std::str::Utf8Error;

use data_encoding::DecodeError;
use thiserror::Error;
use tokio::sync::AcquireError;
use tokio::task::JoinError;

/*
错误集合
不允许链接其他库
 */

///# 线程事件
#[derive(Debug, Error)]
pub enum ThreadEvents {
    ///log错误
    #[error("LogsError{0:#?}")]
    LogsError(#[from] fast_log::error::LogError),
    //redis错误
    #[error("RedisCreateError{0:#?}")]
    RedisCreateError(#[from] deadpool_redis::CreatePoolError),
    #[error("RedisPoolError{0:#?}")]
    RedisPoolError(#[from] deadpool_redis::PoolError),
    #[error("RedisBuildError{0:#?}")]
    RedisBuildError(#[from] deadpool_redis::BuildError),
    #[error("RedisError{0:#?}")]
    RedisError(#[from] redis::RedisError),
    //mysql错误
    #[error("SqlxError{0:#?}")]
    SqlxError(#[from] sqlx::Error),
    //未知错误
    #[error("UnknownError{0:#?}")]
    UnknownError(#[from] anyhow::Error),
    //线程运行错误
    #[error("线程错误Error{0:#?}")]
    ThreadError(#[from] JoinError),
    //IO错误
    #[error("IoError{0:#?}")]
    IoError(#[from] io::Error),
    //时间错误
    #[error("事件错误{0:#?}")]
    TimeError(#[from] tokio::time::error::Error),
    //信号错误
    #[error("信号错误{0:#?}")]
    AcquireError(#[from] AcquireError),
    //解析错误
    #[error("解码错误{0:#?}")]
    DecodeError(#[from] DecodeError),
    //编码错误
    #[error("Utf8Error{0:#?}")]
    Utf8Error(#[from] Utf8Error),
    //网络错误
    #[error("HttpRequestError{0:#?}")]
    HttpRequestError(#[from] reqwest::Error),
    //缓存错误
    #[error("CacheError{0:#?}")]
    CacheError(#[from] stretto::CacheError),
    //写入错误
    #[error("StorageError{0:#?}")]
    StorageError(#[from] cacache::Error),
    //查询错误
    #[error("ORMError{0:#?}")]
    ORMError(#[from] sea_orm::DbErr),
    //查询错误
    #[error("ORM_Rab_Error{0:#?}")]
    OrmRabError(#[from] rbdc::Error),
}
