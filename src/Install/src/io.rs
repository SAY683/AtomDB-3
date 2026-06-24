use std::fmt::{Display, Formatter};
use std::fs::{File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use bevy_reflect::Reflect;
use cacache;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};
use ssri::{Algorithm, Integrity};
use uuid::{Uuid};
use Static::{Alexia, Events, LOCAL_DB};
use Static::alex::Overmaster;
use Static::base::FutureEx;
use View::{Colour, ViewDrive};
use crate::io::file_handler::{write_dds};

const HASH_DB: &str = "HASH";
const MAP_DB: &str = "MAP";
const CACHE_DB: &str = "CACHE";
pub const DB_MODES: [&str; 3] = [HASH_DB, MAP_DB, CACHE_DB];

#[derive(Copy, Clone, Debug, Reflect)]
pub struct DiskWrite;

impl Alexia<DiskWrite> for DiskWrite {
    fn event() -> Vec<FutureEx<'static, Overmaster, Events<DiskWrite>>> {
        write_dds(Colour::input_column("同步文件夹位置").unwrap().as_str(), DiskMode::from(DB_MODES))
    }
}

#[derive(Copy, Eq, Clone, PartialEq, Debug, Reflect, Serialize, Deserialize)]
pub enum DiskMode {
    Hash,
    Map,
    Cache,
}

impl Into<String> for DiskMode {
    fn into(self) -> String {
        match self {
            DiskMode::Hash => { "HASH_Mode".to_string() }
            DiskMode::Map => { "MAP_Mode".to_string() }
            DiskMode::Cache => { "CACHE_Mode".to_string() }
        }
    }
}

impl From<String> for DiskMode {
    fn from(value: String) -> Self {
        match value.as_str() {
            "HASH_Mode" => { DiskMode::Hash }
            "HAP_Mode" => { DiskMode::Map }
            "CACHE_Mode" => { DiskMode::Cache }
            _ => { panic!("NoToken") }
        }
    }
}

impl From<[&'static str; 3]> for DiskMode {
    fn from(value: [&'static str; 3]) -> Self {
        match Colour::select_func_column(&value, "选择模式").unwrap() {
            0 => {
                DiskMode::Hash
            }
            1 => {
                DiskMode::Map
            }
            2 => {
                DiskMode::Cache
            }
            _ => { DiskMode::Map }
        }
    }
}

impl Display for DiskMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskMode::Hash => { write!(f, "{}", HASH_DB) }
            DiskMode::Map => { write!(f, "{}", MAP_DB) }
            DiskMode::Cache => { write!(f, "{}", CACHE_DB) }
        }
    }
}

pub mod file_handler {
    use std::{fs};
    use std::net::{SocketAddr};
    use std::ops::Deref;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use anyhow::anyhow;
    use serde_json::json;
    use spin::RwLock;
    use uuid::fmt::Urn;
    use uuid::Uuid;
    use Error::ThreadEvents;
    use Static::alex::Overmaster;
    use Static::base::FutureEx;
    use Static::Events;
    use View::{Colour, Information, ViewDrive};
    use crate::io::{Disk, DiskMode, DiskWrite, KVStore};
    use crate::LOCAL_BIN_APL;
    use crate::setting::database_config::{Database, Service};
    use crate::setting::local_config::{SUPER_DLR_URL, SUPER_URL};
    use crate::sql_url::{OrmEX};

    ///正式操作
    pub fn write_dds(file: &str, modes: DiskMode) -> Vec<FutureEx<'static, Overmaster, Events<DiskWrite>>> {
        println!("{}", Colour::Output.table(Information { list: vec!["File|文件", "Modes|模式"], data: vec![vec![format!("{}", &file).as_str(), format!("{}", &modes).as_str()]] }));
        let mut xls = vec![];
        match kv_as_disk_modes(file) {
            Ok(psa) => {
                let play = Arc::new(RwLock::new(Colour::view_column(psa.len() as u64)));
                let name = Arc::new(Colour::input_column("name").unwrap_or_else(|_| "default".to_string()));
                let server = Arc::new({
                    let addr = loop {
                        match Colour::input_column_def("network", "127.0.0.1:"){
                            Ok(input) => match input.parse::<SocketAddr>() {
                                Ok(e) => break e,
                                Err(e) => eprintln!("{}", e),
                            },
                            Err(_) => break "127.0.0.1:8964".parse().unwrap(),
                        }
                    };
                    addr
                });
                let apl_content = Arc::new(fs::read_to_string(LOCAL_BIN_APL.as_path()).unwrap_or_else(|e| {
                    eprintln!("APL 文件读取失败 ({}): 使用空内容", e);
                    String::new()
                }));
                psa.into_iter().for_each(|i| {
                    let play = play.clone();
                    let name = name.clone();
                    let server = server.clone();
                    let file = apl_content.clone();
                    xls.push(FutureEx::AsyncTraitSimple(Box::pin(async move {
                        let kv = i;
                        play.write().inc(1);
                        let uuid = match kv.key.clone() {
                            Some(k) => k,
                            None => {
                                eprintln!("跳过无 key 的文件条目");
                                return Err(ThreadEvents::UnknownError(anyhow!("KV 条目缺少 key")));
                            }
                        };
                        let uuid_str = uuid.to_string();
                        let kv = KVStore {
                            hash: None,
                            key: Some(uuid_str.clone()),
                            value: kv.value,
                        };
                        match match modes {
                            DiskMode::Hash => {
                                Some(kv.hash_write().await)
                            }
                            DiskMode::Map => {
                                Some(kv.write_buf().await)
                            }
                            DiskMode::Cache => {
                                match kv.link().await {
                                    Ok(e) => { Some(e) }
                                    Err(e) => {
                                        eprintln!("{:#?}", e);
                                        None
                                    }
                                }
                            }
                        } {
                            None => { return Err(ThreadEvents::UnknownError(anyhow!("信息损坏"))); }
                            Some(e) => {
                                let name = name.to_string();
                                let _sev = *server;
                                let mut set = SUPER_URL.deref().load().postgres.connect_bdc().await?;
                                let conn = set.acquire().await?;
                                let se = Database::insert(&conn, &Database {
                                    name: name.to_string(),
                                    uuid: uuid.to_string(),
                                    hash: e.to_string(),
                                    port: server.to_string(),
                                }).await?;
                                let se1 = Service::insert(&conn, &Service {
                                    uuid: uuid.to_string(),
                                    name: name.to_string(),
                                    logs: Some(file.to_string()),
                                    mode: modes.into(),
                                }).await?;
                                if let true = SUPER_DLR_URL.load().view {
                                    println!("{}", Colour::Monitoring.table(Information {
                                        list: vec!["数据库", "结果"],
                                        data: vec![
                                            vec!["database", json!(se).as_str().unwrap_or("Error")],
                                            vec!["database", json!(se1).as_str().unwrap_or("Error")],
                                        ],
                                    }));
                                }
                            }
                        }
                        //显示
                        if play.read().position() == 1 {
                            play.read().finish();
                        };
                        Ok(DiskWrite)
                    })));
                });
            }
            Err(e) => { eprintln!("{:?}", e); }
        }
        xls
    }

    #[derive(Debug)]
    pub enum FileStatus {
        Dir {
            value: Vec<FileStatus>,
        },
        File {
            value: PathBuf,
        },
    }

    ///# 文件查看
    pub fn file_list_with<P: AsRef<Path>>(t: P) -> Events<Vec<FileStatus>> {
        let mut ve = vec![];
        match fs::metadata(&t) {
            Ok(e) => {
                match e.is_dir() {
                    true => {
                        for entry in fs::read_dir(t)? {
                            let x = entry?.path();
                            match x.is_dir() {
                                true => {
                                    ve.push(FileStatus::Dir { value: file_list_with(x)? });
                                }
                                false => { ve.push(FileStatus::File { value: x }); }
                            }
                        }
                    }
                    false => { return Err(ThreadEvents::UnknownError(anyhow!("错误不是目录"))); }
                }
            }
            Err(e) => { return Err(ThreadEvents::IoError(e)); }
        }
        Ok(ve)
    }

    fn bit(e: FileStatus) -> Vec<KVStore<Uuid, PathBuf>> {
        let mut x = vec![];
        match e {
            FileStatus::Dir { value } => {
                value.into_iter().for_each(|e| {
                    let mut xe = bit(e);
                    x.append(&mut xe);
                });
            }
            FileStatus::File { value } => {
                x.push(KVStore { hash: None, key: Some(Urn::from_uuid(Uuid::new_v4()).into_uuid()), value });
            }
        }
        x
    }

    pub fn kv_store<P: AsRef<Path>>(ts: P) -> Events<Vec<KVStore<Uuid, PathBuf>>> {
        let mls = file_list_with(ts)?;
        let mut x = vec![];
        mls.into_iter().for_each(|e| {
            let mut ml = bit(e);
            x.append(&mut ml);
        });
        Ok(x)
    }

    ///# 写入
    pub fn kv_as_disk_modes<P: AsRef<Path>>(ts: P) -> Events<Vec<KVStore<Uuid, Vec<u8>>>> {
        let mut x = vec![];
        kv_store(ts)?.into_iter().for_each(|e| {
            if let Some(key) = e.key {
                x.push(KVStore::from((key, e.value)));
            }
        });
        Ok(x)
    }
}

///# 存储类型
/// let x = KVStore::from(("21".to_string(), BufReader::new(File::open("./Bin/atom.toml").unwrap())));
/// x.write().await;
/// println!("{}", x.read().await);
pub struct KVStore<RF: ToString, RG: Sized> {
    /// 文件位置
    pub hash: Option<RF>,
    /// 值
    pub key: Option<RF>,
    pub value: RG,
}

impl From<String> for KVStore<String, String> {
    fn from(value: String) -> Self {
        KVStore { hash: None, key: None, value }
    }
}

impl From<(String, String)> for KVStore<String, String> {
    fn from(value: (String, String)) -> Self {
        KVStore { hash: None, key: Some(value.0), value: value.1 }
    }
}

///# 名称 路径
impl From<(String, PathBuf)> for KVStore<String, Vec<u8>> {
    fn from(value: (String, PathBuf)) -> Self {
        let mut xx = vec![];
        let mut mlx = BufReader::new(File::open(value.1).unwrap());
        mlx.read_to_end(&mut xx).unwrap();
        KVStore {
            hash: None,
            key: Some(value.0),
            value: xx,
        }
    }
}

impl From<(Uuid, PathBuf)> for KVStore<Uuid, Vec<u8>> {
    fn from(value: (Uuid, PathBuf)) -> Self {
        let xx = match File::open(&value.1) {
            Ok(f) => {
                let mut buf = vec![];
                let mut reader = BufReader::new(f);
                let _ = reader.read_to_end(&mut buf);
                buf
            }
            Err(e) => {
                eprintln!("无法读取文件 {:?}: {}", value.1, e);
                vec![]
            }
        };
        KVStore {
            hash: None,
            key: Some(value.0),
            value: xx,
        }
    }
}

impl From<KVStore<Uuid, Vec<u8>>> for KVStore<String, Vec<u8>> {
    fn from(value: KVStore<Uuid, Vec<u8>>) -> Self {
        KVStore {
            hash: None,
            key: Some(value.key.unwrap().to_string()),
            value: value.value,
        }
    }
}

///# 磁盘
pub trait Disk {
    const ERROR_INVALID: &'static str;
    ///存储
    fn file() -> String {
        LOCAL_DB.as_path().to_str().unwrap().to_string()
    }
    //转换
    fn string_with(e: Vec<u8>) -> Self::Read;
    ///sql存储
    fn file_storage() -> String {
        unimplemented!()
    }
    type Read;
    ///# KV
    async fn write(&self) -> Integrity;
    async fn read(&self) -> Self::Read;
    async fn remove(&self);
    async fn write_buf(&self) -> Integrity;
    async fn read_buf(&self) -> Self::Read;
    ///# V
    async fn hash_write(&self) -> Integrity;
    async fn hash_read(&self, ees: &Integrity) -> Self::Read;
    async fn remove_hash(&self, ees: &Integrity);
    ///# 链接
    async fn link(&self) -> Events<Integrity>;
    ///+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    /// 字符转换
    fn integrity_str(e: Integrity) -> String {
        e.to_string()
    }
    /// 转换
    fn hash_string(e: String) -> Integrity {
        e.parse().unwrap()
    }
    ///# 检查
    fn hash_check(sri: Integrity, e: &str) -> Algorithm {
        sri.check(e).unwrap()
    }
    ///# 时间
    fn io_time() -> DateTime<Utc> {
        Utc::now()
    }
    fn io_timestamp() -> NaiveDateTime {
        let x = KVStore::<String, String>::io_time();
        sea_orm::prelude::DateTime::new(x.date_naive(), x.time())
    }
}


impl<RF: AsRef<str> + Display, RG: AsRef<[u8]>> Disk for KVStore<RF, RG> {
    const ERROR_INVALID: &'static str = "ASYNC CACHE ERROR";
    fn string_with(e: Vec<u8>) -> Self::Read {
        e
    }
    type Read = Vec<u8>;
    async fn write(&self) -> Integrity {
        if let Some(ref e) = self.hash {
            cacache::write(e.to_string(), self.key.as_ref().unwrap(), self.value.as_ref()).await.expect(Self::ERROR_INVALID)
        } else {
            cacache::write(Self::file(), self.key.as_ref().unwrap(), self.value.as_ref()).await.expect(Self::ERROR_INVALID)
        }
    }
    async fn read(&self) -> Self::Read {
        Self::string_with(if let Some(ref e) = self.hash {
            cacache::read(e.to_string(), self.key.as_ref().unwrap()).await.expect(Self::ERROR_INVALID)
        } else {
            cacache::read(Self::file(), self.key.as_ref().unwrap()).await.expect(Self::ERROR_INVALID)
        })
    }
    async fn remove(&self) {
        if let Some(ref e) = self.hash {
            cacache::remove(e.to_string(), self.key.as_ref().unwrap()).await.expect(Self::ERROR_INVALID);
        } else {
            cacache::remove(Self::file(), self.key.as_ref().unwrap()).await.expect(Self::ERROR_INVALID);
        }
    }

    async fn write_buf(&self) -> Integrity {
        if let Some(ref e) = self.hash {
            let mut x = cacache::Writer::create(e.to_string(), self.key.as_ref().unwrap()).await.expect(Self::ERROR_INVALID);
            x.write_all(self.value.as_ref()).await.expect(Self::ERROR_INVALID);
            x.commit().await.expect(Self::ERROR_INVALID)
        } else {
            let mut x = cacache::Writer::create(Self::file(), self.key.as_ref().unwrap()).await.expect(Self::ERROR_INVALID);
            x.write_all(self.value.as_ref()).await.expect(Self::ERROR_INVALID);
            x.commit().await.expect(Self::ERROR_INVALID)
        }
    }

    async fn read_buf(&self) -> Self::Read {
        Self::string_with(if let Some(ref e) = self.hash {
            let mut x = cacache::Reader::open(e.to_string(), self.key.as_ref().unwrap()).await.expect(Self::ERROR_INVALID);
            let mut r = vec![];
            x.read_to_end(&mut r).await.expect(Self::ERROR_INVALID);
            r
        } else {
            let mut x = cacache::Reader::open(Self::file(), self.key.as_ref().unwrap()).await.expect(Self::ERROR_INVALID);
            let mut r = vec![];
            x.read_to_end(&mut r).await.expect(Self::ERROR_INVALID);
            r
        })
    }

    async fn hash_write(&self) -> Integrity {
        if let Some(ref e) = self.hash {
            cacache::write_hash(e.to_string(), self.value.as_ref()).await.expect(Self::ERROR_INVALID)
        } else {
            cacache::write_hash(Self::file(), self.value.as_ref()).await.expect(Self::ERROR_INVALID)
        }
    }
    async fn hash_read(&self, ees: &Integrity) -> Self::Read {
        Self::string_with(if let Some(ref e) = self.hash {
            cacache::read_hash(e.to_string(), ees).await.expect(Self::ERROR_INVALID)
        } else {
            cacache::read_hash(Self::file(), ees).await.expect(Self::ERROR_INVALID)
        })
    }
    async fn remove_hash(&self, ees: &Integrity) {
        if let Some(ref e) = self.hash {
            cacache::remove_hash(e.to_string(), ees).await.expect(Self::ERROR_INVALID)
        } else {
            cacache::remove_hash(Self::file(), ees).await.expect(Self::ERROR_INVALID)
        }
    }

    async fn link(&self) -> Events<Integrity> {
        Ok(if let Some(ref e) = self.hash {
            cacache::link_to(Path::new(&e.to_string()), self.key.as_ref().unwrap(), Path::new(std::str::from_utf8(self.value.as_ref())?)).await?
        } else {
            cacache::link_to(Path::new(&Self::file()), self.key.as_ref().unwrap(), Path::new(std::str::from_utf8(self.value.as_ref())?)).await?
        })
    }
}