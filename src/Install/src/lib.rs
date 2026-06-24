#![feature(associated_type_defaults)]

extern crate core;

use std::env::current_dir;
use std::env::current_exe;
use std::path::PathBuf;

use anyhow::Result;
use lazy_static::lazy_static;
use crate::setting::local_config::SUPER_DLR_URL;

pub mod system;
pub mod io;
pub mod setting;
pub mod sql_url;
pub mod web;
pub mod rei;


///# 发布
pub const NTS: bool = false;
///# 文件
const DATA: &str = "Data";
const DATA_DB: &str = "Atomic";

const BIN: &str = "Bin";
const SYSTEM_FILE: &str = "atom.toml";
const SYSTEM_DIR_FILE: &str = "atomic.toml";

const HTML_WEB: &str = "Html";
const HTML_INDEX: &str = "index.html";

const LOGS: &str = "Logs";
const LOGS1: &str = "atomic.logs";

//文件路径
lazy_static! {
     ///主网页
     pub static ref LOCAL_BIN_WEB: PathBuf = {
        let mut x = PathBuf::new();
        x.push(LOCAL_PATH.as_ref().unwrap().as_path());
        x.push(BIN);
        x.push(HTML_WEB);
        x.push(HTML_INDEX);
        x
    };
    ///APL
    pub static ref LOCAL_BIN_APL: PathBuf = {
        let mut x = PathBuf::new();
        x.push(LOCAL_PATH.as_ref().unwrap().as_path());
        x.push(BIN);
        x.push(SUPER_DLR_URL.load().apl.as_path());
        x
    };
    //log位置
    pub static ref LOCAL_BIN_LOGS: PathBuf = {
        let mut x = PathBuf::new();
        x.push(LOCAL_PATH.as_ref().unwrap().as_path());
        x.push(LOGS);
        x.push(LOGS1);
        x
    };
    //设置位置
    pub static ref LOCAL_BIN_FILR: PathBuf = {
        let mut x = PathBuf::new();
        x.push(LOCAL_PATH.as_ref().unwrap().as_path());
        x.push(BIN);
        x.push(SYSTEM_FILE);
        x
    };
       pub static ref LOCAL_BIN_DIR_FILR: PathBuf = {
        let mut x = PathBuf::new();
        x.push(LOCAL_PATH.as_ref().unwrap().as_path());
        x.push(BIN);
        x.push(SYSTEM_DIR_FILE);
        x
    };
    //默认存储位置
    pub static ref LOCAL_DEF_DB: PathBuf = {
        let mut x = PathBuf::new();
        x.push(LOCAL_PATH.as_ref().unwrap().as_path());
        x.push(DATA);
        x.push(DATA_DB);
        x
    };
    //我的位置
    pub static ref LOCAL_PATH: Result<PathBuf> = Ok(if NTS {
        let mut et = current_exe()?;
        et.pop();
        et
    } else {
        current_dir()?
    });
}
