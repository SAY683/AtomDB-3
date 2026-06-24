use std::ops::Deref;
use deadpool_redis::redis::AsyncCommands;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use Static::Events;
use crate::setting::database_config::{Database, Service};
use crate::setting::local_config::{SUPER_DLR_URL, SUPER_URL};
use crate::sql_url::OrmEX;
use crate::system::{InstallUtils, Json};

pub async fn build_redis() -> Events<()> {
    let mut eg = SUPER_URL.deref().load().postgres.connect_bdc().await?;
    let conn = eg.acquire().await?;
    let xe = Database::select_by_map(&conn, rbs::value!{}).await?;
    let time = SUPER_DLR_URL.load().time.clone();
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    let xd = SUPER_URL.deref().load().redis.redis_pool().await?;
    let mut cmd = xd.get().await?;
    for i in xe {
        let xv = Service::select_by_map(&conn, rbs::value!{"uuid": i.uuid.clone()}).await?.into_par_iter().find_any(|ie| {
            ie.uuid == i.uuid
        }).unwrap_or(Service::default());
        let bir = serde_json::to_string(&Response {
            name: xv.name.to_string(),
            hash: i.hash,
            port: format!("{}/{}", i.port, xv.name),
            mode: xv.mode,
        }).unwrap();
        if time == 0 {
            cmd.set_ex::<_, _, bool>(i.uuid, bir, time as u64).await.unwrap();
        } else {
            cmd.set::<_, _, bool>(i.uuid, bir).await.unwrap();
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub name: String,
    pub hash: String,
    pub port: String,
    pub mode: String,
}

impl Default for Response {
    fn default() -> Self {
        Response {
            name: "?".to_string(),
            hash: "?".to_string(),
            port: "?".to_string(),
            mode: "?".to_string(),
        }
    }
}

impl InstallUtils for Response {}

impl Json for Response {}