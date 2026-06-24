use std::ops::Deref;
use anyhow::anyhow;
use bevy_reflect::Reflect;
use fast_log::{Config};
use ftlog::{LevelFilter};
use Error::ThreadEvents;
use Install::io::DiskWrite;
use Install::LOCAL_BIN_LOGS;
use Install::setting::local_config::{SUPER_DLR_URL, SUPER_URL};
use Install::web::{web};
use Static::{Alexia, Events};
use Static::alex::Overmaster;
use Static::base::FutureEx;
use View::{Colour, Information, ViewDrive};
use crate::build::log::{log_info, log_info_stop, ORD1, ORD2, ORD3, ORD4, OUT_LOG, OUT_LOG_1};
use crate::test::test_get_db;
use Install::rei::build_redis;

#[derive(Copy, Clone, Reflect, Debug)]
pub struct Burden;

impl Alexia<Burden> for Burden {
    fn event() -> Vec<FutureEx<'static, Overmaster, Events<Burden>>> {
        vec![
            //初始化
            FutureEx::AsyncFnTraitSimple(Box::new(|e| Box::pin(async move {
                init(e).await?;
                Ok(Burden)
            }))),
            //网页统计
            FutureEx::AsyncFnTraitSimple(Box::new(|e| Box::pin(async {
                view(e).await?;
                Ok(Burden)
            }))),
            //后端负载
            FutureEx::AsyncFnTraitSimple(Box::new(|e| Box::pin(async {
                cache(e).await?;
                Ok(Burden)
            })))]
    }
}

///# 网页
pub async fn view(mut e: Overmaster) -> Events<()> {
    if let Overmaster::Subject(ref mut e) = e {
        e.1.wait(&mut e.0.lock());
        web().await?.await?;
    }
    Ok(())
}

///# redis 下载服务
pub async fn cache(mut e: Overmaster) -> Events<()> {
    if let Overmaster::Subject(ref mut e) = e {
        e.1.wait(&mut e.0.lock());
        build_redis().await?;
    }
    Ok(())
}

///# 内核
pub async fn init(mut e: Overmaster) -> Events<()> {
    //日志设置
    if let true = SUPER_DLR_URL.deref().load().view {
        let log_path = LOCAL_BIN_LOGS.as_path().to_str().map(|s| s.to_string()).unwrap_or_else(|| "atomic.log".to_string());
        fast_log::init(Config::new().level(LevelFilter::Debug).file(&log_path).console())?;
    }
    if db_build().await? {
        log_info();
        //控制通知
        if let Overmaster::Subject(ref mut e) = e {
            *e.deref().0.lock() = true;
            e.1.notify_all();
        }

        // 环境变量 ATOMDB_HEADLESS=true 时跳过交互菜单，直接启动 Web
        if std::env::var("ATOMDB_HEADLESS").as_deref() == Ok("true") {
            println!("[AtomDB] 无头模式: 跳过菜单，Web 服务器将自动启动");
            return Ok(());
        }

        'life: loop {
            // 如果检测到非交互终端，自动退出菜单循环
            if std::env::var("ATOMDB_HEADLESS").as_deref() == Ok("true") {
                println!("[AtomDB] 非交互环境，跳过菜单");
                break 'life;
            }
            let index = vec![ORD1, ORD3, ORD4, ORD2];
            let choice = match Colour::select_func_column(&index, OUT_LOG_1) {
                Ok(i) => i,
                Err(_) => {
                    println!("[AtomDB] 检测到非交互终端，启动无头模式 (设置 ATOMDB_HEADLESS=true)");
                    break 'life;
                }
            };
            match index[choice] {
                ORD1 => {
                    //写入
                    println!("[AtomDB] 开始写入任务…");
                    DiskWrite::alliance(DiskWrite::aggregation()).await?;
                    println!("[AtomDB] 写入完成");
                }
                ORD3 => {
                    let xx = format!("http:{}{}", SUPER_DLR_URL.load().port, SUPER_DLR_URL.load().path);
                    println!("[AtomDB] 管理面板: {}", xx);
                    if SUPER_DLR_URL.load().auto {
                        opener::open(xx).unwrap_or_else(|e| {
                            eprintln!("{}", e);
                        });
                    }
                }
                ORD4 => {
                    println!("[AtomDB] Redis 缓存同步…");
                    match build_redis().await {
                        Ok(_) => println!("[AtomDB] Redis 同步完成"),
                        Err(e) => eprintln!("[AtomDB] Redis 同步失败 (可忽略): {:?}", e),
                    }
                }
                ORD2 => {
                    //结束
                    log_info_stop();
                    break 'life;
                }
                e => {
                    println!("[AtomDB] 未知选项: {}", e);
                }
            }
        }
    } else {
        return Err(ThreadEvents::UnknownError(anyhow!("安全退出")));
    }
    Ok(())
}

//生成表
async fn db_build() -> Events<bool> {
    let xe = test_get_db().await?;
    return match xe.is_empty() {
        true => { Ok(true) }
        false => {
            match Colour::view_container("Postgres数据表损坏是否新建？") {
                Ok(e) => {
                    match e {
                        true => {
                            let mut xr = vec![];
                            for e in xe {
                                xr.push(vec![format!("{}", SUPER_URL.deref().load().postgres.connect_rab_execute(e).await?)]);
                            }
                            println!("{}", Colour::Monitoring.table(Information { list: vec![OUT_LOG.to_string()], data: xr }));
                            Ok(true)
                        }
                        false => { Err(ThreadEvents::UnknownError(anyhow!("安全退出"))) }
                    }
                }
                Err(e) => { Err(ThreadEvents::IoError(e)) }
            }
        }
    };
}

pub mod log {
    use std::ops::Deref;
    use Install::setting::local_config::SUPER_DLR_URL;
    use View::{Colour, Information, ViewDrive};

    pub const OUT_LOG: &str = "事件";
    pub const OUT_LOG_1: &str = "菜单";
    pub const ORD1: &str = "写入";
    pub const ORD2: &str = "结束";
    pub const ORD3: &str = "网页";
    pub const ORD4: &str = "缓存";

    ///# 开始显示
    pub fn log_info() {
        println!("{}", Colour::Output.table(Information { list: vec!["AtomicDB".to_string()], data: vec![vec![format!("基本端口{}", SUPER_DLR_URL.deref().load().port.to_string())]] }))
    }

    ///# 结束显示
    pub fn log_info_stop() {
        println!("{}", Colour::Monitoring.table(Information { list: vec!["AtomicDB".to_string(), "说明".to_string()], data: vec![vec![format!("事务结束"), format!("请输入 [Ctrl-C] 结束网页进程")]] }))
    }
}