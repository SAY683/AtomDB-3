#![feature(trivial_bounds)]
#![feature(core_intrinsics)]
#![feature(maybe_uninit_array_assume_init)]
#![feature(box_into_inner)]
#![feature(async_fn_in_trait)]
#![feature(impl_trait_projections)]

use std::any::Any;
use std::env::current_dir;
use std::env::current_exe;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use bevy_reflect::Reflect;
use futures::executor::LocalPool;
use futures::task::SpawnExt;
use lazy_static::lazy_static;
use parking_lot::Condvar;
use parking_lot::Mutex;
pub use rayon::prelude::*;
use sync_cow::SyncCow;
use tokio::spawn;

use alex::Overmaster;
use Error::ThreadEvents;

use crate::aggregation::Aggregation;
use crate::base::FutureEx;
use crate::closure::Gamma;
use crate::function_frame::Execution;

#[macro_use]
pub mod macros;
pub mod closure;
pub mod static_array;
pub mod aggregation;
pub mod alex;
pub mod base;
pub mod function_frame;
pub mod ssh;

///# 控制
///# Arc<(Mutex<bool>, Condvar)>
pub type Subject = Arc<(Mutex<bool>, Condvar)>;
///# 异常返回
pub type Null = Result<()>;
///# 事件返回
pub type Events<UC> = Result<UC, ThreadEvents>;

///# 命令
const BIN_DB: &str = "Data";
///DB存储位置
const BIN_DDB: &str = "DB";
///# 发布
pub const NTS: bool = false;
//文件路径
lazy_static! {
    pub static ref LOCAL_DB: PathBuf = {
        let mut x = PathBuf::new();
        x.push(LOCAL_PATH.as_ref().unwrap().as_path());
        x.push(BIN_DB);
        x
    };
    pub static ref LOCAL_SQL: PathBuf = {
        let mut x = PathBuf::new();
        x.push(LOCAL_PATH.as_ref().unwrap().as_path());
        x.push(BIN_DDB);
        x
    };
    pub static ref LOCAL_PATH: Result<PathBuf> = Ok(if NTS {
        let mut et = current_exe()?;
        et.pop();
        et
    } else {
        current_dir()?
    });
}

/*
Example::run(Example::aggregation()).await.unwrap();
*/
#[derive(Copy, Clone, Debug, Reflect)]
pub struct Example;

impl Alexia<Example> for Example {
    fn event() -> Vec<FutureEx<'static, Overmaster, Events<Example>>> {
        vec![FutureEx::SuperTrait(Box::new(Gamma(Box::new(|_| {
            println!("Hello, world!");
            Ok(Example)
        }))))]
    }
}

pub trait Alexia<NTD: Send + Sized + Sync>: Any + Reflect {
    ///# 单线程运行
    fn submit(e: Vec<std::thread::JoinHandle<NTD>>) -> Events<Arc<SyncCow<Vec<NTD>>>>
        where
            NTD: Clone + 'static,
    {
        let ert = Arc::new(SyncCow::new(vec![]));
        e.into_par_iter()
            .for_each(|i| ert.edit(|x| x.push(i.join().expect("SubmitError"))));
        Ok(ert)
    }
    ///# 事件
    fn event() -> Vec<FutureEx<'static, Overmaster, Events<NTD>>>;
    ///# 执行聚合
    fn exaction() -> Execution<'static, Overmaster, NTD>
        where
            NTD: 'static,
    {
        Execution::from(<Self as Alexia<NTD>>::event())
    }
    ///# 线程聚合
    fn aggregation() -> Aggregation<NTD>
        where
            NTD: 'static,
    {
        Aggregation::from([<Self as Alexia<NTD>>::exaction()])
    }
    ///# 实体运行
    async fn run(value: Aggregation<NTD>) -> Events<Vec<NTD>>
        where
            NTD: 'static,
    {
        let mut ert = vec![];
        for i in value.into_iter() {
            ert.push(match i.await {
                Ok(e) => Ok(e?),
                Err(e) => Err(ThreadEvents::ThreadError(e)),
            }?);
        }
        Ok(ert)
    }
    ///# 虚拟运行
    async fn sync(value: Aggregation<NTD>) -> Events<Arc<SyncCow<Vec<NTD>>>>
        where
            NTD: 'static + Clone,
    {
        let ert = Arc::new(SyncCow::new(vec![]));
        value.0.into_iter().for_each(|i| {
            let er = ert.clone();
            spawn(async move {
                let o = i.await;
                er.edit(|x| {
                    x.push(o.unwrap().unwrap());
                });
            });
        });
        Ok(ert)
    }
    ///# 并行运行
    async fn alliance(value: Aggregation<NTD>) -> Events<Arc<SyncCow<Vec<NTD>>>>
        where
            NTD: 'static + Clone,
    {
        let ert = Arc::new(SyncCow::new(vec![]));
        value.0.into_par_iter().for_each(|i| {
            let er = ert.clone();
            rayon::spawn(|| {
                let mut u = LocalPool::new();
                u.spawner()
                    .spawn(async move {
                        let o = i
                            .await
                            .unwrap_or_else(|x| {
                                panic!("{x}");
                            })
                            .unwrap_or_else(|x| {
                                panic!("{x}");
                            });
                        er.edit(|x| {
                            x.push(o);
                        });
                    })
                    .unwrap();
                u.run();
            });
        });
        Ok(ert)
    }
}

///#通信
pub mod communicate_src {
    use std::sync::{Arc, Mutex};

    use async_channel::{Receiver, Sender};

    ///# Tok_io 提供了多种消息通道，可以满足不同场景的需求:
    ///  ####
    /// [mpsc], [多生产]者，[单消费]者模式
    ///  ####
    /// [ones_hot], [单生产]者[单消费]，[一次]只能[发送]一条消息
    ///  ####
    /// [broadcast]，[多生产]者，[多消费]者，其中每一条发送的消息都可以被所有接收者收到，因此是[广播]
    ///  ####
    /// [watch]，[单生产]者，[多消费]者，只[保存一条最新]的消息，因此接收者只能看到最近的一条消息，例如，这种模式适用于配置文件变化的监听
    ///  ####
    /// [async-channel][多生产]者、[多消费]者，且[每一条消息只能被其中一个]消费者接收

    ///[基本]通信
    pub struct Basic<G: Sized + Send + Sync>
    (pub Arc<Mutex<Sender<G>>>,
     pub Arc<Mutex<Receiver<G>>>);

    ///# 记录
    pub mod record {
        use std::borrow::Cow;
        use std::cell::Cell;
        use std::collections::{BinaryHeap, BTreeSet, HashMap};
        use std::mem::ManuallyDrop;
        use std::ptr::NonNull;
        use std::sync::{Arc, Weak};

        ///[文本]
        pub struct Txt<'ef>(pub Arc<BTreeSet<&'ef str>>);

        ///[数字]
        pub struct Number(pub Cell<f64>);

        ///[段落]
        #[derive(Eq, Ord, PartialOrd, PartialEq, Hash)]
        pub struct Bit<'ef>(pub Cow<'ef, &'ef str>);

        ///[字典]
        pub struct Dictionary<'ef>(pub HashMap<Bit<'ef>, &'ef str>);

        ///[引用]
        pub struct Learn<Gc: Sized + Send + Send>(pub Weak<Gc>);

        ///[队列]
        pub struct Queue(pub BinaryHeap<i64>);

        ///[特殊类型]
        pub union SpecialYield<'ef> {
            pub reference: *const f64,
            pub record_point: *mut &'ef str,
            pub synchronize: ManuallyDrop<String>,
            pub non: NonNull<*mut &'ef str>,
        }
    }
}