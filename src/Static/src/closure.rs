use std::any::{Any, TypeId};
use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//# 基本存储函数
pub enum AlphaSuper<'life, GX: Send> {
	///# 异步函数
	AsyncTrait(Alpha<'life, GX>),
	SimpleAsyncTrait(AlphaSimple<'life, GX>),
}

pub mod type_impl {
	use super::*;
	
	///# 异步
	pub type Alpha<'life, RE> = Pin<Box<dyn Future<Output = RE> + Send + Sync + 'life>>;
	///# 异步闭包 Box<dyn FnMut(RE) -> Alpha<'life, GX> + Send + Sync + 'life>;
	pub type Beta<'life, RE, GX> = Gamma<'life, RE, crate::closure::Alpha<'life, GX>>;
	///# 闭包
	pub type Gamma<'life, RE, GX> = Box<dyn FnMut(RE) -> GX + Send + Sync + 'life>;
	///# 异步简单
	pub type AlphaSimple<'life, RE> = Pin<Box<dyn Future<Output = RE> + Send + 'life>>;
	///# 异步简单
	pub type BetaSimple<'life, RE, GX> = GammaSimple<'life, RE, crate::closure::AlphaSimple<'life, GX>>;
	///# 闭包简单
	pub type GammaSimple<'life, RE, GX> = Box<dyn FnMut(RE) -> GX + Send + 'life>;
}

///# 异步闭包 Box<dyn FnMut(RE) -> Alpha<'life, GX> + Send + Sync + 'life>;
pub struct Beta<'life, RE, GX>(pub Gamma<'life, RE, Alpha<'life, GX>>);

///# 闭包
pub struct Gamma<'life, RE, GX>(pub Box<dyn FnMut(RE) -> GX + Send + Sync + 'life>);

///# 异步简单
pub struct BetaSimple<'life, RE, GX>(pub GammaSimple<'life, RE, AlphaSimple<'life, GX>>);

///# 闭包简单
pub struct GammaSimple<'life, RE, GX>(pub Box<dyn FnMut(RE) -> GX + Send + 'life>);

///# 异步接口
#[async_trait]
pub trait FutureAdapter {
	type Args: Send;
	type RE;
	async fn run(&mut self, arg: Self::Args) -> Self::RE;
}

impl<'life, ND, SE> dyn FutureAdapter<Args = ND, RE = SE> + 'life {
	///# 获取结构id
	pub async fn typ_id(&mut self) -> TypeId
		where
			SE: Any + 'life,
			Self: FutureAdapter<Args = ()>,
	{
		self.run(()).await.type_id()
	}
}

#[async_trait]
impl<'life, ARG: Send, RE> FutureAdapter for BetaSimple<'life, ARG, RE> {
	type Args = ARG;
	type RE = RE;
	async fn run(&mut self, arg: Self::Args) -> Self::RE {
		self.0.0(arg).await
	}
}

#[async_trait]
impl<'life, ARG: Send, RE> FutureAdapter for Beta<'life, ARG, RE> {
	type Args = ARG;
	type RE = RE;
	async fn run(&mut self, arg: Self::Args) -> Self::RE {
		self.0.0(arg).await
	}
}

#[async_trait]
impl<'life, ARG: Send, RE> FutureAdapter for GammaSimple<'life, ARG, RE> {
	type Args = ARG;
	type RE = RE;
	async fn run(&mut self, arg: Self::Args) -> Self::RE {
		self.0(arg)
	}
}

#[async_trait]
impl<'life, ARG: Send, RE> FutureAdapter for Gamma<'life, ARG, RE> {
	type Args = ARG;
	type RE = RE;
	async fn run(&mut self, arg: Self::Args) -> Self::RE {
		self.0(arg)
	}
}

///# 异步
pub type Alpha<'life, RE> = Pin<Box<dyn Future<Output = RE> + Send + Sync + 'life>>;
///# 异步简单
pub type AlphaSimple<'life, RE> = Pin<Box<dyn Future<Output = RE> + Send + 'life>>;


#[async_trait]
impl<'life, RE: Send> FutureAdapter for AlphaSuper<'life, RE> {
	type Args = ();
	type RE = RE;
	///# args=()
	async fn run(&mut self, _: Self::Args) -> Self::RE {
		self.run_gcc().await
	}
}

impl<'life, GX: Send> AlphaSuper<'life, GX> {
	///# 运行
	pub async fn run_gcc(&mut self) -> GX {
		match self {
			AlphaSuper::AsyncTrait(e) => e.await,
			AlphaSuper::SimpleAsyncTrait(e) => e.await,
		}
	}
}
