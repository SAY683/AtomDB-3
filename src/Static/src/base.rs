use crate::closure::FutureAdapter;
use crate::closure::type_impl::{Alpha, AlphaSimple, Beta, BetaSimple, Gamma, GammaSimple};

///# 基本存储函数
pub enum FutureEx<'life, RE: Send, GX: Send> {
	///# 异步函数
	AsyncTrait(Alpha<'life, GX>),
	AsyncTraitSimple(AlphaSimple<'life, GX>),
	///# 异步闭包函数
	AsyncFnTrait(Beta<'life, RE, GX>),
	AsyncFnTraitSimple(BetaSimple<'life, RE, GX>),
	///# 普通函数
	FnTrait(Gamma<'life, RE, GX>),
	FnTraitSimple(GammaSimple<'life, RE, GX>),
	///# 超级
	SuperTrait(Box<dyn FutureAdapter<Args = RE, RE = GX> + Send>),
}

impl<'life, RE: Send + 'life, GX: Send> FutureEx<'life, RE, GX> {
	///# 运行
	pub async fn run(&mut self, arg: RE) -> GX {
		match self {
			FutureEx::AsyncTrait(e) => e.await,
			FutureEx::AsyncFnTrait(e) => e(arg).await,
			FutureEx::FnTrait(e) => e(arg),
			FutureEx::AsyncTraitSimple(e) => e.await,
			FutureEx::AsyncFnTraitSimple(e) => e(arg).await,
			FutureEx::FnTraitSimple(e) => e(arg),
			FutureEx::SuperTrait(e) => { e.run(arg).await }
		}
	}
}
