use std::ops::{AddAssign, Deref, DerefMut};
use std::sync::Arc;

use hashbrown::HashSet;
use spin::rwlock::RwLock;
use tokio::spawn;

use crate::Subject;
use crate::aggregation::Aggregation;
use crate::base::FutureEx;
use crate::function_frame::Execution;

///# 控制机
#[derive(Clone)]
pub enum Overmaster {
	//信号
	Subject(Subject),
	//读写
	RwlockBoolean(Arc<parking_lot::RwLock<bool>>),
	//没有
	Null,
}

///# 管理器
pub struct Alex<'life, NT: Sized + Send> {
	pub alex: Overmaster,
	pub execution: HashSet<Execution<'life, Overmaster, NT>>,
}

impl Default for Overmaster {
	fn default() -> Self {
		Overmaster::Subject(Arc::new((Default::default(), Default::default())))
	}
}

impl<'life, NT: Sized + Send + 'static> Default for Alex<'life, NT> {
	fn default() -> Self {
		Alex {
			alex: Default::default(),
			execution: Default::default(),
		}
	}
}

impl<'life, NT: Sized + Send + Sync + 'static> AddAssign for Alex<'life, NT> {
	fn add_assign(&mut self, rhs: Self) {
		rhs.execution.into_iter().for_each(|x| {
			self.execution.insert(x);
		});
	}
}

impl<NT: Sized + Send> Deref for Alex<'static, NT> {
	type Target = HashSet<Execution<'static, Overmaster, NT>>;
	fn deref(&self) -> &Self::Target {
		&self.execution
	}
}

impl<NT: Sized + Send> DerefMut for Alex<'static, NT> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.execution
	}
}

impl<NT: Sized + Send> Alex<'static, NT> {
	///# 替换控制
	pub fn control_replacement(self, alex: Overmaster) -> Self {
		Alex {
			alex,
			execution: self.execution,
		}
	}
	///# 聚合生成
	pub fn aggregation(self) -> Aggregation<NT>
		where
			NT: 'static,
	{
		let alex = self.alex;
		let mut btree = Vec::default();
		self.execution.into_iter().for_each(|i| {
			i.execution.into_iter().for_each(|i| {
				let mc = alex.clone();
				btree.push(spawn(async move {
					let mc = mc;
					match i {
						FutureEx::AsyncTrait(e) => e.await,
						FutureEx::AsyncTraitSimple(e) => e.await,
						FutureEx::AsyncFnTrait(mut e) => e(mc).await,
						FutureEx::AsyncFnTraitSimple(mut e) => e(mc).await,
						FutureEx::FnTrait(mut e) => e(mc),
						FutureEx::FnTraitSimple(mut e) => e(mc),
						FutureEx::SuperTrait(mut e) => { e.run(mc).await }
					}
				}));
			});
		});
		Aggregation(btree)
	}
	///# 克隆
	pub fn arc_clone(&self) -> Overmaster {
		self.alex.clone()
	}
	///# 数据锁定
	pub fn rwlock(self) -> RwLock<HashSet<Execution<'static, Overmaster, NT>>> {
		RwLock::new(self.execution)
	}
}
