use std::any::Any;
use std::hash::Hash;
use std::ops::{Index, IndexMut};
use std::time::Duration;

use arc_swap::ArcSwap;
use bevy_reflect::{FromReflect, Reflect};
use itertools::interleave;
use ph::BuildDefaultSeededHasher;
use ph::fmph::Function;
use rayon::iter::{
	IntoParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator,
};
use spin::Lazy;
use stretto::{AsyncCache, Cache};

///# 并发存储
///# StorageAccess 请实现
pub struct Archive<NF, const NT: usize>(pub [NF; NT]);

pub trait CacheArchive<K: Hash + Eq, V: Send + Sync + 'static>: IntoIterator<Item = (K, V)> {
	const BUFFER: usize;
	const MAXIMUM: i64;
	fn archive(self, e: Option<Duration>) -> Cache<K, V>;
	async fn archive_nexus(self, e: Option<Duration>) -> AsyncCache<K, V>;
}

impl<K: Hash + Eq, V: Send + Sync + 'static> CacheArchive<K, V> for Vec<(K, V)> {
	const BUFFER: usize = 2048;
	const MAXIMUM: i64 = 1048576;
	
	fn archive(self, e: Option<Duration>) -> Cache<K, V> {
		let opt = Cache::new(Self::BUFFER, Self::MAXIMUM).unwrap();
		match e {
			None => {
				self.into_iter().for_each(|(k, v)| {
					opt.insert(k, v, 0);
				});
			}
			Some(e) => {
				self.into_iter().for_each(|(k, v)| {
					opt.insert_with_ttl(k, v, 0, e);
				});
			}
		}
		opt
	}
	
	async fn archive_nexus(self, e: Option<Duration>) -> AsyncCache<K, V> {
		let opt = AsyncCache::new(Self::BUFFER, Self::MAXIMUM, tokio::spawn).unwrap();
		match e {
			None => {
				for (k, v) in self.into_iter() {
					opt.insert(k, v, 0).await;
				}
			}
			Some(e) => {
				for (k, v) in self.into_iter() {
					opt.insert_with_ttl(k, v, 0, e).await;
				}
			}
		}
		opt
	}
}

impl<const NT: usize, K: Hash + Eq, V: Send + Sync + 'static> CacheArchive<K, V> for Archive<(K, V), NT> {
	const BUFFER: usize = 1024;
	const MAXIMUM: i64 = 1048576;
	fn archive(self, e: Option<Duration>) -> Cache<K, V> {
		let x = Cache::new(Self::BUFFER, Self::MAXIMUM).unwrap();
		self.0.into_iter().for_each(|(k, v)| {
			match e {
				None => { x.insert(k, v, 0); }
				Some(e) => {
					x.insert_with_ttl(k, v, 0, e);
				}
			}
		});
		x.wait().unwrap();
		x
	}
	async fn archive_nexus(self, e: Option<Duration>) -> AsyncCache<K, V> {
		let x = AsyncCache::new(Self::BUFFER, Self::MAXIMUM, tokio::spawn).unwrap();
		for (k, v) in self.into_iter() {
			match e {
				None => { x.insert(k, v, 0).await; }
				Some(e) => {
					x.insert_with_ttl(k, v, 0, e).await;
				}
			}
		}
		x.wait().await.unwrap();
		x
	}
}


///# 反射特征
pub trait ArchiveConstruct: Any {
	fn into_const(self) -> Box<dyn ArchiveConstruct>;
}

impl<NF: Sized + 'static, const GN: usize> ArchiveConstruct for Archive<NF, GN> {
	fn into_const(self) -> Box<dyn ArchiveConstruct> {
		Box::new(self)
	}
}

impl<NF, const GN: usize> Archive<NF, GN> {
	///# 哈希模式
	#[inline(never)]
	pub fn has_access(self) -> Function<BuildDefaultSeededHasher>
		where
			NF: Hash + Send + Sync,
	{
		Function::from(self.0.into_iter().collect::<Vec<_>>())
	}
	///# 全部反射
	#[inline(always)]
	pub fn all_reflection<MD: Sized>(self, i: impl Fn(NF) -> MD) -> Vec<MD>
		where
			NF: Any + 'static,
	{
		self.into_iter().map(i).collect()
	}
	///# 普通反射
	#[inline(always)]
	pub fn reflection<MD: Sized>(self, i: impl Fn(NF) -> MD) -> Vec<MD>
		where
			NF: Reflect,
	{
		self.into_iter().map(i).collect()
	}
	///# 根反射
	#[inline(never)]
	pub fn dynamic_reflection<MD: Sized>(self, i: impl Fn(NF) -> MD) -> Vec<MD>
		where
			NF: FromReflect,
	{
		self.into_iter().map(i).collect()
	}
	///# 迭代
	pub fn into_init(self, i: impl Fn(NF) + Sync + Send)
		where
			NF: Send,
			Self: Into<[NF; GN]>,
	{
		self.into().into_par_iter().for_each(i);
	}
	///# 引用迭代
	pub fn iter_init(&self, i: impl Fn(&NF) + Sync + Send)
		where
			NF: Send + Sync,
			Self: AsRef<[NF; GN]>,
	{
		self.as_ref().par_iter().for_each(i);
	}
	///# 可变引用迭代
	pub fn mut_init(&mut self, i: impl Fn(&NF) + Sync + Send)
		where
			NF: Send + Sync,
			Self: AsMut<[NF; GN]>,
	{
		self.as_mut().par_iter_mut().for_each(|x| i(x));
	}
	///# 合并
	pub fn all_vector<const ER: usize>(self, elt: Self) -> Vec<NF>
		where
			NF: Sized,
			Self: Sized,
	{
		interleave(self.0, elt.0).collect::<Vec<NF>>()
	}
}

impl<RME: Sized, const GN: usize> IntoIterator for Archive<RME, GN> {
	type Item = RME;
	type IntoIter = std::array::IntoIter<RME, GN>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl<RME: Sized, const GN: usize> Index<usize> for Archive<RME, GN> {
	type Output = RME;
	fn index(&self, index: usize) -> &Self::Output {
		self.0.index(index)
	}
}

impl<RME: Sized, const GN: usize> IndexMut<usize> for Archive<RME, GN> {
	fn index_mut(&mut self, index: usize) -> &mut Self::Output {
		self.0.index_mut(index)
	}
}

impl<RME: Sized, const GN: usize> AsRef<[RME; GN]> for Archive<RME, GN> {
	fn as_ref(&self) -> &[RME; GN] {
		&self.0
	}
}

impl<RME: Sized, const GN: usize> AsMut<[RME; GN]> for Archive<RME, GN> {
	fn as_mut(&mut self) -> &mut [RME; GN] {
		&mut self.0
	}
}

///# 集合
pub static mut ARCED: Lazy<ArcSwap<Box<dyn ArchiveConstruct>>> = Lazy::new(|| { ArcSwap::from_pointee(archive!(0).into_const()) });

impl<NF, const NT: usize> From<[NF; NT]> for Archive<NF, NT> {
	fn from(value: [NF; NT]) -> Self {
		Archive(value)
	}
}
