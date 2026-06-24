use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::ops::{Index, IndexMut};
use std::vec;

use uuid::fmt::Urn;
use uuid::Uuid;

use crate::base::FutureEx;
use crate::Events;

///# 执行机
pub struct Execution<'life, DF: Send, NT: Send> {
	pub id: String,
	pub execution: Vec<FutureEx<'life, DF, Events<NT>>>,
}

impl<'life, DF: Send, NT: Send + Sized> From<Vec<FutureEx<'life, DF, Events<NT>>>>
for Execution<'life, DF, NT>
{
	fn from(value: Vec<FutureEx<'life, DF, Events<NT>>>) -> Self {
		Execution {
			id: Urn::from_uuid(Uuid::new_v4()).to_string(),
			execution: value,
		}
	}
}

impl<'life, DF: Send, NT: Send> Default for Execution<'life, DF, NT> {
	fn default() -> Self {
		Execution {
			id: String::default(),
			execution: Vec::default(),
		}
	}
}

impl<'life, DF: Send, NT: Send> IntoIterator for Execution<'life, DF, NT> {
	type Item = FutureEx<'life, DF, Events<NT>>;
	type IntoIter = vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.execution.into_iter()
	}
}

impl<'life, DF: Send, NT: Send> Index<usize> for Execution<'life, DF, NT> {
	type Output = FutureEx<'life, DF, Events<NT>>;
	fn index(&self, index: usize) -> &Self::Output {
		self.execution.index(index)
	}
}

impl<'life, DF: Send, NT: Send> IndexMut<usize> for Execution<'life, DF, NT> {
	fn index_mut(&mut self, index: usize) -> &mut Self::Output {
		self.execution.index_mut(index)
	}
}

impl<'life, DF: Send, NT: Send> Eq for Execution<'life, DF, NT> {}

impl<'life, DF: Send, NT: Send> PartialEq<Self> for Execution<'life, DF, NT> {
	fn eq(&self, other: &Self) -> bool {
		self.id.eq(&other.id)
	}
}

impl<'life, DF: Send, NT: Send> PartialOrd<Self> for Execution<'life, DF, NT> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.id.partial_cmp(&other.id)
	}
}

impl<'life, DF: Send, NT: Send> Ord for Execution<'life, DF, NT> {
	fn cmp(&self, other: &Self) -> Ordering {
		self.id.cmp(&other.id)
	}
}

impl<'life, DF: Send, NT: Send> Hash for Execution<'life, DF, NT> {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.id.hash(state)
	}
}
