use crate::alex::{Alex, Overmaster};
use crate::base::FutureEx;
use crate::function_frame::Execution;
use hashbrown::HashSet;
use std::hash::Hash;
use std::ops::AddAssign;
use std::vec;
use tokio::task::JoinHandle;
use crate::Events;

///# 聚合
pub struct Aggregation<NT: Sized + Send>(pub Vec<JoinHandle<Events<NT>>>);

impl<NT: Sized + Send + Sync> IntoIterator for Aggregation<NT> {
    type Item = JoinHandle<Events<NT>>;
    type IntoIter = vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
impl<NT: Sized + Send + Sync> AddAssign for Aggregation<NT> {
    fn add_assign(&mut self, rhs: Self) {
        let mut rhs = rhs.0;
        self.0.append(&mut rhs);
    }
}
impl<NT: Sized + Send + 'static, F: IntoIterator<Item = Execution<'static, Overmaster, NT>>> From<F>
    for Aggregation<NT>
where
    Execution<'static, Overmaster, NT>: Eq + Hash,
{
    fn from(value: F) -> Self {
        Alex {
            alex: Default::default(),
            execution: HashSet::from_iter(value),
        }
        .aggregation()
    }
}
impl<NT: Sized + Send + 'static> FromIterator<FutureEx<'static, Overmaster, Events<NT>>>
    for Aggregation<NT>
{
    fn from_iter<T: IntoIterator<Item = FutureEx<'static, Overmaster, Events<NT>>>>(
        iter: T,
    ) -> Self {
        Alex {
            alex: Default::default(),
            execution: HashSet::from_iter([Execution::from(iter.into_iter().collect::<Vec<_>>())]),
        }
        .aggregation()
    }
}
