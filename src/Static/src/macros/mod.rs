///# alpha
///# alpha!({});
///# alpha!({},{},);
#[macro_export]
macro_rules! alpha {
    ($e:block) => {
        Box::pin(async move { $e })
    };
    ($($e:expr),+ $(,)?) => {
        Box::pin(async move { $($e)+ });
    };
}
///# Beta
///# beta!(i,i32,{async{}});
///# beta!(i,i32,{async{}},{},{},);
#[macro_export]
macro_rules! beta {
    ($($a:ident,$b:ty),*,$i:block)=>{
        Box::new(move |$($a: $b),*| {
            Box::pin(async move { $i
            })
        })
    };
    ($($a:ident,$b:ty),*,$i:block,$($e:expr),+ $(,)?)=>{
        Box::new(move |$($a: $b),*| {
            $($e)+ Box::pin(async move { $i
            })
        })
    };
}
///# Gamma
///# gamma!(i,i32,{});
///# gamma!(i,i32,{},{});
#[macro_export]
macro_rules! gamma {
    ($($a:ident,$b:ty),*,$e:block) => {
        Box::new(move |$($a: $b),*| $e)
    };
    ($($a:ident,$b:ty),*,$($e:expr),+ $(,)?) => {
        Box::new(move |$($a: $b),*| {$($e)+})
    };
}
///# archive!()
#[macro_export]
macro_rules! archive {
	($($a:expr),+ $(,)?)=>{
		$crate::static_array::Archive([$($a),+])
    };
}
