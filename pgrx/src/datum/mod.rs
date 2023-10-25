//LICENSE Portions Copyright 2019-2021 ZomboDB, LLC.
//LICENSE
//LICENSE Portions Copyright 2021-2023 Technology Concepts & Design, Inc.
//LICENSE
//LICENSE Portions Copyright 2023-2023 PgCentral Foundation, Inc. <contact@pgcentral.org>
//LICENSE
//LICENSE All rights reserved.
//LICENSE
//LICENSE Use of this source code is governed by the MIT license that can be found in the LICENSE file.
//! Handing for easily converting Postgres Datum types into their corresponding Rust types
//! and converting Rust types into their corresponding Postgres types
mod anyarray;
mod anyelement;
mod array;
mod date;
pub mod datetime_support;
mod from;
mod geo;
mod inet;
mod internal;
mod interval;
mod into;
mod item_pointer_data;
mod json;
pub mod numeric;
pub mod numeric_support;
#[deny(unsafe_op_in_unsafe_fn)]
mod range;
mod time;
mod time_stamp;
mod time_stamp_with_timezone;
mod time_with_timezone;
mod tuples;
mod uuid;
mod varlena;

pub use self::time::*;
pub use self::uuid::*;
pub use anyarray::*;
pub use anyelement::*;
pub use array::*;
pub use date::*;
pub use datetime_support::*;
pub use from::*;
pub use geo::*;
pub use inet::*;
pub use internal::*;
pub use interval::*;
pub use into::*;
pub use item_pointer_data::*;
pub use json::*;
pub use numeric::{AnyNumeric, Numeric};
use once_cell::sync::Lazy;
pub use range::*;
use std::any::TypeId;
pub use time_stamp::*;
pub use time_stamp_with_timezone::*;
pub use time_with_timezone::*;
pub use tuples::*;
pub use varlena::*;

use crate::PgBox;
use pgrx_sql_entity_graph::RustSqlMapping;

/// A tagging trait to indicate a user type is also meant to be used by Postgres
/// Implemented automatically by `#[derive(PostgresType)]`
pub trait PostgresType {}

/// A type which can have it's [`core::any::TypeId`]s registered for Rust to SQL mapping.
///
/// An example use of this trait:
///
/// ```rust
/// use pgrx::prelude::*;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Copy, Serialize, Deserialize, PostgresType)]
/// struct Treat<'a> { best_part: &'a str, };
///
/// let mut mappings = Default::default();
/// let treat_string = stringify!(Treat).to_string();
/// <Treat<'static> as pgrx::datum::WithTypeIds>::register_with_refs(&mut mappings, treat_string.clone());
///
/// assert!(mappings.iter().any(|x| x.id == core::any::TypeId::of::<Treat<'static>>()));
/// ```
///
/// This trait uses the fact that inherent implementations are a higher priority than trait
/// implementations.
pub trait WithTypeIds {
    const ITEM_ID: Lazy<TypeId>;
    const OPTION_ID: Lazy<Option<TypeId>>;
    const VEC_ID: Lazy<Option<TypeId>>;
    const VEC_OPTION_ID: Lazy<Option<TypeId>>;
    const OPTION_VEC_ID: Lazy<Option<TypeId>>;
    const OPTION_VEC_OPTION_ID: Lazy<Option<TypeId>>;
    const ARRAY_ID: Lazy<Option<TypeId>>;
    const OPTION_ARRAY_ID: Lazy<Option<TypeId>>;
    const VARIADICARRAY_ID: Lazy<Option<TypeId>>;
    const OPTION_VARIADICARRAY_ID: Lazy<Option<TypeId>>;
    const VARLENA_ID: Lazy<Option<TypeId>>;
    const OPTION_VARLENA_ID: Lazy<Option<TypeId>>;

    fn register_with_refs(map: &mut std::collections::HashSet<RustSqlMapping>, single_sql: String)
    where
        Self: 'static,
    {
        Self::register(map, single_sql.clone());
        <&Self as WithTypeIds>::register(map, single_sql.clone());
        <&mut Self as WithTypeIds>::register(map, single_sql);
    }

    fn register_sized_with_refs(
        _map: &mut std::collections::HashSet<RustSqlMapping>,
        _single_sql: String,
    ) where
        Self: 'static,
    {
        ()
    }

    fn register_sized(_map: &mut std::collections::HashSet<RustSqlMapping>, _single_sql: String)
    where
        Self: 'static,
    {
        ()
    }

    fn register_varlena_with_refs(
        _map: &mut std::collections::HashSet<RustSqlMapping>,
        _single_sql: String,
    ) where
        Self: 'static,
    {
        ()
    }

    fn register_varlena(_map: &mut std::collections::HashSet<RustSqlMapping>, _single_sql: String)
    where
        Self: 'static,
    {
        ()
    }

    fn register_array_with_refs(
        _map: &mut std::collections::HashSet<RustSqlMapping>,
        _single_sql: String,
    ) where
        Self: 'static,
    {
        ()
    }

    fn register_array(_map: &mut std::collections::HashSet<RustSqlMapping>, _single_sql: String)
    where
        Self: 'static,
    {
        ()
    }

    fn register(set: &mut std::collections::HashSet<RustSqlMapping>, single_sql: String)
    where
        Self: 'static,
    {
        let rust = core::any::type_name::<Self>();
        assert_eq!(
            set.insert(RustSqlMapping {
                sql: single_sql.clone(),
                rust: rust.to_string(),
                id: *Self::ITEM_ID,
            }),
            true,
            "Cannot set mapping of `{}` twice, was already `{}`.",
            rust,
            single_sql,
        );
    }
}

impl<T: 'static + ?Sized> WithTypeIds for T {
    const ITEM_ID: Lazy<TypeId> = Lazy::new(|| TypeId::of::<T>());
    const OPTION_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
    const VEC_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
    const VEC_OPTION_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
    const OPTION_VEC_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
    const OPTION_VEC_OPTION_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
    const ARRAY_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
    const OPTION_ARRAY_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
    const VARIADICARRAY_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
    const OPTION_VARIADICARRAY_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
    const VARLENA_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
    const OPTION_VARLENA_ID: Lazy<Option<TypeId>> = Lazy::new(|| None);
}

/// A type which can have its [`core::any::TypeId`]s registered for Rust to SQL mapping.
///
/// An example use of this trait:
///
/// ```rust
/// use pgrx::prelude::*;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Copy, Serialize, Deserialize, PostgresType)]
/// pub struct Treat<'a> { best_part: &'a str, };
///
/// let mut mappings = Default::default();
/// let treat_string = stringify!(Treat).to_string();
///
/// pgrx::datum::WithSizedTypeIds::<Treat<'static>>::register_sized_with_refs(
///     &mut mappings,
///     treat_string.clone()
/// );
///
/// assert!(mappings.iter().any(|x| x.id == core::any::TypeId::of::<Option<Treat<'static>>>()));
/// ```
///
/// This trait uses the fact that inherent implementations are a higher priority than trait
/// implementations.
pub struct WithSizedTypeIds<T>(pub core::marker::PhantomData<T>);

impl<T: 'static> WithSizedTypeIds<T> {
    pub const PG_BOX_ID: Lazy<Option<TypeId>> = Lazy::new(|| Some(TypeId::of::<PgBox<T>>()));
    pub const PG_BOX_OPTION_ID: Lazy<Option<TypeId>> =
        Lazy::new(|| Some(TypeId::of::<PgBox<Option<T>>>()));
    pub const PG_BOX_VEC_ID: Lazy<Option<TypeId>> =
        Lazy::new(|| Some(TypeId::of::<PgBox<Vec<T>>>()));
    pub const OPTION_ID: Lazy<Option<TypeId>> = Lazy::new(|| Some(TypeId::of::<Option<T>>()));
    pub const VEC_ID: Lazy<Option<TypeId>> = Lazy::new(|| Some(TypeId::of::<Vec<T>>()));
    pub const VEC_OPTION_ID: Lazy<Option<TypeId>> =
        Lazy::new(|| Some(TypeId::of::<Vec<Option<T>>>()));
    pub const OPTION_VEC_ID: Lazy<Option<TypeId>> =
        Lazy::new(|| Some(TypeId::of::<Option<Vec<T>>>()));
    pub const OPTION_VEC_OPTION_ID: Lazy<Option<TypeId>> =
        Lazy::new(|| Some(TypeId::of::<Option<Vec<Option<T>>>>()));

    pub fn register_sized_with_refs(
        map: &mut std::collections::HashSet<RustSqlMapping>,
        single_sql: String,
    ) where
        Self: 'static,
    {
        WithSizedTypeIds::<T>::register_sized(map, single_sql.clone());
        WithSizedTypeIds::<&T>::register_sized(map, single_sql.clone());
        WithSizedTypeIds::<&mut T>::register_sized(map, single_sql);
    }

    pub fn register_sized(map: &mut std::collections::HashSet<RustSqlMapping>, single_sql: String) {
        let set_sql = format!("{}[]", single_sql);

        if let Some(id) = *WithSizedTypeIds::<T>::PG_BOX_ID {
            let rust = core::any::type_name::<crate::PgBox<T>>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping {
                    sql: single_sql.clone(),
                    rust: rust.to_string(),
                    id: id,
                }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }

        if let Some(id) = *WithSizedTypeIds::<T>::PG_BOX_OPTION_ID {
            let rust = core::any::type_name::<crate::PgBox<Option<T>>>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping {
                    sql: single_sql.clone(),
                    rust: rust.to_string(),
                    id: id,
                }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }

        if let Some(id) = *WithSizedTypeIds::<T>::PG_BOX_VEC_ID {
            let rust = core::any::type_name::<crate::PgBox<Vec<T>>>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping { sql: set_sql.clone(), rust: rust.to_string(), id: id }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }

        if let Some(id) = *WithSizedTypeIds::<T>::OPTION_ID {
            let rust = core::any::type_name::<Option<T>>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping {
                    sql: single_sql.clone(),
                    rust: rust.to_string(),
                    id: id,
                }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }

        if let Some(id) = *WithSizedTypeIds::<T>::VEC_ID {
            let rust = core::any::type_name::<T>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping { sql: set_sql.clone(), rust: rust.to_string(), id: id }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }
        if let Some(id) = *WithSizedTypeIds::<T>::VEC_OPTION_ID {
            let rust = core::any::type_name::<Vec<Option<T>>>();
            assert_eq!(
                map.insert(RustSqlMapping { sql: set_sql.clone(), rust: rust.to_string(), id: id }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }
        if let Some(id) = *WithSizedTypeIds::<T>::OPTION_VEC_ID {
            let rust = core::any::type_name::<Option<Vec<T>>>();
            assert_eq!(
                map.insert(RustSqlMapping { sql: set_sql.clone(), rust: rust.to_string(), id: id }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }
        if let Some(id) = *WithSizedTypeIds::<T>::OPTION_VEC_OPTION_ID {
            let rust = core::any::type_name::<Option<Vec<Option<T>>>>();
            assert_eq!(
                map.insert(RustSqlMapping { sql: set_sql.clone(), rust: rust.to_string(), id: id }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }
    }
}

/// An [`Array`] compatible type which can have its [`core::any::TypeId`]s registered for Rust to SQL mapping.
///
/// An example use of this trait:
///
/// ```rust
/// use pgrx::prelude::*;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize, PostgresType)]
/// pub struct Treat { best_part: String, };
///
/// let mut mappings = Default::default();
/// let treat_string = stringify!(Treat).to_string();
///
/// pgrx::datum::WithArrayTypeIds::<Treat>::register_array_with_refs(
///     &mut mappings,
///     treat_string.clone()
/// );
///
/// assert!(mappings.iter().any(|x| x.id == core::any::TypeId::of::<Array<Treat>>()));
/// ```
///
/// This trait uses the fact that inherent implementations are a higher priority than trait
/// implementations.
pub struct WithArrayTypeIds<T>(pub core::marker::PhantomData<T>);

impl<T: FromDatum + 'static> WithArrayTypeIds<T> {
    pub const ARRAY_ID: Lazy<Option<TypeId>> = Lazy::new(|| Some(TypeId::of::<Array<T>>()));
    pub const OPTION_ARRAY_ID: Lazy<Option<TypeId>> =
        Lazy::new(|| Some(TypeId::of::<Option<Array<T>>>()));
    pub const VARIADICARRAY_ID: Lazy<Option<TypeId>> =
        Lazy::new(|| Some(TypeId::of::<VariadicArray<T>>()));
    pub const OPTION_VARIADICARRAY_ID: Lazy<Option<TypeId>> =
        Lazy::new(|| Some(TypeId::of::<Option<VariadicArray<T>>>()));

    pub fn register_array_with_refs(
        map: &mut std::collections::HashSet<RustSqlMapping>,
        single_sql: String,
    ) where
        Self: 'static,
    {
        WithArrayTypeIds::<T>::register_array(map, single_sql.clone());
        WithArrayTypeIds::<&T>::register_array(map, single_sql.clone());
        WithArrayTypeIds::<&mut T>::register_array(map, single_sql);
    }

    pub fn register_array(map: &mut std::collections::HashSet<RustSqlMapping>, single_sql: String) {
        let set_sql = format!("{}[]", single_sql);

        if let Some(id) = *WithArrayTypeIds::<T>::ARRAY_ID {
            let rust = core::any::type_name::<Array<T>>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping { sql: set_sql.clone(), rust: rust.to_string(), id: id }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }
        if let Some(id) = *WithArrayTypeIds::<T>::OPTION_ARRAY_ID {
            let rust = core::any::type_name::<Option<Array<T>>>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping { sql: set_sql.clone(), rust: rust.to_string(), id: id }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }

        if let Some(id) = *WithArrayTypeIds::<T>::VARIADICARRAY_ID {
            let rust = core::any::type_name::<VariadicArray<T>>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping { sql: set_sql.clone(), rust: rust.to_string(), id: id }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }
        if let Some(id) = *WithArrayTypeIds::<T>::OPTION_VARIADICARRAY_ID {
            let rust = core::any::type_name::<Option<VariadicArray<T>>>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping { sql: set_sql.clone(), rust: rust.to_string(), id: id }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }
    }
}

/// A [`PgVarlena`] compatible type which can have its [`core::any::TypeId`]s registered for Rust to SQL mapping.
///
/// An example use of this trait:
///
/// ```rust
/// use pgrx::prelude::*;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Copy, Serialize, Deserialize, PostgresType)]
/// pub struct Treat<'a> { best_part: &'a str, };
///
/// let mut mappings = Default::default();
/// let treat_string = stringify!(Treat).to_string();
///
/// pgrx::datum::WithVarlenaTypeIds::<Treat<'static>>::register_varlena_with_refs(
///     &mut mappings,
///     treat_string.clone()
/// );
///
/// assert!(mappings.iter().any(|x| x.id == core::any::TypeId::of::<PgVarlena<Treat<'static>>>()));
/// ```
///
/// This trait uses the fact that inherent implementations are a higher priority than trait
/// implementations.
pub struct WithVarlenaTypeIds<T>(pub core::marker::PhantomData<T>);

impl<T: Copy + 'static> WithVarlenaTypeIds<T> {
    pub const VARLENA_ID: Lazy<Option<TypeId>> = Lazy::new(|| Some(TypeId::of::<PgVarlena<T>>()));
    pub const PG_BOX_VARLENA_ID: Lazy<Option<TypeId>> =
        Lazy::new(|| Some(TypeId::of::<PgBox<PgVarlena<T>>>()));
    pub const OPTION_VARLENA_ID: Lazy<Option<TypeId>> =
        Lazy::new(|| Some(TypeId::of::<Option<PgVarlena<T>>>()));

    pub fn register_varlena_with_refs(
        map: &mut std::collections::HashSet<RustSqlMapping>,
        single_sql: String,
    ) where
        Self: 'static,
    {
        WithVarlenaTypeIds::<T>::register_varlena(map, single_sql.clone());
        WithVarlenaTypeIds::<&T>::register_varlena(map, single_sql.clone());
        WithVarlenaTypeIds::<&mut T>::register_varlena(map, single_sql);
    }

    pub fn register_varlena(
        map: &mut std::collections::HashSet<RustSqlMapping>,
        single_sql: String,
    ) {
        if let Some(id) = *WithVarlenaTypeIds::<T>::VARLENA_ID {
            let rust = core::any::type_name::<PgVarlena<T>>();
            assert_eq!(
                map.insert(RustSqlMapping {
                    sql: single_sql.clone(),
                    rust: rust.to_string(),
                    id: id,
                }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }

        if let Some(id) = *WithVarlenaTypeIds::<T>::PG_BOX_VARLENA_ID {
            let rust = core::any::type_name::<PgBox<PgVarlena<T>>>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping {
                    sql: single_sql.clone(),
                    rust: rust.to_string(),
                    id: id,
                }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }
        if let Some(id) = *WithVarlenaTypeIds::<T>::OPTION_VARLENA_ID {
            let rust = core::any::type_name::<Option<PgVarlena<T>>>().to_string();
            assert_eq!(
                map.insert(RustSqlMapping {
                    sql: single_sql.clone(),
                    rust: rust.to_string(),
                    id: id,
                }),
                true,
                "Cannot map `{}` twice.",
                rust,
            );
        }
    }
}
use core::ffi::c_void;
use core::marker::PhantomData;
use core::num::TryFromIntError;
use core::ptr;
use core::str::{self, Utf8Error};

pub unsafe trait BorrowDatum {
    type As<'dat>: Sized
    where
        Self: 'dat;
    unsafe fn borrow<'dat>(datum: Datum<'dat>) -> Self::As<'dat>;
}

#[repr(transparent)]
pub struct Datum<'dat>(pgrx_pg_sys::Datum, PhantomData<&'dat c_void>);

impl<'dat> Datum<'dat> {
    pub(crate) unsafe fn promote(datum: pgrx_pg_sys::Datum) -> Datum<'dat> {
        Datum(datum, PhantomData)
    }
    pub unsafe fn borrow_as<T: BorrowDatum>(self) -> T::As<'dat> {
        unsafe { T::borrow(self) }
    }
}

unsafe impl BorrowDatum for str {
    type As<'dat> = &'dat str;
    unsafe fn borrow<'dat>(d: Datum<'dat>) -> Self::As<'dat> {
        let varlena_ptr = d.0.cast_mut_ptr::<u8>();
        unsafe {
            let varlena_len = *varlena_ptr;
            let byte_slice =
                ptr::slice_from_raw_parts(varlena_ptr, varlena_len.saturating_sub(1) as usize);
            str::from_utf8_unchecked(&*byte_slice)
        }
    }
}

unsafe impl BorrowDatum for i64 {
    type As<'dat> = i64;
    unsafe fn borrow<'dat>(d: Datum<'dat>) -> Self::As<'dat> {
        let data = d.0.value();
        data as i64
    }
}

unsafe impl BorrowDatum for i8 {
    type As<'dat> = i8;
    unsafe fn borrow<'dat>(d: Datum<'dat>) -> Self::As<'dat> {
        let data = d.0.value();
        data as i8
    }
}

unsafe impl BorrowDatum for i16 {
    type As<'dat> = i16;
    unsafe fn borrow<'dat>(d: Datum<'dat>) -> Self::As<'dat> {
        let data = d.0.value();
        data as i16
    }
}

unsafe impl BorrowDatum for i32 {
    type As<'dat> = i32;
    unsafe fn borrow<'dat>(d: Datum<'dat>) -> Self::As<'dat> {
        let data = d.0.value();
        data as i32
    }
}

unsafe impl BorrowDatum for f32 {
    type As<'dat> = f32;
    unsafe fn borrow<'dat>(d: Datum<'dat>) -> Self::As<'dat> {
        let data = d.0.value();
        f32::from_bits(data as u32)
    }
}

unsafe impl BorrowDatum for f64 {
    type As<'dat> = f64;
    unsafe fn borrow<'dat>(d: Datum<'dat>) -> Self::As<'dat> {
        let data = d.0.value();
        f64::from_bits(data as u64)
    }
}

unsafe impl BorrowDatum for String {
    type As<'dat> = String;
    unsafe fn borrow<'dat>(d: Datum<'dat>) -> Self::As<'dat> {
        let string = <str as BorrowDatum>::borrow(d);
        string.to_owned()
    }
}

pub trait TryDatum {
    type Error;
}

pub enum VarlenaStrErr {
    /// actually a null pointer
    NullPtr,
    /// checked for UTF-8 and found wanting
    NotUtf8,
    /// the varlena was not UTF-8
    TooSmall,
}

pub enum DatumKind<T: TryDatum> {
    Is(T),
    WrongOid,
    Null,
    TypeErr(<T as TryDatum>::Error),
}


struct ArenaLt<'arena>(PhantomData<&'arena Arena<'arena>>);

struct Memory {
    data: Vec<u8>,
}

struct Arena<'arena>(PhantomData<&'arena Memory>);
