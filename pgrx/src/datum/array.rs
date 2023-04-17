/*
Portions Copyright 2019-2021 ZomboDB, LLC.
Portions Copyright 2021-2022 Technology Concepts & Design, Inc. <support@tcdi.com>

All rights reserved.

Use of this source code is governed by the MIT license that can be found in the LICENSE file.
*/

use crate::array::RawArray;
use crate::layout::*;
use crate::slice::PallocSlice;
use crate::toast::Toast;
use crate::varlena;
use crate::{pg_sys, FromDatum, IntoDatum, PgMemoryContexts};
use bitvec::slice::BitSlice;
use core::ops::DerefMut;
use core::ptr::NonNull;
use pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};
use serde::Serializer;
use std::marker::PhantomData;
use std::mem;

/** An array of some type (eg. `TEXT[]`, `int[]`)

While conceptually similar to a [`Vec<T>`][std::vec::Vec], arrays are lazy.

Using a [`Vec<T>`][std::vec::Vec] here means each element of the passed array will be eagerly fetched and converted into a Rust type:

```rust,no_run
use pgrx::prelude::*;

#[pg_extern]
fn with_vec(elems: Vec<String>) {
    // Elements all already converted.
    for elem in elems {
        todo!()
    }
}
```

Using an array, elements are only fetched and converted into a Rust type on demand:

```rust,no_run
use pgrx::prelude::*;

#[pg_extern]
fn with_vec(elems: Array<String>) {
    // Elements converted one by one
    for maybe_elem in elems {
        let elem = maybe_elem.unwrap();
        todo!()
    }
}
```
*/
pub struct Array<'a, T: FromDatum> {
    nelems: usize,
    // Remove this field if/when we figure out how to stop using pg_sys::deconstruct_array
    elem_slice: ElemSlice<T>,
    null_slice: NullKind<'a>,
    elem_layout: Layout,
    // Rust drops in FIFO order, drop this last
    raw: Toast<RawArray>,
    _marker: PhantomData<T>,
}

// FIXME: When Array::over gets removed, this enum can probably be dropped
// since we won't be entertaining ArrayTypes which don't use bitslices anymore.
// However, we could also use a static resolution? Hard to say what's best.
enum NullKind<'a> {
    Bits(&'a BitSlice<u8>),
    Strict(usize),
}

impl NullKind<'_> {
    fn get(&self, index: usize) -> Option<bool> {
        match self {
            Self::Bits(b1) => b1.get(index).map(|b| !b),
            Self::Strict(len) => index.le(len).then(|| false),
        }
    }

    fn any(&self) -> bool {
        match self {
            Self::Bits(b1) => !b1.all(),
            Self::Strict(_) => false,
        }
    }

    // counts nulls until the i-th bit, non-inclusive
    fn count_nulls_until(&self, i: usize) -> usize {
        match self {
            Self::Bits(b1) => BitSlice::split_at(b1, core::cmp::min(i, b1.len())).0.count_zeros(),
            Self::Strict(_) => 0,
        }
    }
}

enum ElemSlice<T: FromDatum> {
    Datum(PallocSlice<pg_sys::Datum>),
    Bare(NonNull<[mem::MaybeUninit<T>]>),
}

impl<T: FromDatum> ElemSlice<T> {
    unsafe fn get(&self, idx: usize, is_null: bool, oid: pg_sys::Oid) -> Option<T> {
        match self {
            ElemSlice::Bare(slice) => (!is_null).then(|| slice.as_ref()[idx].assume_init_read()),
            ElemSlice::Datum(slice) => T::from_polymorphic_datum(*(slice.get(idx)?), is_null, oid),
        }
    }
}

impl<'a, T: FromDatum + serde::Serialize> serde::Serialize for Array<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

#[deny(unsafe_op_in_unsafe_fn)]
impl<'a, T: FromDatum> Array<'a, T> {
    /// # Safety
    ///
    /// This function requires that the RawArray was obtained in a properly-constructed form
    /// (probably from Postgres).
    unsafe fn deconstruct_from(mut raw: Toast<RawArray>) -> Array<'a, T> {
        let oid = raw.oid();
        let elem_layout = Layout::lookup_oid(oid);
        let nelems = raw.len();
        let null_slice = raw
            .nulls_bitslice()
            .map(|nonnull| NullKind::Bits(unsafe { &*nonnull.as_ptr() }))
            .unwrap_or(NullKind::Strict(nelems));
        if let (NullKind::Strict(_), Layout { size: Size::Fixed(_), pass: PassBy::Value, .. }) =
            (&null_slice, elem_layout)
        {
            let elem_slice = ElemSlice::Bare(raw.data());
            Array { raw, nelems, elem_slice, null_slice, elem_layout, _marker: PhantomData }
        } else {
            /*
            FIXME(jubilee): This way of getting array buffers causes problems for any Drop impl,
            and clashes with assumptions of Array being a "zero-copy", lifetime-bound array.
            It also risks leaking memory, as deconstruct_array calls palloc.

            SAFETY: We have already asserted the validity of the RawArray, so
            this only makes mistakes if we mix things up and pass Postgres the wrong data.
            */
            let (elements, nulls) = unsafe { raw.deconstruct(elem_layout) };
            // The array was just deconstructed, which allocates twice: effectively [Datum] and [bool].
            // But pgx doesn't actually need [bool] if NullKind's handling of BitSlices is correct.
            // So, assert correctness of the NullKind implementation and cleanup.
            // SAFETY: The pointer we got should be correctly constructed for slice validity.
            let pallocd_null_slice =
                unsafe { PallocSlice::from_raw_parts(NonNull::new(nulls).unwrap(), nelems) };
            #[cfg(debug_assertions)]
            for i in 0..nelems {
                assert!(null_slice
                    .get(i)
                    .unwrap()
                    .eq(unsafe { pallocd_null_slice.get_unchecked(i) }));
            }

            // SAFETY: This was just handed over as a palloc, so of course we can do this.
            let elem_slice = ElemSlice::Datum(unsafe {
                PallocSlice::from_raw_parts(NonNull::new(elements).unwrap(), nelems)
            });
            Array { raw, nelems, elem_slice, null_slice, elem_layout, _marker: PhantomData }
        }
    }

    /// Rips out the underlying pg_sys::ArrayType pointer.
    /// Note that Array may have caused Postgres to allocate to unbox the datum,
    /// and this can hypothetically cause a memory leak if so.
    pub fn into_array_type(self) -> *const pg_sys::ArrayType {
        // may be worth replacing this function when Toast<T> matures enough
        // to be used as a public type with a fn(self) -> Toast<RawArray>

        let Array { raw, elem_slice, .. } = self;
        let _ = elem_slice;
        // Wrap the Toast<RawArray> to prevent it from deallocating itself
        let mut raw = core::mem::ManuallyDrop::new(raw);
        let ptr = raw.deref_mut().deref_mut() as *mut RawArray;
        // SAFETY: Leaks are safe if they aren't use-after-frees!
        unsafe { ptr.read() }.into_ptr().as_ptr() as _
    }

    // # Panics
    //
    // Panics if it detects the slightest misalignment between types,
    // or if a valid slice contains nulls, which may be uninit data.
    #[deprecated(
        since = "0.5.0",
        note = "this function cannot be safe and is not generically sound\n\
        even `unsafe fn as_slice(&self) -> &[T]` is not sound for all `&[T]`\n\
        if you are sure your usage is sound, consider RawArray"
    )]
    pub unsafe fn as_slice(&self) -> &[T] {
        const DATUM_SIZE: usize = mem::size_of::<pg_sys::Datum>();
        if self.null_slice.any() {
            panic!("null detected: can't expose potentially uninit data as a slice!")
        }
        match self.elem_layout.size_matches::<T>() {
            // SAFETY: Rust slice layout matches Postgres data layout and this array is "owned"
            #[allow(unreachable_patterns)] // happens on 32-bit when DATUM_SIZE = 4
            Some(1 | 2 | 4 | DATUM_SIZE) => unsafe { self.raw.assume_init_data_slice::<T>() },
            _ => panic!("no correctly-sized slice exists"),
        }
    }

    /// Return an Iterator of Option<T> over the contained Datums.
    pub fn iter(&self) -> ArrayIterator<'_, T> {
        ArrayIterator { array: self, curr: 0 }
    }

    /// Return an Iterator of the contained Datums (converted to Rust types).
    ///
    /// This function will panic when called if the array contains any SQL NULL values.
    pub fn iter_deny_null(&self) -> ArrayTypedIterator<'_, T> {
        if unsafe { self.raw.any_nulls() } {
            panic!("array contains NULL");
        }

        ArrayTypedIterator { array: self, curr: 0 }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.nelems
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nelems == 0
    }

    #[allow(clippy::option_option)]
    #[inline]
    pub fn get(&self, i: usize) -> Option<Option<T>> {
        if i >= self.nelems {
            None
        } else {
            Some(unsafe { self.walk_index(i) })
        }
    }

    /// Walk to an index in the array from a byte offset
    /// This function exists to help with memoizing the array's offsets
    ///
    /// # Safety
    /// Implies reading
    // #[inline]
    // unsafe fn f(&self, i: usize, bytes: usize) -> *mut u8 {

    // Here, we walk to a particular index, but start from a particular starting offset.
    // todo!()
    // }

    /// # Safety
    /// Implies reading
    #[inline]
    unsafe fn walk_index(&self, idx: usize) -> Option<T> {
        let zero = self.raw.byte_ptr();
        let at_byte = zero; // when memoize: unsafe { zero.add(bytes) };
        let is_null = self.null_slice.get(idx)?;
        if is_null {
            return None;
        }
        let nulls = self.null_slice.count_nulls_until(idx); // nulls < idx
        let idx = idx - nulls;
        let walk_strat = self.elem_layout.size;
        match walk_strat {
            Size::Fixed(n) => {
                let full_offset = n as usize * idx;
                let at_byte = unsafe { at_byte.add(full_offset) };
                match self.elem_layout.pass {
                    PassBy::Value => Some(unsafe { at_byte.cast::<T>().read() }),
                    PassBy::Ref => {
                        let datum = pg_sys::Datum::from(at_byte);
                        unsafe { T::from_polymorphic_datum(datum, is_null, self.raw.oid()) }
                    }
                }
            }
            Size::Varlena => {
                // In this branch, we have to be mindful of alignment.
                let mut at_byte = at_byte;
                let align = self.elem_layout.align.as_usize();
                unsafe {
                    for _ in 0..idx {
                        let varsize = varlena::varsize_any(at_byte.cast());
                        let align_mask = varsize & (align - 1);
                        let align_offset = if align_mask != 0 { align - align_mask } else { 0 };
                        at_byte = at_byte.add(varsize + align_offset);
                    }
                    let datum = pg_sys::Datum::from(at_byte);
                    T::from_polymorphic_datum(datum, is_null, self.raw.oid())
                }
            }
            Size::CStr => {
                let mut at_char = at_byte.cast();
                unsafe {
                    for _ in 0..idx {
                        let strlen = core::ffi::CStr::from_ptr(at_char).to_bytes().len();
                        at_char = at_char.add(strlen + 2);
                    }
                    let datum = pg_sys::Datum::from(at_char);
                    T::from_polymorphic_datum(datum, is_null, self.raw.oid())
                }
            }
        }
    }
}

pub struct VariadicArray<'a, T: FromDatum>(Array<'a, T>);

impl<'a, T: FromDatum + serde::Serialize> serde::Serialize for VariadicArray<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.0.iter())
    }
}

impl<'a, T: FromDatum> VariadicArray<'a, T> {
    pub fn into_array_type(self) -> *const pg_sys::ArrayType {
        self.0.into_array_type()
    }

    // # Panics
    //
    // Panics if it detects the slightest misalignment between types,
    // or if a valid slice contains nulls, which may be uninit data.
    #[deprecated(
        since = "0.5.0",
        note = "this function cannot be safe and is not generically sound\n\
        even `unsafe fn as_slice(&self) -> &[T]` is not sound for all `&[T]`\n\
        if you are sure your usage is sound, consider RawArray"
    )]
    #[allow(deprecated)]
    pub unsafe fn as_slice(&self) -> &[T] {
        self.0.as_slice()
    }

    /// Return an Iterator of Option<T> over the contained Datums.
    pub fn iter(&self) -> ArrayIterator<'_, T> {
        self.0.iter()
    }

    /// Return an Iterator of the contained Datums (converted to Rust types).
    ///
    /// This function will panic when called if the array contains any SQL NULL values.
    pub fn iter_deny_null(&self) -> ArrayTypedIterator<'_, T> {
        self.0.iter_deny_null()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[allow(clippy::option_option)]
    #[inline]
    pub fn get(&self, i: usize) -> Option<Option<T>> {
        self.0.get(i)
    }
}

pub struct ArrayTypedIterator<'a, T: 'a + FromDatum> {
    array: &'a Array<'a, T>,
    curr: usize,
}

impl<'a, T: FromDatum> Iterator for ArrayTypedIterator<'a, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.curr >= self.array.nelems {
            None
        } else {
            let element = self
                .array
                .get(self.curr)
                .expect("array index out of bounds")
                .expect("array element was unexpectedly NULL during iteration");
            self.curr += 1;
            Some(element)
        }
    }
}

impl<'a, T: FromDatum + serde::Serialize> serde::Serialize for ArrayTypedIterator<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.array.iter())
    }
}

pub struct ArrayIterator<'a, T: 'a + FromDatum> {
    array: &'a Array<'a, T>,
    curr: usize,
}

impl<'a, T: FromDatum> Iterator for ArrayIterator<'a, T> {
    type Item = Option<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.curr >= self.array.nelems {
            None
        } else {
            let element = self.array.get(self.curr).unwrap();
            self.curr += 1;
            Some(element)
        }
    }
}

pub struct ArrayIntoIterator<'a, T: FromDatum> {
    array: Array<'a, T>,
    curr: usize,
}

impl<'a, T: FromDatum> IntoIterator for Array<'a, T> {
    type Item = Option<T>;
    type IntoIter = ArrayIntoIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        ArrayIntoIterator { array: self, curr: 0 }
    }
}

impl<'a, T: FromDatum> IntoIterator for VariadicArray<'a, T> {
    type Item = Option<T>;
    type IntoIter = ArrayIntoIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        ArrayIntoIterator { array: self.0, curr: 0 }
    }
}

impl<'a, T: FromDatum> Iterator for ArrayIntoIterator<'a, T> {
    type Item = Option<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.curr >= self.array.nelems {
            None
        } else {
            let element = self.array.get(self.curr).unwrap();
            self.curr += 1;
            Some(element)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.array.nelems))
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.array.nelems
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.array.get(n)
    }
}

impl<'a, T: FromDatum> FromDatum for VariadicArray<'a, T> {
    #[inline]
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        oid: pg_sys::Oid,
    ) -> Option<VariadicArray<'a, T>> {
        Array::from_polymorphic_datum(datum, is_null, oid).map(Self)
    }
}

impl<'a, T: FromDatum> FromDatum for Array<'a, T> {
    #[inline]
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        _typoid: pg_sys::Oid,
    ) -> Option<Array<'a, T>> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr())?;
            let raw = RawArray::detoast_from_varlena(ptr);
            Some(Array::deconstruct_from(raw))
        }
    }
}

impl<T: FromDatum> FromDatum for Vec<T> {
    #[inline]
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        typoid: pg_sys::Oid,
    ) -> Option<Vec<T>> {
        if is_null {
            None
        } else {
            let array = Array::<T>::from_polymorphic_datum(datum, is_null, typoid).unwrap();
            let mut v = Vec::with_capacity(array.len());

            for element in array.iter() {
                v.push(element.expect("array element was NULL"))
            }
            Some(v)
        }
    }
}

impl<T: FromDatum> FromDatum for Vec<Option<T>> {
    #[inline]
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        typoid: pg_sys::Oid,
    ) -> Option<Vec<Option<T>>> {
        if is_null || datum.is_null() {
            None
        } else {
            let array = Array::<T>::from_polymorphic_datum(datum, is_null, typoid).unwrap();
            Some(array.iter().collect::<Vec<_>>())
        }
    }
}

impl<T> IntoDatum for Vec<T>
where
    T: IntoDatum,
{
    fn into_datum(self) -> Option<pg_sys::Datum> {
        let mut state = unsafe {
            pg_sys::initArrayResult(
                T::type_oid(),
                PgMemoryContexts::CurrentMemoryContext.value(),
                false,
            )
        };
        for s in self {
            let datum = s.into_datum();
            let isnull = datum.is_none();

            unsafe {
                state = pg_sys::accumArrayResult(
                    state,
                    datum.unwrap_or(0.into()),
                    isnull,
                    T::type_oid(),
                    PgMemoryContexts::CurrentMemoryContext.value(),
                );
            }
        }

        if state.is_null() {
            // shouldn't happen
            None
        } else {
            Some(unsafe {
                pg_sys::makeArrayResult(state, PgMemoryContexts::CurrentMemoryContext.value())
            })
        }
    }

    fn type_oid() -> pg_sys::Oid {
        unsafe { pg_sys::get_array_type(T::type_oid()) }
    }

    #[inline]
    fn is_compatible_with(other: pg_sys::Oid) -> bool {
        Self::type_oid() == other || other == unsafe { pg_sys::get_array_type(T::type_oid()) }
    }
}

impl<'a, T> IntoDatum for &'a [T]
where
    T: IntoDatum + Copy + 'a,
{
    fn into_datum(self) -> Option<pg_sys::Datum> {
        let mut state = unsafe {
            pg_sys::initArrayResult(
                T::type_oid(),
                PgMemoryContexts::CurrentMemoryContext.value(),
                false,
            )
        };
        for s in self {
            let datum = s.into_datum();
            let isnull = datum.is_none();

            unsafe {
                state = pg_sys::accumArrayResult(
                    state,
                    datum.unwrap_or(0.into()),
                    isnull,
                    T::type_oid(),
                    PgMemoryContexts::CurrentMemoryContext.value(),
                );
            }
        }

        if state.is_null() {
            // shouldn't happen
            None
        } else {
            Some(unsafe {
                pg_sys::makeArrayResult(state, PgMemoryContexts::CurrentMemoryContext.value())
            })
        }
    }

    fn type_oid() -> pg_sys::Oid {
        unsafe { pg_sys::get_array_type(T::type_oid()) }
    }

    #[inline]
    fn is_compatible_with(other: pg_sys::Oid) -> bool {
        Self::type_oid() == other || other == unsafe { pg_sys::get_array_type(T::type_oid()) }
    }
}

unsafe impl<'a, T> SqlTranslatable for Array<'a, T>
where
    T: SqlTranslatable + FromDatum,
{
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        match T::argument_sql()? {
            SqlMapping::As(sql) => Ok(SqlMapping::As(format!("{sql}[]"))),
            SqlMapping::Skip => Err(ArgumentError::SkipInArray),
            SqlMapping::Composite { .. } => Ok(SqlMapping::Composite { array_brackets: true }),
            SqlMapping::Source { .. } => Ok(SqlMapping::Source { array_brackets: true }),
        }
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        match T::return_sql()? {
            Returns::One(SqlMapping::As(sql)) => {
                Ok(Returns::One(SqlMapping::As(format!("{sql}[]"))))
            }
            Returns::One(SqlMapping::Composite { array_brackets: _ }) => {
                Ok(Returns::One(SqlMapping::Composite { array_brackets: true }))
            }
            Returns::One(SqlMapping::Source { array_brackets: _ }) => {
                Ok(Returns::One(SqlMapping::Source { array_brackets: true }))
            }
            Returns::One(SqlMapping::Skip) => Err(ReturnsError::SkipInArray),
            Returns::SetOf(_) => Err(ReturnsError::SetOfInArray),
            Returns::Table(_) => Err(ReturnsError::TableInArray),
        }
    }
}

unsafe impl<'a, T> SqlTranslatable for VariadicArray<'a, T>
where
    T: SqlTranslatable + FromDatum,
{
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        match T::argument_sql()? {
            SqlMapping::As(sql) => Ok(SqlMapping::As(format!("{sql}[]"))),
            SqlMapping::Skip => Err(ArgumentError::SkipInArray),
            SqlMapping::Composite { .. } => Ok(SqlMapping::Composite { array_brackets: true }),
            SqlMapping::Source { .. } => Ok(SqlMapping::Source { array_brackets: true }),
        }
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        match T::return_sql()? {
            Returns::One(SqlMapping::As(sql)) => {
                Ok(Returns::One(SqlMapping::As(format!("{sql}[]"))))
            }
            Returns::One(SqlMapping::Composite { array_brackets: _ }) => {
                Ok(Returns::One(SqlMapping::Composite { array_brackets: true }))
            }
            Returns::One(SqlMapping::Source { array_brackets: _ }) => {
                Ok(Returns::One(SqlMapping::Source { array_brackets: true }))
            }
            Returns::One(SqlMapping::Skip) => Err(ReturnsError::SkipInArray),
            Returns::SetOf(_) => Err(ReturnsError::SetOfInArray),
            Returns::Table(_) => Err(ReturnsError::TableInArray),
        }
    }

    fn variadic() -> bool {
        true
    }
}
