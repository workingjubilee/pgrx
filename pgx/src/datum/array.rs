/*
Portions Copyright 2019-2021 ZomboDB, LLC.
Portions Copyright 2021-2022 Technology Concepts & Design, Inc. <support@tcdi.com>

All rights reserved.

Use of this source code is governed by the MIT license that can be found in the LICENSE file.
*/

use crate::{array::RawArray, pg_sys, FromDatum, IntoDatum, PgMemoryContexts};
use bitvec::slice::BitSlice;
use core::ptr::NonNull;
use serde::Serializer;
use std::marker::PhantomData;
use std::{mem, ptr, slice};

pub type VariadicArray<'a, T> = Array<'a, T>;

pub struct Array<'a, T: FromDatum> {
    _ptr: Option<NonNull<pg_sys::varlena>>,
    raw: Option<RawArray>,
    nelems: usize,
    elem_slice: DataKind<'a, T>,
    null_slice: NullKind<'a>,
    _marker: PhantomData<T>,
}

enum DataKind<'a, T> {
    Ref(&'a [pg_sys::Datum]),
    Val(&'a [T]),
}

impl<'a, T> DataKind<'a, T>
where
    T: FromDatum + Clone,
{
    fn get_datum(&self, index: usize, is_null: bool) -> Option<T> {
        match self {
            Self::Ref(d) => unsafe { T::from_datum(d[index], is_null) },
            Self::Val(s) if !is_null => s.get(index).cloned(),
            Self::Val(_) => None,
        }
    }
}

impl<'a, T> DataKind<'a, T> {
    fn as_slice(&self) -> &[T] {
        match self {
            Self::Val(s) => s,
            Self::Ref(datums) => panic!("oh no")
        }
    }
}

impl<'a, T> From<&'a [pg_sys::Datum]> for DataKind<'a, T> {
    fn from(data: &'a [pg_sys::Datum]) -> DataKind<'a, T> {
        DataKind::Ref(data)
    }
}

// FIXME: When Array::over gets removed, this enum can probably be dropped
// since we won't be entertaining ArrayTypes which don't use bitslices anymore.
// However, we could also use a static resolution? Hard to say what's best.
enum NullKind<'a> {
    Bits(&'a BitSlice<u8>),
    Bytes(&'a [bool]),
    Strict(usize),
}

impl<'a> From<&'a [bool]> for NullKind<'a> {
    fn from(b8: &'a [bool]) -> NullKind<'a> {
        NullKind::Bytes(b8)
    }
}

impl NullKind<'_> {
    fn check(&self, index: usize) -> Option<bool> {
        match self {
            Self::Bits(b1) => b1.get(index).map(|b| !b),
            Self::Bytes(b8) => b8.get(index).map(|b| *b),
            Self::Strict(len) => index.le(len).then(|| false),
        }
    }
}

impl<'a, T: FromDatum + Clone + serde::Serialize> serde::Serialize for Array<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'a, T: FromDatum + Clone + serde::Serialize> serde::Serialize for ArrayTypedIterator<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.array.iter())
    }
}

#[deny(unsafe_op_in_unsafe_fn)]
impl<'a, T: FromDatum> Array<'a, T> {
    /// Create an [`Array`](crate::datum::Array) over an array of [`pg_sys::Datum`](pg_sys::Datum) values and a corresponding array
    /// of "is_null" indicators
    ///
    /// `T` can be [`pg_sys::Datum`](pg_sys::Datum) if the elements are not all of the same type
    ///
    /// # Safety
    ///
    /// This function requires that:
    /// - `elements` is non-null
    /// - `nulls` is non-null
    /// - both `elements` and `nulls` point to a slice of equal-or-greater length than `nelems`
    #[deprecated(
        since = "0.5.0",
        note = "creating arbitrary Arrays from raw pointers has unsound interactions!
    please open an issue in tcdi/pgx if you need this, with your stated use-case"
    )]
    pub unsafe fn over(
        elements: *mut pg_sys::Datum,
        nulls: *mut bool,
        nelems: usize,
    ) -> Array<'a, T> {
        // FIXME: This function existing prevents simply using NonNull<varlena>
        // or NonNull<ArrayType>. It has also caused issues like tcdi/pgx#633
        // Ideally it would cease being used soon.
        // It can be replaced with ways to make Postgres varlena arrays in Rust,
        // if there are any users who desire such a thing.
        //
        // Remember to remove the Array::over tests in pgx-tests/src/tests/array_tests.rs
        // when you finally kill this off.
        let _ptr: Option<NonNull<pg_sys::varlena>> = None;
        let raw: Option<RawArray> = None;
        Array::<T> {
            _ptr,
            raw,
            nelems,
            elem_slice: unsafe { slice::from_raw_parts(elements, nelems) }.into(),
            null_slice: unsafe { slice::from_raw_parts(nulls, nelems) }.into(),
            _marker: PhantomData,
        }
    }

    /// # Safety
    ///
    /// This function requires that the RawArray was obtained in a properly-constructed form
    /// (probably from Postgres).
    unsafe fn deconstruct_from(
        _ptr: Option<NonNull<pg_sys::varlena>>,
        raw: RawArray,
        typlen: libc::c_int,
        typbyval: bool,
        typalign: libc::c_char,
    ) -> Array<'a, T> {
        let oid = raw.oid();
        let len = raw.len();
        let array = raw.into_ptr().as_ptr();

        // outvals for deconstruct_array
        let mut elements: *mut pg_sys::Datum = ptr::null_mut();
        let mut nulls: *mut bool = ptr::null_mut();
        let mut nelems = 0;

        // FIXME: This way of getting array buffers causes problems for any Drop impl,
        // and clashes with assumptions of Array being a "zero-copy", lifetime-bound array,
        // some of which are implicitly embedded in other methods (e.g. Array::over).
        // It also risks leaking memory, as deconstruct_array calls palloc.
        // So either we don't use this, we use it more conditionally, or something.
        // SAFETY: We have already asserted the validity of the RawArray, so
        // this only makes mistakes if we mix things up and pass Postgres the wrong data.
        unsafe {
            pg_sys::deconstruct_array(
                array,
                oid,
                typlen,
                typbyval,
                typalign,
                &mut elements,
                &mut nulls,
                &mut nelems,
            )
        };

        let nelems = nelems as usize;

        // Check our RawArray len impl for correctness.
        assert_eq!(nelems, len);
        let mut raw = unsafe { RawArray::from_ptr(NonNull::new_unchecked(array)) };

        let null_slice = raw
            .nulls_bitslice()
            .map(|nonnull| NullKind::Bits(unsafe { &*nonnull.as_ptr() }))
            .unwrap_or(NullKind::Strict(nelems));

        Array {
            _ptr,
            raw: Some(raw),
            nelems,
            elem_slice: unsafe { slice::from_raw_parts(elements, nelems) }.into(),
            null_slice,
            _marker: PhantomData,
        }
    }

    unsafe fn direct_from(
        _ptr: Option<NonNull<pg_sys::varlena>>,
        mut raw: RawArray,
        typlen: libc::c_int,
        typalign: libc::c_char,
    ) -> Array<'a, T> {
        let oid = raw.oid();
        let len = raw.len();
        // Attempt to handle the array directly.
        // First, assert on alignment
        let eval_align = match typalign as u8 {
            b'c' => 1,
            b's' => mem::align_of::<libc::c_short>(),
            b'i' => mem::align_of::<libc::c_int>(),
            b'd' => mem::align_of::<f64>(),
            _ => panic!("PGX encountered unfamiliar typalign?"),
        };
        let mem_align = mem::align_of::<T>();
        assert_eq!(
            eval_align,
            mem_align,
            "by-value align mismatch. Postgres said {ch},
            type was Rust: {rs_ty}, OID#{oid}, Len: {typlen}",
            ch = char::from(typalign as u8),
            rs_ty = std::any::type_name::<T>()
        );

        let elems_raw = raw.data();
        let nulls_raw = raw.nulls_bitslice();
        let elem_slice = DataKind::Val(unsafe { &*elems_raw.expect("surely this won't go wrong?").as_ptr() });
        let null_slice = match nulls_raw {
            Some(raw) => NullKind::Bits(unsafe { &*raw.as_ptr() }),
            None => NullKind::Strict(len),
        };

        Array {
            _ptr,
            raw: Some(raw),
            nelems: len,
            elem_slice,
            null_slice,
            _marker: PhantomData,
        }
    }

    pub fn into_array_type(mut self) -> *const pg_sys::ArrayType {
        let ptr = mem::take(&mut self.raw).map(|raw| raw.into_ptr().as_ptr() as _);
        mem::forget(self);
        ptr.unwrap_or(ptr::null())
    }

    pub fn as_slice(&self) -> &[T] {
        self.elem_slice.as_slice()
    }

    /// Return an Iterator of Option<T> over the contained Datums.
    pub fn iter(&self) -> ArrayIterator<'_, T> {
        ArrayIterator {
            array: self,
            curr: 0,
        }
    }

    /// Return an Iterator of the contained Datums (converted to Rust types).
    ///
    /// This function will panic when called if the array contains any SQL NULL values.
    pub fn iter_deny_null(&self) -> ArrayTypedIterator<'_, T> {
        if let Some(at) = &self.raw {
            // SAFETY: if Some, then the ArrayType is from Postgres
            if unsafe { at.any_nulls() } {
                panic!("array contains NULL");
            }
        } else {
            panic!("array is NULL");
        };

        ArrayTypedIterator {
            array: self,
            curr: 0,
        }
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
    pub fn get(&self, i: usize) -> Option<Option<T>>
    where
        T: Clone,
    {
        self.null_slice
            .check(i)
            .map(|b| self.elem_slice.get_datum(i, b))
    }
}

pub struct ArrayTypedIterator<'a, T: 'a + FromDatum> {
    array: &'a Array<'a, T>,
    curr: usize,
}

impl<'a, T: FromDatum + Clone> Iterator for ArrayTypedIterator<'a, T> {
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

pub struct ArrayIterator<'a, T: 'a + FromDatum> {
    array: &'a Array<'a, T>,
    curr: usize,
}

impl<'a, T: FromDatum + Clone> Iterator for ArrayIterator<'a, T> {
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

impl<'a, T: FromDatum + Clone> IntoIterator for Array<'a, T> {
    type Item = Option<T>;
    type IntoIter = ArrayIntoIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        ArrayIntoIterator {
            array: self,
            curr: 0,
        }
    }
}

impl<'a, T: FromDatum + Clone> Iterator for ArrayIntoIterator<'a, T> {
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

impl<'a, T: FromDatum> FromDatum for Array<'a, T> {
    #[inline]
    unsafe fn from_datum(datum: pg_sys::Datum, is_null: bool) -> Option<Array<'a, T>> {
        if is_null || datum.is_null() {
            None
        } else {
            let ptr = datum.ptr_cast();
            let array = pg_sys::pg_detoast_datum(datum.ptr_cast()) as *mut pg_sys::ArrayType;
            let raw =
                RawArray::from_ptr(NonNull::new(array).expect("detoast returned null ArrayType*"));
            let ptr = NonNull::new(ptr);

            // outvals for get_typlenbyvalalign()
            let mut typlen = 0;
            let mut typbyval = false;
            let mut typalign = 0;
            let oid = raw.oid();

            pg_sys::get_typlenbyvalalign(oid, &mut typlen, &mut typbyval, &mut typalign);
            let typlen = typlen as _;

            if typbyval && (mem::size_of::<T>() == typlen as usize) && if let 1 | 2 | 4 | 8 = typlen { true } else { false } {
                Some(Array::direct_from(ptr, raw, typlen, typalign))
            } else {
                Some(Array::deconstruct_from(
                    ptr, raw, typlen, typbyval, typalign,
                ))
            }
        }
    }
}

impl<T: FromDatum> FromDatum for Vec<T>
where
    T: Clone,
{
    #[inline]
    unsafe fn from_datum(datum: pg_sys::Datum, is_null: bool) -> Option<Vec<T>> {
        if is_null || datum.is_null() {
            None
        } else {
            let array = Array::<T>::from_datum(datum, is_null).unwrap();
            let mut v = Vec::with_capacity(array.len());

            for element in array.iter() {
                v.push(element.expect("array element was NULL"))
            }
            Some(v)
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
            // shoudln't happen
            None
        } else {
            Some(unsafe {
                pg_sys::makeArrayResult(state, PgMemoryContexts::CurrentMemoryContext.value())
            })
        }
    }

    fn type_oid() -> u32 {
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
            // shoudln't happen
            None
        } else {
            Some(unsafe {
                pg_sys::makeArrayResult(state, PgMemoryContexts::CurrentMemoryContext.value())
            })
        }
    }

    fn type_oid() -> u32 {
        unsafe { pg_sys::get_array_type(T::type_oid()) }
    }

    #[inline]
    fn is_compatible_with(other: pg_sys::Oid) -> bool {
        Self::type_oid() == other || other == unsafe { pg_sys::get_array_type(T::type_oid()) }
    }
}
