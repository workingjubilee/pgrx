use crate::datum::BorrowDatum;
use crate::layout::PassBy;
use crate::pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};
use crate::{pg_sys, varlena};
use core::borrow::Borrow;
use core::{ptr, slice, str};

use bstr::{BStr, ByteSlice};

// We reexport these types so people don't have to care whether they're pulled from BStr or std,
// they just use the ones from pgrx::text::*
pub use bstr::{Bytes, Chars};
pub use core::str::{Utf8Chunks, Utf8Error};

/// A Postgres string, AKA `TEXT`.
///
/// This is a varlena: a reference to a variable-length header followed by a slice of bytes.
/// Usually this will be UTF-8, but this is not always strictly enforced by PostgreSQL.
#[repr(transparent)]
pub struct Text([u8]);

// API decision: we could deref to TextData and move some fn to TextData so it can be returned from
// `split_at`, `trim`, etc., and thus preserve conveniences that [u8] doesn't have?

impl Text {
    /// Obtain a reference to the Text's data as bytes
    pub fn as_bytes(&self) -> &[u8] {
        let self_ptr = self as *const Text as *const pg_sys::varlena;
        unsafe {
            let len = varlena::varsize_any_exhdr(self_ptr);
            let data = varlena::vardata_any(self_ptr);

            slice::from_raw_parts(data.cast::<u8>(), len)
        }
    }

    /// Obtain a mutable reference the Text's data as bytes
    ///
    /// # Safety
    /// Like [`str::as_bytes_mut`], this can cause problems if you change Text in a way that
    /// your database is not specified to support, so the caller must assure that it remains in
    /// a valid encoding for the database.
    pub unsafe fn as_bytes_mut(&mut self) -> &mut [u8] {
        let self_ptr = self as *mut Text as *mut pg_sys::varlena;
        unsafe {
            let len = varlena::varsize_any_exhdr(self_ptr);
            let data = varlena::vardata_any(self_ptr);

            slice::from_raw_parts_mut(data.cast::<u8>().cast_mut(), len)
        }
    }

    /// Reborrow `&Text as `&BStr`
    ///
    /// We do not implement Deref to BStr or [u8] because we'd like to expose a more selective API.
    /// Several fn that [u8] implements are implemented very differently on str, and we would like
    /// the API of Text to "feel like" that of str in most cases.
    fn as_bstr(&self) -> &BStr {
        self.as_bytes().borrow()
    }

    /// Iterate over the UTF-8 characters of this Text
    ///
    /// If the data is not UTF-8, the replacement character � is returned.
    pub fn chars(&self) -> Chars<'_> {
        self.as_bstr().chars()
    }

    /// Iterate over the Text's data as bytes
    pub fn bytes(&self) -> Bytes<'_> {
        self.as_bstr().bytes()
    }

    /// Is the data ASCII?
    pub fn is_ascii(&self) -> bool {
        self.as_bytes().is_ascii()
    }

    /// Is the varlena larger than its header?
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }

    /// Length of the data in bytes
    pub fn len_data(&self) -> usize {
        self.as_bytes().len()
    }

    /// Length of the entire varlena in bytes
    pub fn len_full(&self) -> usize {
        self.0.len()
    }

    /// Obtain a reference to the varlena data if it is a UTF-8 str
    pub fn to_str(&self) -> Result<&str, Utf8Error> {
        str::from_utf8(self.as_bytes())
    }

    /// Iterate over the UTF-8 chunks of the Text's data
    pub fn utf8_chunks(&self) -> Utf8Chunks {
        self.as_bytes().utf8_chunks()
    }
}

unsafe impl BorrowDatum for Text {
    const PASS: PassBy = PassBy::Ref;
    unsafe fn point_from(ptr: *mut u8) -> *mut Self {
        unsafe {
            let len = varlena::varsize_any(ptr.cast());
            ptr::slice_from_raw_parts_mut(ptr, len) as *mut Text
        }
    }
}

unsafe impl<'dat> SqlTranslatable for &'dat Text {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::literal("TEXT"))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::literal("TEXT")))
    }
}