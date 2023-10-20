#![deny(unsafe_op_in_unsafe_fn)]
use crate::pg_sys;
use core::{marker::PhantomData, ptr::NonNull};

/// A borrowed memory context.
pub struct MemCx<'mcx> {
    ptr: NonNull<pg_sys::MemoryContextData>,
    _marker: PhantomData<&'mcx pg_sys::MemoryContextData>,
}

impl<'mcx> MemCx<'mcx> {
    /// Create a MemCx from a [pg_sys::MemoryContext].
    ///
    /// # Safety
    ///
    /// Dereferences the pointer and generates a memory context with the inferred lifetime.
    /// This should be made public only after we have other safe interfaces published first,
    /// as it is both easy and also pretty incorrect.
    pub(crate) unsafe fn from_ptr(ptr: pg_sys::MemoryContext) -> Option<MemCx<'mcx>> {
        unsafe {
            pg_sys::MemoryContextIsValid(ptr)
                .then_some(MemCx { ptr: NonNull::new_unchecked(ptr), _marker: PhantomData })
        }
    }

    /// Forwards to [pg_sys::MemoryContextAlloc].
    pub(crate) unsafe fn alloc_bytes(&self, size: usize) -> *mut u8 {
        // SAFETY: We have no real safety invariants to uphold, aside from those that pgrx enforces.
        unsafe { pg_sys::MemoryContextAlloc(self.ptr.as_ptr(), size).cast() }
    }

    /// Run some code inside this context, switching to it and then switching back.
    pub(crate) unsafe fn exec_in<T>(&self, f: impl FnOnce() -> T) -> T {
        let res;
        // SAFETY: This sort of thing is why we need only one thread to be manipulating the Postgres context.
        // Fortunately, pgrx runs main thread checks and the like whenever we call Postgres functions.
        unsafe {
            let remembered = pg_sys::MemoryContextSwitchTo(self.ptr.as_ptr());
            res = f();
            pg_sys::MemoryContextSwitchTo(remembered);
        }
        res
    }
}

/// Acquire the current context and operate inside it.
pub fn current_context<'curr, F, T>(f: F) -> T
where
    F: for<'clos> FnOnce(&'clos MemCx<'curr>) -> T,
{
    let memcx = unsafe { MemCx::from_ptr(pg_sys::CurrentMemoryContext) }
        .expect("no current memory context?");
    let ret = { f(&memcx) };
    ret
}
