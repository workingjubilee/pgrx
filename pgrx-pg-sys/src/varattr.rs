/*!
A definition of variable-length arrays

A varlena type looks kinda like this:
```rust
#[repr(C)]
struct Varlena<T, const B: usize>
where
    B: 1 | 2 | 4,
{
    vl_len: [u8; B],
    header: T::Header,
    vl_data: [MaybeUninit<u8>], // so, !Sized
}
```

Some varlena types guarantee vl_len is 4 bytes and aligned, calling it an i32
However, it is nonetheless inadvisable to simply poke at the raw integer,
as `vl_len` has a special encoding for the length that can vary (see Toast).
Even if a pointer arrives over Postgres FFI using one of these aligned types,
it is wise to be cautious about this, as pointers may be null and unaligned.
Postgres usually offers some alignment, but this complicates that.
*/

use core::mem::MaybeUninit;
use core::ops::{Deref, DerefMut};
use core::ptr::NonNull;

/**
Designates something as a Postgres "variable length array" type
*/
// This probably shouldn't be sealed, as many varlenas are user types.
pub unsafe trait VarLen {}

/**
"The Oversized-Attribute Storage Technique".

The C in the C Programming Language apparently stands for Corny.

*/
pub unsafe trait Toast {
    type De;

    fn detoast(&self) -> Self::De {
        todo!()
    }
}

enum ToastBits {
    Direct = 0b00,
    Byte = 0b1,
    Compressed = 0b10,
}

impl ToastBits {
    const fn vlen_bytes(self) -> u8 {
        match self {
            ToastBits::Direct => 4,
            ToastBits::Byte => 1,
            ToastBits::Compressed => 4,
        }
    }
}

mod vlahead_experiment {
    use super::*;
    // this is possible, using dynamic traits, but probably isn't an improvement in terms of codegen
    // compared to simply doing the check each time?
    struct VlaHead<const N: usize>([u8; N]);

    trait VlaHeader {}

    impl VlaHeader for VlaHead<1> {}

    impl VlaHeader for VlaHead<4> {}

    fn array_from_toast_bits<const N: usize>(bits: ToastBits) -> Box<dyn VlaHeader> {
        match bits.vlen_bytes() {
            1 => Box::new(VlaHead([0; 1])),
            4 => Box::new(VlaHead([0; 4])),
            _ => panic!(),
        }
    }
}

type vsize = u32;

fn read_varlena_word() -> vsize {
    todo!()
}

/**
A varlena pointer plus metadata.

This offers an unwrapped version of the information needed to handle a varlena pointer,
and is the type that should be produced by first unwrapping any varlena.
*/
struct VlaBytes {
    p: *mut u8,
    b_len: vsize,
    kind: Toasting,
}

impl VlaBytes {
    pub fn as_uninit_bytes(&self) -> &[MaybeUninit<u8>] {
        unsafe { core::slice::from_raw_parts(self.p.cast(), self.b_len as _) }
    }

    pub fn as_mut_uninit_bytes(&self) -> &mut [MaybeUninit<u8>] {
        unsafe { core::slice::from_raw_parts_mut(self.p.cast(), self.b_len as _) }
    }
}

// #[derive(Clone, Copy)]
// struct vartag_external(u8);
// this is actually a byte tag, we ne

#[derive(Clone, Copy)]
#[repr(u8)]
enum VarTagExternal {
    Memory = 1,
    Expanded = 2,
    ExpandedMut = 3,
    OnDisk = 18,
}

/// Representation of toast bits
#[repr(u8)]
#[derive(Clone, Copy)]
enum Toasting {
    Direct = 0b00,
    Short = 0b1,
    Compressed = 0b10,
}

enum VarlenaKind {
    Short(NonNull<ShortVarlena>),
    Compressed(NonNull<VarlenaBytes>),
    Full(NonNull<VarlenaBytes>),
    OutOfLine(ToastPtrKind),
}

enum ToastPtrKind {
    Table(ToastPtr<crate::varatt_external>),
    Memory(Unaligned<crate::varatt_indirect>),
    Expanded(ToastPtr<crate::varatt_expanded>),
    ExpandedMut(ToastPtr<crate::varatt_expanded>),
}

/// "Toast pointers" represent any toast indirection
///
/// They can be actual pointers, or represent a value in a table.
#[repr(C)]
struct ToastPtr<T> {
    vl_len_: u8,
    vl_tag_: VarTagExternal,
    data: Unaligned<T>,
}

trait HasVhdr {}

impl<T> HasVhdr for ToastPtr<T> {}

impl<T> Deref for ToastPtr<T> {
    type Target = Unaligned<T>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for ToastPtr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

struct vartag_external(u8);

struct VarlenaBytes {}

struct ExternalToast(Unaligned<crate::varatt_external>);
struct IndirectToast(Unaligned<crate::varatt_indirect>);
struct ExpandedToast(Unaligned<crate::varatt_expanded>);
struct ExpandedToastMut(Unaligned<crate::varatt_expanded>);

#[repr(C, packed)]
pub struct Unaligned<T>(T);
// use unaligned::Unaligned;

impl Toasting {
    fn as_bitmask(self) -> u8 {
        let endian =
            if cfg!(target_endian = "big") { u8::reverse_bits } else { core::convert::identity };
        endian(match self {
            Toasting::Direct => 0b00,
            Toasting::Short => 0b01,
            Toasting::Compressed => 0b10,
        })
    }
}

#[repr(C)]
union VlaHBytes {
    short: u8,
    tagged: [u8; 2],
    full: [u8; 4],
}

#[repr(C)]
struct ShortVarlena {
    len: u8,
    bytes: [MaybeUninit<u8>],
}

#[repr(C)]
struct B1_3 {
    vl_len: u8,
    bytes: MaybeUninit<[u8; 3]>,
}

#[repr(C)]
struct B1_Tag_2 {
    byte: u8,
    tag: u8,
    bytes: MaybeUninit<[u8; 2]>,
}
