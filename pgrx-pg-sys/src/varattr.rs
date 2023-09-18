/*!
A definition of variable-length arrays

A varlena type looks kinda like this:
```rust
#[repr(C)]
struct Varlena<T> {
    header: T,
    vl_data: MaybeUninit<[u8]>, // implies !Sized
}

#[repr(C)]
struct AnyT<const _1or4: usize> {
    vl_len: [u8; _1or4],
    maybe_fields: Types,
}
```

Some varlena types guarantee vl_len is 4 bytes and aligned, calling it an i32
However, it is nonetheless inadvisable to simply poke at the raw integer,
as `vl_len` has a special encoding for the length that can vary (see Toast).
Even if a pointer arrives over Postgres FFI using one of these aligned types,
it is wise to be cautious about this, as pointers may be null and unaligned.
Postgres usually offers some alignment, but this complicates that.
*/

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
    fn vlen_bytes(self) -> u8 {
        match self {
            ToastBits::Direct => 4,
            ToastBits::Byte => 1,
            ToastBits::Compressed => 4,
        }
    }
}

type vsize = u32;

fn read_varlena_word() -> vsize {
    todo!()
}

struct VarBytes {
    p: *mut u8,
    len: u32,
}

type Encoding = ();
enum Toasting {
    OutOfLine(Encoding),
    Byte(u8),
    Compressed(u32),
    Direct(u32),
}
