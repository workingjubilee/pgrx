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
as `vl_len` has a special encoding for length payload that can vary.
Even if a pointer arrives over Postgres FFI using one of these aligned types,
it is wise to be cautious about this.
*/

/**
Designates something as a Postgres "variable length array" type
*/
// This probably shouldn't be sealed, as many varlenas are user types.
pub unsafe trait VarLen {}
