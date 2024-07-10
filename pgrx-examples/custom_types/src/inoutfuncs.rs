use pgrx::prelude::*;
use pgrx::stringinfo::StringInfo;
use serde::{Deserialize, Serialize};

#[derive(PostgresType, Serialize, Deserialize, Debug, PartialEq)]
#[inoutfuncs]
pub struct NullHorror {}

impl InOutFuncs for NullHorror {
    fn input(_input: &core::ffi::CStr) -> Self
    where
        Self: Sized,
    {
        NullHorror {}
    }

    fn output(&self, _buffer: &mut StringInfo) {}

    const NULL_ERROR_MESSAGE: Option<&'static str> = Some("An error message");
}
