// From "external" crates:
pub use ::pgx_macros::{
    extension_sql, extension_sql_file, pg_aggregate, pg_extern, pg_guard, pg_schema, pg_test,
    pg_trigger, search_path, PostgresEnum, PostgresType,
};
pub use ::pgx_pg_sys as pg_sys;

// Necessary local macros:
pub use crate::{default, name};

// Needed for variant RETURNS
pub use crate::iter::{SetOfIterator, TableIterator};

// Needed for complex returns.
pub use crate::pgbox::PgBox;
pub use crate::PgHeapTuple;

pub use crate::datum::{Date, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone};

pub use crate::pg_sys::PgBuiltInOids;
