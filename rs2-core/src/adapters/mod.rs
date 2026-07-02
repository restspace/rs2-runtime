//! Built-in capability implementations. Embedders supply their own by
//! implementing the traits in [`crate::capabilities`].
//!
//! M1 ships `local_fs` (streamed file store on the local filesystem) and
//! `mem_data` (in-memory data store, also the default test double).
//! S3 and database-backed adapters are M1 stretch items tracked in the PRD;
//! they slot in behind the same traits without touching services.

mod credential;
mod file_data;
mod file_log;
#[cfg(feature = "http")]
mod http_out;
mod local_fs;
mod mem_data;
mod mem_query;
mod registry;

pub use credential::{AuthStrategy, CredentialInjector};
pub use file_data::FileDataStore;
pub use file_log::FileLogStore;
#[cfg(feature = "http")]
pub use http_out::UreqHttpOut;
pub use local_fs::LocalFsFileStore;
pub use mem_data::MemDataStore;
pub use mem_query::MemQueryStore;
pub use registry::{BuiltinRegistry, DataFactory, FileFactory, QueryFactory};
