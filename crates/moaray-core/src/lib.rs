//! moaray-core — shared wire types, the dual-path `Provider` trait, the model
//! router, and the unified error model. No I/O, no HTTP server, no config: this
//! crate is the dependency floor that `moaray-providers`, `moaray-moa`, and the
//! `moaray` bin all build on.

pub mod error;
pub mod provider;
pub mod router;
pub mod types;
pub mod usage;

pub use error::{Error, ErrorEnvelope, Result};
pub use provider::{ByteStream, Provider, RawResponse, ReqCtx};
pub use router::{route, RouteTarget, MOA_AUTO, MOA_PREFIX};
pub use types::{ChatChunk, ChatMessage, ChatRequest, ChatResponse};
pub use usage::{compute_cost, UsageArm, UsagePath, UsageRecord, UsageSink, UsageStatus};
