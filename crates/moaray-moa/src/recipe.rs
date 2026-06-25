//! The orchestrator's own view of a MoA recipe.
//!
//! **Dependency boundary:** `moaray-moa` depends only on `moaray-core`, so it
//! cannot (and must not) reference `moaray_config::RecipeConfig`. The bin
//! translates a validated config recipe into this self-contained [`Recipe`] when
//! it wires the orchestrator. Keeping a separate type here means the fan-out core
//! stays testable with hand-built recipes and never grows a config-crate
//! dependency.

/// MoA fusion strategy (mirrors `moaray_config::Strategy`, decoupled by design).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Aggregator fuses all successful proposer outputs into one answer.
    ConcatSynthesize,
    /// Judge selects/merges the single best answer from the successful arms.
    QuorumJudge,
}

/// A resolved MoA recipe the orchestrator runs.
///
/// All invariants (non-empty proposers, `1 <= quorum <= proposers.len()`, known
/// models) are guaranteed by `moaray-config` validation before this is built;
/// the orchestrator trusts them.
#[derive(Debug, Clone)]
pub struct Recipe {
    /// Recipe name (the `<recipe>` in `model: moa/<recipe>`).
    pub name: String,
    /// Proposer model names; each becomes one fan-out arm.
    pub proposers: Vec<String>,
    /// Aggregator/judge model name.
    pub aggregator: String,
    /// Fusion strategy.
    pub strategy: Strategy,
    /// Per-arm timeout in milliseconds (also bounded by `ReqCtx.deadline`).
    pub arm_timeout_ms: u64,
    /// Minimum number of proposer arms that must succeed to proceed.
    pub quorum: usize,
}
