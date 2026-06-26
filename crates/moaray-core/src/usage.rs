//! Usage accounting types — the `UsageSink` trait, the `UsageRecord` DTO, and the
//! pure `compute_cost` helper.
//!
//! These live in `moaray-core` (the dependency floor) on purpose: the app builds
//! a [`UsageRecord`] — including its cost — *before* handing it to a concrete
//! sink, so the cost helper must not live in the store crate (dependency
//! inversion). The trait is object-safe and `Send + Sync` so an
//! `Arc<dyn UsageSink>` can sit in the shared app state and the record can be
//! moved to a dedicated writer thread.
//!
//! **Secret hygiene (no-secret-logging, P95):** a [`UsageRecord`] carries only
//! non-secret, low-cardinality fields — never an api_key, a token, prompt/response
//! text, or the internal `state_key` (which holds `base_url`). The `upstream_id`
//! is the low-cardinality observability label, never the state key.

/// Which response path produced a usage row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsagePath {
    /// A single passthrough (non-stream) upstream call.
    Passthrough,
    /// A MoA fan-out (proposer or aggregator arm).
    Moa,
}

impl UsagePath {
    /// Stable, low-cardinality string form (also the stored `path` column).
    pub fn as_str(&self) -> &'static str {
        match self {
            UsagePath::Passthrough => "passthrough",
            UsagePath::Moa => "moa",
        }
    }
}

/// Which arm of a request a row accounts for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageArm {
    /// A passthrough request (the whole request is one arm).
    Passthrough,
    /// A MoA proposer arm.
    Proposer,
    /// A MoA aggregator/judge arm.
    Aggregator,
}

impl UsageArm {
    /// Stable, low-cardinality string form (also the stored `arm` column).
    pub fn as_str(&self) -> &'static str {
        match self {
            UsageArm::Passthrough => "passthrough",
            UsageArm::Proposer => "proposer",
            UsageArm::Aggregator => "aggregator",
        }
    }
}

/// Outcome class of one accounted upstream call.
///
/// Degradation is explicit: `ok_no_usage`/`failed`/`timeout`/`unpriced` rows
/// store `NULL` cost (and usually `NULL` tokens) — **unmeasured is never recorded
/// as zero** (zero would read as "free", which is wrong).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageStatus {
    /// Usable response with a usage object → tokens + cost present (if priced).
    Ok,
    /// Usable response but no usage object → tokens NULL, cost NULL.
    OkNoUsage,
    /// Tokens present but the model has no configured price → cost NULL.
    Unpriced,
    /// Upstream/transport error → tokens NULL, cost NULL.
    Failed,
    /// Did not return within the effective timeout → tokens NULL, cost NULL.
    Timeout,
}

impl UsageStatus {
    /// Stable, low-cardinality string form (also the stored `status` column).
    pub fn as_str(&self) -> &'static str {
        match self {
            UsageStatus::Ok => "ok",
            UsageStatus::OkNoUsage => "ok_no_usage",
            UsageStatus::Unpriced => "unpriced",
            UsageStatus::Failed => "failed",
            UsageStatus::Timeout => "timeout",
        }
    }
}

/// One accounted upstream call. All fields are non-secret and `Send` (the writer
/// moves records across an OS-thread boundary — no `Rc`/non-`Send` members).
///
/// The `id` column (autoincrement PK) is assigned by the store, not here. Token
/// counts and the price snapshot are stored raw so cost is recomputable exactly
/// at query time regardless of the convenience `cost_nano_usd` column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageRecord {
    /// Correlation id (`ReqCtx::request_id`); ties one fan-out's arms together.
    pub request_id: String,
    /// Wall-clock unix-millis stamped at the EMISSION site (the app handler),
    /// never by the writer — batching/backlog must not skew billing time.
    pub ts_unix_ms: i64,
    /// Which response path produced this row.
    pub path: UsagePath,
    /// Which arm this row accounts for.
    pub arm: UsageArm,
    /// Public model name.
    pub model: String,
    /// Low-cardinality observability label (NOT the internal `state_key`).
    pub upstream_id: String,
    /// Non-secret caller key label (`ReqCtx::caller_key_id`).
    pub caller_key_id: String,
    /// Raw prompt tokens (source of truth). `None` = unmeasured.
    pub prompt_tokens: Option<i64>,
    /// Raw completion tokens (source of truth). `None` = unmeasured.
    pub completion_tokens: Option<i64>,
    /// Prompt price snapshot applied (nano-USD per 1M tokens). `None` = unpriced.
    pub price_prompt_nano_per_mtok: Option<i64>,
    /// Completion price snapshot applied (nano-USD per 1M tokens). `None` = unpriced.
    pub price_completion_nano_per_mtok: Option<i64>,
    /// Convenience computed cost (nano-USD). `None` = unmeasured/unpriced.
    pub cost_nano_usd: Option<i64>,
    /// Outcome class.
    pub status: UsageStatus,
}

/// The accounting sink: object-safe, `Send + Sync`, `record`-only.
///
/// The hot path calls [`UsageSink::record`] which must never block (the concrete
/// SQLite sink `try_send`s onto a bounded channel and drops on full). Shutdown
/// flushing is handled by a separate writer handle held outside the app state, so
/// this trait needs no `flush`/`join` method and stays trivially object-safe.
pub trait UsageSink: Send + Sync {
    /// Enqueue one accounted call. Must not block the caller.
    fn record(&self, rec: UsageRecord);
}

/// Compute `cost_nano_usd` from raw tokens and a nano-USD-per-Mtok price snapshot.
///
/// Canonical formula (single source of truth, plan DP2):
/// `round(prompt_tokens * price_prompt / 1e6) + round(completion_tokens * price_completion / 1e6)`.
/// Returns `None` if either tokens or prices are absent (an unmeasured/unpriced
/// row stores NULL cost, never 0). A missing token side defaults to 0 *only when
/// the other side and both prices are present* — i.e. a genuinely-zero token
/// count still yields a (possibly 0) cost, distinct from `None`.
pub fn compute_cost(
    prompt_tokens: Option<i64>,
    completion_tokens: Option<i64>,
    price_prompt_nano_per_mtok: Option<i64>,
    price_completion_nano_per_mtok: Option<i64>,
) -> Option<i64> {
    // Cost is defined only when we have measured tokens AND a price snapshot.
    let (pt, ct) = (prompt_tokens?, completion_tokens?);
    let (pp, cp) = (
        price_prompt_nano_per_mtok?,
        price_completion_nano_per_mtok?,
    );
    // i128 intermediates so a large token count * nano price cannot overflow i64
    // before the divide. round-half-up via (x + 5e5) / 1e6 on non-negative inputs.
    let mtok: i128 = 1_000_000;
    let round_div = |tokens: i64, price: i64| -> i128 {
        let num = (tokens as i128) * (price as i128);
        // round to nearest; tokens/price are validated non-negative.
        (num + mtok / 2) / mtok
    };
    let cost = round_div(pt, pp) + round_div(ct, cp);
    Some(cost as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_token_at_low_price_is_nonzero() {
        // $0.15/Mtok → 150_000_000 nano-USD/Mtok. 1 prompt token → 150 nano-USD.
        let cost = compute_cost(Some(1), Some(0), Some(150_000_000), Some(150_000_000));
        assert_eq!(cost, Some(150));
        assert!(cost.unwrap() > 0, "G1 invariant: a 1-token call costs > 0");
    }

    #[test]
    fn sums_prompt_and_completion() {
        // 1000 prompt @ $0.15/Mtok + 500 completion @ $0.60/Mtok
        // = round(1000*150_000_000/1e6) + round(500*600_000_000/1e6)
        // = 150_000 + 300_000 = 450_000 nano-USD.
        let cost = compute_cost(Some(1000), Some(500), Some(150_000_000), Some(600_000_000));
        assert_eq!(cost, Some(450_000));
    }

    #[test]
    fn rounds_half_up() {
        // 3 tokens @ 1 nano/Mtok → 3/1e6 = 0.000003 → rounds to 0.
        assert_eq!(compute_cost(Some(3), Some(0), Some(1), Some(1)), Some(0));
        // 500_001 tokens @ 1 nano/Mtok → 0.500001 → rounds to 1.
        assert_eq!(
            compute_cost(Some(500_001), Some(0), Some(1), Some(1)),
            Some(1)
        );
    }

    #[test]
    fn none_when_tokens_or_price_absent() {
        assert_eq!(compute_cost(None, Some(10), Some(1), Some(1)), None);
        assert_eq!(compute_cost(Some(10), None, Some(1), Some(1)), None);
        assert_eq!(compute_cost(Some(10), Some(10), None, Some(1)), None);
        assert_eq!(compute_cost(Some(10), Some(10), Some(1), None), None);
    }

    #[test]
    fn genuinely_zero_tokens_is_zero_not_none() {
        assert_eq!(
            compute_cost(Some(0), Some(0), Some(150_000_000), Some(150_000_000)),
            Some(0)
        );
    }

    #[test]
    fn no_overflow_on_large_counts() {
        // 1e12 tokens @ 1e9 nano/Mtok would overflow i64 if multiplied directly.
        let cost = compute_cost(Some(1_000_000_000_000), Some(0), Some(1_000_000_000), Some(1));
        // 1e12 * 1e9 / 1e6 = 1e15 nano-USD, fits in i64.
        assert_eq!(cost, Some(1_000_000_000_000_000));
    }
}
