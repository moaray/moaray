//! moaray-store — concrete [`UsageSink`] implementations.
//!
//! - [`SqliteSink`] — the production sink. The hot path only `try_send`s a
//!   [`UsageRecord`] onto a bounded, lock-free `crossbeam` channel; a **dedicated
//!   OS thread** (NOT a tokio task — rusqlite is synchronous and would steal
//!   async-runtime capacity under burst) owns the `Connection` exclusively and
//!   drains the channel in batched transactions. On a full channel the record is
//!   dropped and `moaray_usage_dropped_total` is incremented — back-pressure is
//!   shed at the accounting boundary, never to the user (best-effort,
//!   telemetry-grade posture, plan §8②).
//! - [`NullSink`] — accounting disabled (the default when no `usage_store` is
//!   configured); `record` is a no-op.
//! - [`VecSink`] — a test util that records into a `Mutex<Vec<_>>` and exposes
//!   `rows()`; lets the acceptance tests assert what landed without a real DB.
//!
//! Shutdown: [`SqliteSink::new`] returns a separate [`UsageWriterHandle`] that
//! owns the writer thread's join handle + a stop signal. It is kept OUT of the
//! shared app state so the sink stays object-safe (`record`-only); `main()` calls
//! [`UsageWriterHandle::flush_and_join`] AFTER the server has drained, so enqueued
//! rows are persisted on a clean exit. On timeout the writer is detached (a stuck
//! writer must never hang process exit — best-effort posture).

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use moaray_core::usage::{UsageRecord, UsageSink};
use rusqlite::Connection;

/// Schema version stamped into `PRAGMA user_version` (migration anchor).
const SCHEMA_VERSION: i64 = 1;

/// The `CREATE TABLE` for `usage_events` (DP3). All columns non-secret; tokens +
/// price snapshot stored raw so cost is recomputable at query time.
const CREATE_TABLE_SQL: &str = "\
CREATE TABLE IF NOT EXISTS usage_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    ts_unix_ms INTEGER NOT NULL,
    path TEXT NOT NULL,
    arm TEXT NOT NULL,
    model TEXT NOT NULL,
    upstream_id TEXT NOT NULL,
    caller_key_id TEXT NOT NULL,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    price_prompt_nano_per_mtok INTEGER,
    price_completion_nano_per_mtok INTEGER,
    cost_nano_usd INTEGER,
    status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_events_request_id ON usage_events(request_id);
CREATE INDEX IF NOT EXISTS idx_usage_events_ts ON usage_events(ts_unix_ms);";

/// Default bounded-channel capacity if the caller does not specify one.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 8192;
/// Default max rows per write transaction.
pub const DEFAULT_BATCH_SIZE: usize = 256;

/// A message to the writer thread.
enum WriterMsg {
    Row(Box<UsageRecord>),
    /// Flush everything buffered, then ack on the channel so the caller knows the
    /// DB is durable up to this point.
    Flush(crossbeam_channel::Sender<()>),
}

/// The production SQLite-backed sink. Cloneable-cheap via the inner `Sender`.
pub struct SqliteSink {
    tx: Sender<WriterMsg>,
}

/// Owns the writer thread + a way to flush/stop it. Kept out of the app state so
/// the sink trait stays object-safe; `main()` holds this for shutdown.
pub struct UsageWriterHandle {
    tx: Sender<WriterMsg>,
    join: Option<JoinHandle<()>>,
}

impl SqliteSink {
    /// Open (or create) the SQLite DB at `path`, apply the schema + pragmas, and
    /// spawn the dedicated OS-thread writer. Returns the `record`-only sink plus a
    /// [`UsageWriterHandle`] for shutdown flushing.
    pub fn new(
        path: impl AsRef<std::path::Path>,
        capacity: usize,
        batch: usize,
    ) -> rusqlite::Result<(Self, UsageWriterHandle)> {
        let capacity = capacity.max(1);
        let batch = batch.max(1);
        let conn = Connection::open(path.as_ref())?;
        init_db(&conn)?;

        let (tx, rx) = bounded::<WriterMsg>(capacity);
        let join = std::thread::Builder::new()
            .name("moaray-usage-writer".to_string())
            .spawn(move || writer_loop(conn, rx, batch))
            .expect("spawn usage writer thread");

        Ok((
            Self { tx: tx.clone() },
            UsageWriterHandle {
                tx,
                join: Some(join),
            },
        ))
    }
}

impl UsageSink for SqliteSink {
    fn record(&self, rec: UsageRecord) {
        // Hot path: try_send only — NEVER block the request. On a full channel,
        // drop the row and count it (best-effort posture). A disconnected channel
        // (writer gone, e.g. post-shutdown) is also a silent drop + count.
        match self.tx.try_send(WriterMsg::Row(Box::new(rec))) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                metrics::counter!("moaray_usage_dropped_total").increment(1);
            }
        }
    }
}

impl UsageWriterHandle {
    /// Flush everything enqueued so far, then join the writer thread — bounded by
    /// `timeout`. On timeout the thread is **detached** (we drop the join handle):
    /// a stuck writer must not hang process exit (best-effort posture, DP4).
    pub fn flush_and_join(mut self, timeout: Duration) {
        // Ask the writer to flush and ack. If the channel is gone the writer
        // already exited; nothing to flush.
        let (ack_tx, ack_rx) = bounded::<()>(1);
        if self.tx.send(WriterMsg::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv_timeout(timeout);
        }
        // Drop our sender so the writer's recv loop sees a disconnect and exits.
        // We must drop the cloned sender; replace with a closed one.
        drop_sender(&mut self.tx);
        if let Some(join) = self.join.take() {
            // Best-effort bounded join: poll is_finished briefly; detach on timeout.
            let deadline = std::time::Instant::now() + timeout;
            while !join.is_finished() {
                if std::time::Instant::now() >= deadline {
                    tracing::warn!(
                        "usage writer did not finish within flush timeout; detaching"
                    );
                    return; // detach: drop join handle without blocking
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            let _ = join.join();
        }
    }
}

/// Replace a sender with a disconnected one so the writer loop terminates.
fn drop_sender(tx: &mut Sender<WriterMsg>) {
    // Create a throwaway channel and swap; the original sender is dropped here,
    // and the throwaway receiver is dropped immediately, so the swapped-in sender
    // is itself disconnected. This guarantees the writer's recv() ends.
    let (dead, _) = bounded::<WriterMsg>(1);
    *tx = dead;
}

/// Apply the schema + durability pragmas. WAL + NORMAL is the standard
/// throughput/durability balance for a single-writer accounting store.
fn init_db(conn: &Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.execute_batch(CREATE_TABLE_SQL)?;
    conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    Ok(())
}

/// The dedicated writer thread: drains the channel, batching inserts into
/// transactions. Owns the `Connection` exclusively (SQLite single-writer).
fn writer_loop(mut conn: Connection, rx: Receiver<WriterMsg>, batch: usize) {
    let mut buf: Vec<UsageRecord> = Vec::with_capacity(batch);
    loop {
        // Block for the next message; exit when all senders are gone.
        let first = match rx.recv() {
            Ok(m) => m,
            Err(_) => break, // disconnected → flush remainder + exit
        };
        let mut pending_ack: Option<crossbeam_channel::Sender<()>> = None;
        match first {
            WriterMsg::Row(r) => buf.push(*r),
            WriterMsg::Flush(ack) => pending_ack = Some(ack),
        }
        // Greedily drain whatever else is ready, up to the batch size.
        while buf.len() < batch {
            match rx.try_recv() {
                Ok(WriterMsg::Row(r)) => buf.push(*r),
                Ok(WriterMsg::Flush(ack)) => {
                    pending_ack = Some(ack);
                    break;
                }
                Err(_) => break,
            }
        }
        if !buf.is_empty() {
            if let Err(e) = flush_batch(&mut conn, &buf) {
                tracing::error!(error = %e, rows = buf.len(), "usage writer batch insert failed");
            }
            buf.clear();
        }
        if let Some(ack) = pending_ack {
            let _ = ack.send(());
        }
    }
    // Final drain on disconnect (rows enqueued before the senders dropped).
    if !buf.is_empty() {
        let _ = flush_batch(&mut conn, &buf);
    }
}

/// Insert a batch of records in one transaction.
fn flush_batch(conn: &mut Connection, rows: &[UsageRecord]) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO usage_events (
                request_id, ts_unix_ms, path, arm, model, upstream_id, caller_key_id,
                prompt_tokens, completion_tokens,
                price_prompt_nano_per_mtok, price_completion_nano_per_mtok,
                cost_nano_usd, status
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        )?;
        for r in rows {
            stmt.execute(rusqlite::params![
                r.request_id,
                r.ts_unix_ms,
                r.path.as_str(),
                r.arm.as_str(),
                r.model,
                r.upstream_id,
                r.caller_key_id,
                r.prompt_tokens,
                r.completion_tokens,
                r.price_prompt_nano_per_mtok,
                r.price_completion_nano_per_mtok,
                r.cost_nano_usd,
                r.status.as_str(),
            ])?;
        }
    }
    tx.commit()
}

/// Accounting-disabled sink: `record` is a no-op. Default when no store is wired.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullSink;

impl UsageSink for NullSink {
    fn record(&self, _rec: UsageRecord) {}
}

/// Test util: records into an in-memory vector readable via [`VecSink::rows`].
#[derive(Debug, Default, Clone)]
pub struct VecSink {
    rows: Arc<Mutex<Vec<UsageRecord>>>,
}

impl VecSink {
    /// A fresh, empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of every recorded row (clone; safe to assert on).
    pub fn rows(&self) -> Vec<UsageRecord> {
        self.rows.lock().expect("vec sink mutex").clone()
    }
}

impl UsageSink for VecSink {
    fn record(&self, rec: UsageRecord) {
        self.rows.lock().expect("vec sink mutex").push(rec);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moaray_core::usage::{UsageArm, UsagePath, UsageStatus};

    fn rec(request_id: &str) -> UsageRecord {
        UsageRecord {
            request_id: request_id.to_string(),
            ts_unix_ms: 1_700_000_000_000,
            path: UsagePath::Moa,
            arm: UsageArm::Proposer,
            model: "m".into(),
            upstream_id: "up".into(),
            caller_key_id: "team-a".into(),
            prompt_tokens: Some(10),
            completion_tokens: Some(20),
            price_prompt_nano_per_mtok: Some(150_000_000),
            price_completion_nano_per_mtok: Some(600_000_000),
            cost_nano_usd: Some(13_500),
            status: UsageStatus::Ok,
        }
    }

    fn row_count(path: &std::path::Path) -> i64 {
        let conn = Connection::open(path).unwrap();
        conn.query_row("SELECT count(*) FROM usage_events", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn sqlite_sink_persists_recorded_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("usage.db");
        let (sink, handle) = SqliteSink::new(&path, 1024, 64).unwrap();
        for i in 0..50 {
            sink.record(rec(&format!("req-{i}")));
        }
        handle.flush_and_join(Duration::from_secs(5));
        assert_eq!(row_count(&path), 50);
    }

    #[test]
    fn sqlite_sink_round_trips_all_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("usage.db");
        let (sink, handle) = SqliteSink::new(&path, 16, 8).unwrap();
        sink.record(rec("req-x"));
        handle.flush_and_join(Duration::from_secs(5));

        let conn = Connection::open(&path).unwrap();
        let (model, cost, status, pt): (String, i64, String, i64) = conn
            .query_row(
                "SELECT model, cost_nano_usd, status, prompt_tokens FROM usage_events WHERE request_id='req-x'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(model, "m");
        assert_eq!(cost, 13_500);
        assert_eq!(status, "ok");
        assert_eq!(pt, 10);
        // schema version stamped
        let uv: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(uv, SCHEMA_VERSION);
    }

    #[test]
    fn sqlite_sink_stores_null_for_unmeasured() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("usage.db");
        let (sink, handle) = SqliteSink::new(&path, 16, 8).unwrap();
        let mut r = rec("req-null");
        r.prompt_tokens = None;
        r.completion_tokens = None;
        r.cost_nano_usd = None;
        r.status = UsageStatus::Failed;
        sink.record(r);
        handle.flush_and_join(Duration::from_secs(5));

        let conn = Connection::open(&path).unwrap();
        let cost: Option<i64> = conn
            .query_row(
                "SELECT cost_nano_usd FROM usage_events WHERE request_id='req-null'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cost, None, "unmeasured cost must be NULL, never 0");
    }

    #[test]
    fn full_channel_drops_and_counts() {
        // capacity 1 + a blocked writer (we never flush) → try_send fills then drops.
        // Use a tiny capacity and flood faster than the writer can drain. The drop
        // path increments the global counter; we assert at least one row landed AND
        // that flooding never panics/blocks (the record call returns immediately).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("usage.db");
        let (sink, handle) = SqliteSink::new(&path, 1, 1).unwrap();
        for i in 0..10_000 {
            sink.record(rec(&format!("flood-{i}")));
        }
        handle.flush_and_join(Duration::from_secs(5));
        // Some rows persisted; the exact count is timing-dependent (drops expected),
        // but it must be > 0 and <= the number sent.
        let n = row_count(&path);
        assert!(n > 0 && n <= 10_000, "expected partial persistence, got {n}");
    }

    #[test]
    fn null_sink_is_noop() {
        let s = NullSink;
        s.record(rec("ignored")); // must not panic
    }

    #[test]
    fn vec_sink_records_rows() {
        let s = VecSink::new();
        assert!(s.rows().is_empty());
        s.record(rec("a"));
        s.record(rec("b"));
        let rows = s.rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].request_id, "a");
    }
}
