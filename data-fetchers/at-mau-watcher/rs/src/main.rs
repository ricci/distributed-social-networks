use std::collections::HashMap;
use std::fs;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::{io::Cursor};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, TimeZone, Utc};
use futures::StreamExt;
use im::hashset::HashSet;
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize, Serializer};
use serde::ser::SerializeMap;
use serde_ipld_dagcbor as dagcbor;
use tokio_tungstenite::tungstenite::Message;
use tokio::sync::{mpsc, Mutex, Semaphore};

// Atrium imports (adjust if your Atrium API differs).
use atrium_api::com::atproto::sync::subscribe_repos::{Account, Commit, Identity, NSID};

const SNAPSHOT_FILE: &str = "accounts_snapshot.json";
const SNAPSHOT_INTERVAL: u64 = 300;
const RESOLVE_TTL_SECONDS: i64 = 24 * 60 * 60;
const RESOLUTION_QUEUE_SIZE: usize = 10_000;
const RESOLUTION_WORKERS: usize = 10;

#[derive(Clone, Debug)]
struct AccountEntry {
    // Box/Arc reduce per-entry allocation overhead; PDS is interned for sharing.
    did: Box<str>,
    pds: Option<Arc<str>>,
    handle: Option<Box<str>>,
    last_seen: i64,
    last_resolved: Option<i64>,
}

impl PartialEq for AccountEntry {
    fn eq(&self, other: &Self) -> bool {
        self.did == other.did
    }
}

impl Eq for AccountEntry {}

impl Hash for AccountEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.did.hash(state);
    }
}

impl Borrow<str> for AccountEntry {
    fn borrow(&self) -> &str {
        self.did.as_ref()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SnapshotEntry {
    pds: Option<Box<str>>,
    handle: Option<Box<str>>,
    #[serde(with = "iso8601")]
    last_seen: DateTime<Utc>,
    #[serde(with = "iso8601::option")]
    last_resolved: Option<DateTime<Utc>>,
}

#[derive(Default)]
struct AccountStore {
    // Persistent set snapshot for lock-minimized writes.
    accounts: Arc<HashSet<AccountEntry>>,
    // Intern PDS strings to reduce memory when many accounts share the same PDS.
    pds_pool: HashMap<String, Arc<str>>,
}

struct RateStats {
    current_events: AtomicU64,
    current_bytes: AtomicU64,
    events_per_sec: AtomicU64,
    bytes_per_sec: AtomicU64,
    events_per_sec_1m: AtomicU64,
    bytes_per_sec_1m: AtomicU64,
    events_per_sec_1h: AtomicU64,
    bytes_per_sec_1h: AtomicU64,
    events_per_sec_1d: AtomicU64,
    bytes_per_sec_1d: AtomicU64,
}

impl RateStats {
    fn new() -> Self {
        Self {
            current_events: AtomicU64::new(0),
            current_bytes: AtomicU64::new(0),
            events_per_sec: AtomicU64::new(0),
            bytes_per_sec: AtomicU64::new(0),
            events_per_sec_1m: AtomicU64::new(0),
            bytes_per_sec_1m: AtomicU64::new(0),
            events_per_sec_1h: AtomicU64::new(0),
            bytes_per_sec_1h: AtomicU64::new(0),
            events_per_sec_1d: AtomicU64::new(0),
            bytes_per_sec_1d: AtomicU64::new(0),
        }
    }

    fn record(&self, events: u64, bytes: u64) {
        self.current_events.fetch_add(events, Ordering::Relaxed);
        self.current_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64, u64, u64, u64, u64, u64, u64) {
        (
            self.events_per_sec.load(Ordering::Relaxed),
            self.bytes_per_sec.load(Ordering::Relaxed),
            self.events_per_sec_1m.load(Ordering::Relaxed),
            self.bytes_per_sec_1m.load(Ordering::Relaxed),
            self.events_per_sec_1h.load(Ordering::Relaxed),
            self.bytes_per_sec_1h.load(Ordering::Relaxed),
            self.events_per_sec_1d.load(Ordering::Relaxed),
            self.bytes_per_sec_1d.load(Ordering::Relaxed),
        )
    }
}

async fn rate_sampler(stats: Arc<RateStats>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut events_window: std::collections::VecDeque<u64> = std::collections::VecDeque::new();
    let mut bytes_window: std::collections::VecDeque<u64> = std::collections::VecDeque::new();
    let mut sum_events_1m = 0u64;
    let mut sum_bytes_1m = 0u64;
    let mut sum_events_1h = 0u64;
    let mut sum_bytes_1h = 0u64;
    let mut sum_events_1d = 0u64;
    let mut sum_bytes_1d = 0u64;

    loop {
        ticker.tick().await;
        let events = stats.current_events.swap(0, Ordering::Relaxed);
        let bytes = stats.current_bytes.swap(0, Ordering::Relaxed);

        stats.events_per_sec.store(events, Ordering::Relaxed);
        stats.bytes_per_sec.store(bytes, Ordering::Relaxed);

        events_window.push_back(events);
        bytes_window.push_back(bytes);
        sum_events_1m += events;
        sum_bytes_1m += bytes;
        sum_events_1h += events;
        sum_bytes_1h += bytes;
        sum_events_1d += events;
        sum_bytes_1d += bytes;

        if events_window.len() > 60 {
            sum_events_1m -= events_window[events_window.len() - 61];
            sum_bytes_1m -= bytes_window[bytes_window.len() - 61];
        }
        if events_window.len() > 3600 {
            sum_events_1h -= events_window[events_window.len() - 3601];
            sum_bytes_1h -= bytes_window[bytes_window.len() - 3601];
        }
        if events_window.len() > 86400 {
            sum_events_1d -= events_window[0];
            sum_bytes_1d -= bytes_window[0];
            events_window.pop_front();
            bytes_window.pop_front();
        }

        let len_1m = events_window.len().min(60) as u64;
        let len_1h = events_window.len().min(3600) as u64;
        let len_1d = events_window.len().min(86400) as u64;

        stats
            .events_per_sec_1m
            .store(if len_1m > 0 { sum_events_1m / len_1m } else { 0 }, Ordering::Relaxed);
        stats
            .bytes_per_sec_1m
            .store(if len_1m > 0 { sum_bytes_1m / len_1m } else { 0 }, Ordering::Relaxed);
        stats
            .events_per_sec_1h
            .store(if len_1h > 0 { sum_events_1h / len_1h } else { 0 }, Ordering::Relaxed);
        stats
            .bytes_per_sec_1h
            .store(if len_1h > 0 { sum_bytes_1h / len_1h } else { 0 }, Ordering::Relaxed);
        stats
            .events_per_sec_1d
            .store(if len_1d > 0 { sum_events_1d / len_1d } else { 0 }, Ordering::Relaxed);
        stats
            .bytes_per_sec_1d
            .store(if len_1d > 0 { sum_bytes_1d / len_1d } else { 0 }, Ordering::Relaxed);
    }
}

impl AccountStore {
    fn intern_pds(&mut self, value: &str) -> Arc<str> {
        if let Some(existing) = self.pds_pool.get(value) {
            return existing.clone();
        }
        let arc: Arc<str> = Arc::from(value);
        self.pds_pool.insert(value.to_string(), arc.clone());
        arc
    }

    fn upsert(&mut self, entry: AccountEntry) {
        // Copy-on-write update to preserve snapshot sharing.
        let accounts = Arc::make_mut(&mut self.accounts);
        let _ = accounts.remove(entry.did.as_ref());
        accounts.insert(entry);
    }

    fn update_last_seen(
        &mut self,
        did: &str,
        now: i64,
        force_resolve: bool,
        resolve_ttl_seconds: i64,
    ) -> bool {
        let mut need_resolve = false;
        // Copy-on-write update keeps snapshots cheap to clone.
        let accounts = Arc::make_mut(&mut self.accounts);
        let mut entry = accounts.remove(did).unwrap_or_else(|| AccountEntry {
            did: did.to_string().into_boxed_str(),
            pds: None,
            handle: None,
            last_seen: now,
            last_resolved: None,
        });

        entry.last_seen = now;

        if force_resolve {
            need_resolve = true;
        } else if let Some(last_resolved) = entry.last_resolved {
            let age = now - last_resolved;
            if age >= resolve_ttl_seconds {
                need_resolve = true;
            }
        } else {
            need_resolve = true;
        }

        self.upsert(entry);
        need_resolve
    }
}

mod iso8601 {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_rfc3339())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&raw)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }

    pub mod option {
        use chrono::{DateTime, Utc};
        use serde::{Deserialize, Deserializer, Serializer};

        pub fn serialize<S>(value: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match value {
                Some(dt) => serializer.serialize_some(&dt.to_rfc3339()),
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let raw = Option::<String>::deserialize(deserializer)?;
            match raw {
                Some(raw) => DateTime::parse_from_rfc3339(&raw)
                    .map(|dt| Some(dt.with_timezone(&Utc)))
                    .map_err(serde::de::Error::custom),
                None => Ok(None),
            }
        }
    }
}

fn load_snapshot(path: &str) -> HashMap<Box<str>, SnapshotEntry> {
    if !Path::new(path).exists() {
        return HashMap::new();
    }

    let data = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(_) => return HashMap::new(),
    };

    let parsed: HashMap<Box<str>, SnapshotEntry> = match serde_json::from_str(&data) {
        Ok(parsed) => parsed,
        Err(_) => return HashMap::new(),
    };

    parsed
}

fn write_snapshot_file(path: &str, accounts: Arc<HashSet<AccountEntry>>, verbose: bool) {
    let tmp_path = format!("{path}.tmp");

    if let Ok(file) = fs::File::create(&tmp_path) {
        let mut writer = BufWriter::new(file);
        let write_result = (|| {
            // Stream JSON without building a full map in memory.
            let mut serializer = serde_json::Serializer::new(&mut writer);
            let mut map = serializer.serialize_map(Some(accounts.len()))?;
            for entry in accounts.iter() {
                let last_seen = Utc.timestamp_opt(entry.last_seen, 0).single().unwrap_or_else(|| {
                    Utc.timestamp_opt(0, 0).single().unwrap()
                });
                let last_resolved = entry.last_resolved.and_then(|ts| {
                    Utc.timestamp_opt(ts, 0).single()
                });
                let snapshot = SnapshotEntry {
                    pds: entry.pds.as_deref().map(|pds| pds.to_string().into_boxed_str()),
                    handle: entry.handle.clone(),
                    last_seen,
                    last_resolved,
                };
                map.serialize_entry(&entry.did, &snapshot)?;
            }
            map.end()
        })();
        if write_result.is_ok() {
            let _ = writer.flush();
            drop(writer);
            let _ = fs::rename(&tmp_path, path);
            if verbose {
                println!("Saved snapshot to {}: {} accounts", path, accounts.len());
            }
        }
    }
}

async fn save_snapshot(path: &str, state: Arc<Mutex<AccountStore>>, verbose: bool) {
    let snapshot = {
        let guard = state.lock().await;
        guard.accounts.clone()
    };
    let path = path.to_string();

    // Snapshot is written in a dedicated thread to avoid blocking async tasks.
    let handle = std::thread::spawn(move || write_snapshot_file(&path, snapshot, verbose));
    let _ = tokio::task::spawn_blocking(move || {
        let _ = handle.join();
    })
    .await;
}

async fn periodic_snapshot(path: String, state: Arc<Mutex<AccountStore>>, interval: u64) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval));
    loop {
        ticker.tick().await;
        save_snapshot(&path, state.clone(), false).await;
    }
}

async fn resolve_did_document(
    did: &str,
    client: &reqwest::Client,
    timeout_seconds: u64,
) -> Option<serde_json::Value> {
    let url = if did.starts_with("did:plc:") {
        format!("https://plc.directory/{did}")
    } else if did.starts_with("did:web:") {
        let domain = &did["did:web:".len()..];
        format!("https://{domain}/.well-known/did.json")
    } else {
        return None;
    };

    let response = client.get(url).timeout(Duration::from_secs(timeout_seconds)).send().await;
    let response = match response {
        Ok(response) => response,
        Err(_) => return None,
    };

    if !response.status().is_success() {
        return None;
    }

    response.json::<serde_json::Value>().await.ok()
}

fn extract_pds_from_diddoc(doc: &serde_json::Value) -> Option<String> {
    let services = doc.get("service").or_else(|| doc.get("services"))?;
    let services = services.as_array()?;
    for svc in services {
        let svc_id = svc.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let svc_type = svc.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let endpoint = svc
            .get("serviceEndpoint")
            .or_else(|| svc.get("endpoint"))
            .and_then(|v| v.as_str());
        if svc_id.ends_with("#atproto_pds")
            && svc_type == "AtprotoPersonalDataServer"
            && endpoint.is_some()
        {
            return endpoint.map(|s| s.to_string());
        }
    }
    None
}

fn extract_handle_from_diddoc(doc: &serde_json::Value) -> Option<String> {
    let also_known_as = doc.get("alsoKnownAs")?.as_array()?;
    for aka in also_known_as {
        if let Some(value) = aka.as_str() {
            if let Some(handle) = value.strip_prefix("at://") {
                return Some(handle.to_string());
            }
        }
    }
    None
}

async fn resolve_if_needed(
    did: String,
    state: Arc<Mutex<AccountStore>>,
    client: reqwest::Client,
    force_resolve: bool,
    resolve_ttl_seconds: i64,
) {
    let now = Utc::now().timestamp();
    {
        let mut guard = state.lock().await;
        // Update bookkeeping before doing network I/O.
        let accounts = Arc::make_mut(&mut guard.accounts);
        let mut entry = match accounts.remove(did.as_str()) {
            Some(entry) => entry,
            None => return,
        };

        let mut need_resolve = false;
        if force_resolve {
            need_resolve = true;
        } else if let Some(last_resolved) = entry.last_resolved {
            let age = now - last_resolved;
            if age >= resolve_ttl_seconds {
                need_resolve = true;
            }
        } else {
            need_resolve = true;
        }

        if !need_resolve {
            guard.upsert(entry);
            return;
        }

        entry.last_resolved = Some(now);
        guard.upsert(entry);
    }

    let doc = resolve_did_document(&did, &client, 5).await;
    let doc = match doc {
        Some(doc) => doc,
        None => return,
    };

    let pds = extract_pds_from_diddoc(&doc);
    let handle = extract_handle_from_diddoc(&doc);

    let mut guard = state.lock().await;
    // Apply resolved values under lock.
    let accounts = Arc::make_mut(&mut guard.accounts);
    let mut entry = match accounts.remove(did.as_str()) {
        Some(entry) => entry,
        None => return,
    };

    if let Some(pds) = pds {
        entry.pds = Some(guard.intern_pds(&pds));
    }
    if let Some(handle) = handle {
        entry.handle = Some(handle.into_boxed_str());
    }

    guard.upsert(entry);
}

async fn resolution_dispatcher(
    mut resolve_rx: mpsc::Receiver<(String, bool)>,
    state: Arc<Mutex<AccountStore>>,
    client: reqwest::Client,
    resolve_ttl_seconds: i64,
    workers: usize,
    resolve_processed: Arc<AtomicU64>,
) {
    let semaphore = Arc::new(Semaphore::new(workers));
    while let Some((did, force)) = resolve_rx.recv().await {
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => continue,
        };
        let state = state.clone();
        let client = client.clone();
        let resolve_processed = resolve_processed.clone();
        tokio::spawn(async move {
            let _permit = permit;
            resolve_if_needed(did, state, client, force, resolve_ttl_seconds).await;
            resolve_processed.fetch_add(1, Ordering::Relaxed);
        });
    }
}

async fn account_update_worker(
    mut update_rx: mpsc::UnboundedReceiver<(String, bool)>,
    resolve_tx: mpsc::Sender<(String, bool)>,
    state: Arc<Mutex<AccountStore>>,
    resolve_ttl_seconds: i64,
    update_processed: Arc<AtomicU64>,
    resolve_enqueued: Arc<AtomicU64>,
    resolve_dropped: Arc<AtomicU64>,
) {
    while let Some((did, force)) = update_rx.recv().await {
        update_processed.fetch_add(1, Ordering::Relaxed);
        let now = Utc::now().timestamp();
        let need_resolve = {
            let mut guard = state.lock().await;
            guard.update_last_seen(&did, now, force, resolve_ttl_seconds)
        };

        if need_resolve {
            if resolve_tx.try_send((did, force)).is_ok() {
                resolve_enqueued.fetch_add(1, Ordering::Relaxed);
            } else {
                resolve_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn run_firehose(
    relay: Option<String>,
    update_tx: mpsc::UnboundedSender<(String, bool)>,
    update_enqueued: Arc<AtomicU64>,
    update_processed: Arc<AtomicU64>,
    resolve_enqueued: Arc<AtomicU64>,
    resolve_processed: Arc<AtomicU64>,
    resolve_dropped: Arc<AtomicU64>,
    rate_stats: Arc<RateStats>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let base = relay.unwrap_or_else(|| "wss://bsky.network/xrpc".to_string());
    if base.contains(NSID) {
        println!(
            "Warning: relay already includes subscribeRepos; using as-is: {}",
            base
        );
    }
    let url = if base.contains(NSID) {
        base
    } else {
        format!("{}/{}", base.trim_end_matches('/'), NSID)
    };
    println!("Connecting to firehose: {}", url);
    let (ws_stream, _) = tokio_tungstenite::connect_async(url).await?;

    let (_, mut stream) = ws_stream.split();
    let mut total_messages: u64 = 0;
    let mut binary_messages: u64 = 0;
    let mut text_messages: u64 = 0;
    let mut close_messages: u64 = 0;
    let mut other_messages: u64 = 0;
    let mut frame_parse_failures: u64 = 0;
    let mut body_parse_failures: u64 = 0;
    let mut commit_events: u64 = 0;
    let mut account_events: u64 = 0;
    let mut identity_events: u64 = 0;
    let mut error_frames: u64 = 0;
    let mut other_events: u64 = 0;
    let mut enqueued_updates: u64 = 0;

    while let Some(message) = stream.next().await {
        let message = message?;
        total_messages += 1;
        let bytes = match message {
            Message::Binary(bytes) => {
                binary_messages += 1;
                rate_stats.record(1, bytes.len() as u64);
                bytes
            }
            Message::Text(text) => {
                text_messages += 1;
                rate_stats.record(1, text.len() as u64);
                text.into_bytes()
            }
            Message::Close(_) => {
                close_messages += 1;
                break;
            }
            _ => {
                other_messages += 1;
                continue;
            }
        };

        // Firehose frames are CBOR header + CBOR body (per atrium firehose example).
        let frame = match decode_firehose_frame(&bytes) {
            Ok(frame) => frame,
            Err(err) => {
                frame_parse_failures += 1;
                if frame_parse_failures <= 5 {
                    let prefix_len = bytes.len().min(16);
                    let prefix = bytes[..prefix_len]
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    println!(
                        "Firehose frame parse failed ({} bytes, prefix: {}): {}",
                        bytes.len(),
                        prefix,
                        err
                    );
                }
                continue;
            }
        };
        match frame {
            FirehoseFrame::Message(t, body_start) => {
                let mut did = None;
                let mut force_resolve = false;
                let body = &bytes[body_start..];

                match t.as_deref() {
                    Some("#commit") => match dagcbor::from_reader::<Commit, _>(body) {
                        Ok(commit) => {
                            commit_events += 1;
                            did = Some(commit.repo.as_str().to_string());
                        }
                        Err(err) => {
                            body_parse_failures += 1;
                            if body_parse_failures <= 5 {
                                println!("Commit decode failed: {err}");
                            }
                        }
                    },
                    Some("#account") => match dagcbor::from_reader::<Account, _>(body) {
                        Ok(account) => {
                            account_events += 1;
                            did = Some(account.did.as_str().to_string());
                            force_resolve = true;
                        }
                        Err(err) => {
                            body_parse_failures += 1;
                            if body_parse_failures <= 5 {
                                println!("Account decode failed: {err}");
                            }
                        }
                    },
                    Some("#identity") => {
                        match dagcbor::from_reader::<Identity, _>(body) {
                            Ok(identity) => {
                                identity_events += 1;
                                did = Some(identity.did.as_str().to_string());
                                force_resolve = true;
                            }
                            Err(err) => {
                                body_parse_failures += 1;
                                if body_parse_failures <= 5 {
                                    println!("Identity decode failed: {err}");
                                }
                            }
                        }
                    }
                    _ => {
                        other_events += 1;
                    }
                }

                if let Some(did) = did {
                    let _ = update_tx.send((did, force_resolve));
                    update_enqueued.fetch_add(1, Ordering::Relaxed);
                    enqueued_updates += 1;
                }
            }
            FirehoseFrame::Error => {
                error_frames += 1;
            }
        }

        if total_messages % 1000 == 0 {
            let (eps, bps, eps_1m, bps_1m, eps_1h, bps_1h, eps_1d, bps_1d) =
                rate_stats.snapshot();
            let updates_backlog = update_enqueued.load(Ordering::Relaxed)
                .saturating_sub(update_processed.load(Ordering::Relaxed));
            let resolves_backlog = resolve_enqueued.load(Ordering::Relaxed)
                .saturating_sub(resolve_processed.load(Ordering::Relaxed));
            println!(
                "Firehose stats: total={}, binary={}, text={}, close={}, other={}, frame_failures={}, body_failures={}, commit={}, account={}, identity={}, other_events={}, error_frames={}, enqueued={}, updates_backlog={}, resolves_backlog={}, resolve_dropped={}, eps={}, bps={}, eps_1m={}, bps_1m={}, eps_1h={}, bps_1h={}, eps_1d={}, bps_1d={}",
                total_messages,
                binary_messages,
                text_messages,
                close_messages,
                other_messages,
                frame_parse_failures,
                body_parse_failures,
                commit_events,
                account_events,
                identity_events,
                other_events,
                error_frames,
                enqueued_updates,
                updates_backlog,
                resolves_backlog,
                resolve_dropped.load(Ordering::Relaxed),
                eps,
                bps,
                eps_1m,
                bps_1m,
                eps_1h,
                bps_1h,
                eps_1d,
                bps_1d
            );
        }
    }

    println!(
        "Firehose final: total={}, binary={}, text={}, close={}, other={}, frame_failures={}, body_failures={}, commit={}, account={}, identity={}, other_events={}, error_frames={}, enqueued={}, updates_backlog={}, resolves_backlog={}, resolve_dropped={}",
        total_messages,
        binary_messages,
        text_messages,
        close_messages,
        other_messages,
        frame_parse_failures,
        body_parse_failures,
        commit_events,
        account_events,
        identity_events,
        other_events,
        error_frames,
        enqueued_updates,
        update_enqueued.load(Ordering::Relaxed)
            .saturating_sub(update_processed.load(Ordering::Relaxed)),
        resolve_enqueued
            .load(Ordering::Relaxed)
            .saturating_sub(resolve_processed.load(Ordering::Relaxed)),
        resolve_dropped.load(Ordering::Relaxed)
    );

    Ok(())
}

#[derive(Debug)]
enum FirehoseFrame {
    Message(Option<String>, usize),
    Error,
}

#[derive(Debug)]
enum FrameHeader {
    Message(Option<String>),
    Error,
}

impl TryFrom<Ipld> for FrameHeader {
    type Error = String;

    fn try_from(value: Ipld) -> Result<Self, String> {
        if let Ipld::Map(map) = value {
            if let Some(Ipld::Integer(op)) = map.get("op") {
                match op {
                    1 => {
                        let t = if let Some(Ipld::String(t)) = map.get("t") {
                            Some(t.clone())
                        } else {
                            None
                        };
                        return Ok(FrameHeader::Message(t));
                    }
                    -1 => return Ok(FrameHeader::Error),
                    _ => {}
                }
            }
        }
        Err("invalid frame header".to_string())
    }
}

fn decode_firehose_frame(bytes: &[u8]) -> Result<FirehoseFrame, String> {
    let mut cursor = Cursor::new(bytes);
    match dagcbor::from_reader::<Ipld, _>(&mut cursor) {
        Err(dagcbor::DecodeError::TrailingData) => {
            let split = cursor.position() as usize;
            let (left, _right) = bytes.split_at(split);
            // Header is an IPLD map with `op` and optional `t`.
            let header_ipld =
                dagcbor::from_slice::<Ipld>(left).map_err(|err| err.to_string())?;
            let header = FrameHeader::try_from(header_ipld)?;
            match header {
                FrameHeader::Message(t) => Ok(FirehoseFrame::Message(t, split)),
                FrameHeader::Error => Ok(FirehoseFrame::Error),
            }
        }
        Err(err) => Err(err.to_string()),
        Ok(_) => Err("missing frame body".to_string()),
    }
}

#[derive(Clone)]
struct Args {
    relay: Option<String>,
    snapshot_read_file: Option<String>,
    snapshot_file: String,
    snapshot_interval: u64,
    resolve_workers: usize,
    resolve_ttl_seconds: i64,
}

fn parse_args() -> Args {
    let mut relay = None;
    let mut snapshot_read_file = None;
    let mut snapshot_file = SNAPSHOT_FILE.to_string();
    let mut snapshot_interval = SNAPSHOT_INTERVAL;
    let mut resolve_workers = RESOLUTION_WORKERS;
    let mut resolve_ttl_seconds = RESOLVE_TTL_SECONDS;

    let mut args = std::env::args().skip(1).peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!(
                    "Usage: at-mau-watcher [options]\n\
                     \n\
                     --relay <url>                 Relay base URL, e.g. wss://bsky.network/xrpc\n\
                     --snapshot-file <path>        Output snapshot JSON path (default: {default_snapshot})\n\
                     --snapshot-read-file <path>   Input snapshot JSON path (defaults to --snapshot-file)\n\
                     --snapshot-interval <seconds> Seconds between periodic snapshots (default: {default_interval})\n\
                     --resolve-workers <n>          Number of DID resolution workers (default: {default_workers})\n\
                     --resolve-ttl-seconds <n>      Re-resolve DIDs older than this many seconds (default: {default_ttl})\n",
                    default_snapshot = SNAPSHOT_FILE,
                    default_interval = SNAPSHOT_INTERVAL,
                    default_workers = RESOLUTION_WORKERS,
                    default_ttl = RESOLVE_TTL_SECONDS
                );
                std::process::exit(0);
            }
            "--relay" => {
                if let Some(value) = args.next() {
                    relay = Some(value);
                }
            }
            "--snapshot-file" => {
                if let Some(value) = args.next() {
                    snapshot_file = value;
                }
            }
            "--snapshot-read-file" => {
                if let Some(value) = args.next() {
                    snapshot_read_file = Some(value);
                }
            }
            "--snapshot-interval" => {
                if let Some(value) = args.next() {
                    if let Ok(parsed) = value.parse::<u64>() {
                        snapshot_interval = parsed;
                    }
                }
            }
            "--resolve-workers" => {
                if let Some(value) = args.next() {
                    if let Ok(parsed) = value.parse::<usize>() {
                        resolve_workers = parsed;
                    }
                }
            }
            "--resolve-ttl-seconds" => {
                if let Some(value) = args.next() {
                    if let Ok(parsed) = value.parse::<i64>() {
                        resolve_ttl_seconds = parsed;
                    }
                }
            }
            _ => {}
        }
    }

    Args {
        relay,
        snapshot_read_file,
        snapshot_file,
        snapshot_interval,
        resolve_workers,
        resolve_ttl_seconds,
    }
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    let snapshot_read_path = args
        .snapshot_read_file
        .as_deref()
        .unwrap_or(&args.snapshot_file);
    let snapshot = load_snapshot(snapshot_read_path);
    if !snapshot.is_empty() {
        println!(
            "Loaded snapshot from {}: {} accounts",
            snapshot_read_path,
            snapshot.len()
        );
    }

    let mut store = AccountStore {
        accounts: Arc::new(HashSet::new()),
        pds_pool: HashMap::new(),
    };
    let mut accounts = HashSet::new();
    for (did, entry) in snapshot {
        let last_seen = entry.last_seen.timestamp();
        let last_resolved = entry.last_resolved.map(|dt| dt.timestamp());
        let pds = entry.pds.as_deref().map(|value| store.intern_pds(value));
        accounts.insert(AccountEntry {
            did,
            pds,
            handle: entry.handle,
            last_seen,
            last_resolved,
        });
    }
    store.accounts = Arc::new(accounts);
    let state = Arc::new(Mutex::new(store));

    let update_enqueued = Arc::new(AtomicU64::new(0));
    let update_processed = Arc::new(AtomicU64::new(0));
    let resolve_enqueued = Arc::new(AtomicU64::new(0));
    let resolve_processed = Arc::new(AtomicU64::new(0));
    let resolve_dropped = Arc::new(AtomicU64::new(0));
    let rate_stats = Arc::new(RateStats::new());

    let (resolve_tx, resolve_rx) = mpsc::channel(RESOLUTION_QUEUE_SIZE);
    let (update_tx, update_rx) = mpsc::unbounded_channel();

    let client = reqwest::Client::new();

    tokio::spawn(resolution_dispatcher(
        resolve_rx,
        state.clone(),
        client.clone(),
        args.resolve_ttl_seconds,
        args.resolve_workers,
        resolve_processed.clone(),
    ));

    tokio::spawn(account_update_worker(
        update_rx,
        resolve_tx.clone(),
        state.clone(),
        args.resolve_ttl_seconds,
        update_processed.clone(),
        resolve_enqueued.clone(),
        resolve_dropped.clone(),
    ));

    let snapshot_path = args.snapshot_file.clone();
    tokio::spawn(periodic_snapshot(
        snapshot_path,
        state.clone(),
        args.snapshot_interval,
    ));
    tokio::spawn(rate_sampler(rate_stats.clone()));

    #[cfg(unix)]
    {
        let snapshot_path = args.snapshot_file.clone();
        let state = state.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut sig) = signal(SignalKind::user_defined1()) {
                while sig.recv().await.is_some() {
                    println!("Received SIGUSR1, scheduling snapshot to {}", snapshot_path);
                    save_snapshot(&snapshot_path, state.clone(), true).await;
                }
            }
        });
    }
    {
        let snapshot_path = args.snapshot_file.clone();
        let state = state.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                println!("Received Ctrl+C, saving snapshot to {}", snapshot_path);
                save_snapshot(&snapshot_path, state.clone(), true).await;
                std::process::exit(0);
            }
        });
    }

    let mut reconnect_delay = 1u64;
    let max_reconnect_delay = 60u64;

    loop {
        if let Some(relay) = args.relay.clone() {
            println!("Using relay: {}", relay);
        } else {
            println!("Using default relay: wss://bsky.network/xrpc");
        }

        let relay = args.relay.clone();
        let update_tx = update_tx.clone();
        let firehose = run_firehose(
            relay,
            update_tx,
            update_enqueued.clone(),
            update_processed.clone(),
            resolve_enqueued.clone(),
            resolve_processed.clone(),
            resolve_dropped.clone(),
            rate_stats.clone(),
        )
        .await;

        if let Err(err) = firehose {
            println!("Firehose client error: {err}; reconnecting in {reconnect_delay}s");
        } else {
            println!("Firehose client stopped; reconnecting...");
        }

        save_snapshot(&args.snapshot_file, state.clone(), true).await;
        tokio::time::sleep(Duration::from_secs(reconnect_delay)).await;
        reconnect_delay = std::cmp::min(reconnect_delay * 2, max_reconnect_delay);
    }
}
