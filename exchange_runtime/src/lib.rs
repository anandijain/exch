use exchange_core::{
    BookEvent, BookSnapshot, InstrumentId, Level, NewOrder, Price, Side, Venue, VenueConfig,
    VenueError,
};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug)]
pub struct ExchangeRuntime {
    venue: Mutex<Venue>,
    feed_log: Mutex<PublicFeedLog>,
}

impl ExchangeRuntime {
    pub fn new(venue: Venue, feed_log: PublicFeedLog) -> Self {
        Self {
            venue: Mutex::new(venue),
            feed_log: Mutex::new(feed_log),
        }
    }

    pub fn submit_limit(
        &self,
        instrument_id: InstrumentId,
        order: NewOrder,
    ) -> Result<RuntimeCommandResult, RuntimeError> {
        let events = self
            .venue
            .lock()
            .expect("venue mutex poisoned")
            .submit_limit(instrument_id, order)?;
        self.record_public_events(instrument_id, events)
    }

    pub fn replace_limit(
        &self,
        instrument_id: InstrumentId,
        old_order_id: u64,
        new_order: NewOrder,
    ) -> Result<RuntimeCommandResult, RuntimeError> {
        let events = self
            .venue
            .lock()
            .expect("venue mutex poisoned")
            .replace_limit(instrument_id, old_order_id, new_order)?;
        self.record_public_events(instrument_id, events)
    }

    pub fn cancel(
        &self,
        instrument_id: InstrumentId,
        order_id: u64,
    ) -> Result<RuntimeCommandResult, RuntimeError> {
        let event = self
            .venue
            .lock()
            .expect("venue mutex poisoned")
            .cancel(instrument_id, order_id)?;
        self.record_public_events(instrument_id, vec![event])
    }

    pub fn snapshot(
        &self,
        instrument_id: InstrumentId,
        depth: usize,
    ) -> Result<BookSnapshot, VenueError> {
        self.venue
            .lock()
            .expect("venue mutex poisoned")
            .snapshot(instrument_id, depth)
    }

    pub fn config(&self) -> VenueConfig {
        self.venue
            .lock()
            .expect("venue mutex poisoned")
            .config()
            .clone()
    }

    pub fn balance(&self, account_id: u64, asset: &str) -> u128 {
        self.venue
            .lock()
            .expect("venue mutex poisoned")
            .balance(account_id, asset)
    }

    pub fn reserved(&self, account_id: u64, asset: &str) -> u128 {
        self.venue
            .lock()
            .expect("venue mutex poisoned")
            .reserved(account_id, asset)
    }

    pub fn revenue(&self, asset: &str) -> u128 {
        self.venue
            .lock()
            .expect("venue mutex poisoned")
            .revenue(asset)
    }

    pub fn replay(&self, instrument_id: InstrumentId, after_seq: u64) -> Vec<PublicFeedEntry> {
        self.feed_log
            .lock()
            .expect("feed log mutex poisoned")
            .replay(instrument_id, after_seq)
    }

    fn record_public_events(
        &self,
        instrument_id: InstrumentId,
        book_events: Vec<BookEvent>,
    ) -> Result<RuntimeCommandResult, RuntimeError> {
        let feed_entries = self
            .feed_log
            .lock()
            .expect("feed log mutex poisoned")
            .record(instrument_id, &book_events)?;
        Ok(RuntimeCommandResult {
            book_events,
            feed_entries,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeCommandResult {
    pub book_events: Vec<BookEvent>,
    pub feed_entries: Vec<PublicFeedEntry>,
}

#[derive(Debug)]
pub enum RuntimeError {
    Venue(VenueError),
    FeedLog(std::io::Error),
}

impl From<VenueError> for RuntimeError {
    fn from(error: VenueError) -> Self {
        Self::Venue(error)
    }
}

impl From<std::io::Error> for RuntimeError {
    fn from(error: std::io::Error) -> Self {
        Self::FeedLog(error)
    }
}

#[derive(Debug)]
pub struct PublicFeedLog {
    entries: BTreeMap<InstrumentId, Vec<PublicFeedEntry>>,
    writer: Option<File>,
}

impl PublicFeedLog {
    pub fn in_memory() -> Self {
        Self {
            entries: BTreeMap::new(),
            writer: None,
        }
    }

    pub fn durable(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let entries = read_existing_entries(path)?;
        let writer = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            entries,
            writer: Some(writer),
        })
    }

    pub fn from_env() -> std::io::Result<Self> {
        match std::env::var("EXCH_FEED_LOG_PATH") {
            Ok(path) if !path.trim().is_empty() => Self::durable(PathBuf::from(path)),
            _ => Ok(Self::in_memory()),
        }
    }

    pub fn record(
        &mut self,
        instrument_id: InstrumentId,
        events: &[BookEvent],
    ) -> std::io::Result<Vec<PublicFeedEntry>> {
        let entries = events
            .iter()
            .filter(|event| is_public_event(event))
            .map(|event| PublicFeedEntry {
                instrument_id,
                seq: event_seq(event),
                message: format!("event instrument={instrument_id} {}", event_line(event)),
            })
            .collect::<Vec<_>>();

        if entries.is_empty() {
            return Ok(entries);
        }

        if let Some(writer) = &mut self.writer {
            for entry in &entries {
                writeln!(
                    writer,
                    "{}\t{}\t{}",
                    entry.instrument_id, entry.seq, entry.message
                )?;
            }
            writer.flush()?;
        }

        self.entries
            .entry(instrument_id)
            .or_default()
            .extend(entries.clone());
        Ok(entries)
    }

    pub fn replay(&self, instrument_id: InstrumentId, after_seq: u64) -> Vec<PublicFeedEntry> {
        self.entries
            .get(&instrument_id)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| entry.seq > after_seq)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicFeedEntry {
    pub instrument_id: InstrumentId,
    pub seq: u64,
    pub message: String,
}

fn read_existing_entries(
    path: &Path,
) -> std::io::Result<BTreeMap<InstrumentId, Vec<PublicFeedEntry>>> {
    let mut entries = BTreeMap::new();
    if !path.exists() {
        return Ok(entries);
    }

    let file = File::open(path)?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        let mut parts = line.splitn(3, '\t');
        let Some(instrument_id) = parts.next().and_then(|value| value.parse().ok()) else {
            continue;
        };
        let Some(seq) = parts.next().and_then(|value| value.parse().ok()) else {
            continue;
        };
        let Some(message) = parts.next() else {
            continue;
        };
        entries
            .entry(instrument_id)
            .or_insert_with(Vec::new)
            .push(PublicFeedEntry {
                instrument_id,
                seq,
                message: message.to_string(),
            });
    }

    Ok(entries)
}

pub fn levels(levels: &[Level]) -> String {
    if levels.is_empty() {
        return "-".to_string();
    }

    levels
        .iter()
        .map(|level| format!("{}@{}", level.quantity, level.price.0))
        .collect::<Vec<_>>()
        .join(",")
}

pub fn is_public_event(event: &BookEvent) -> bool {
    !matches!(
        event,
        BookEvent::Accepted { .. } | BookEvent::Rejected { .. }
    )
}

pub fn event_seq(event: &BookEvent) -> u64 {
    match event {
        BookEvent::Accepted { seq, .. }
        | BookEvent::Executed { seq, .. }
        | BookEvent::Rested { seq, .. }
        | BookEvent::Canceled { seq, .. }
        | BookEvent::Rejected { seq, .. } => *seq,
    }
}

pub fn private_event(event: &BookEvent) -> String {
    match event {
        BookEvent::Accepted { seq, order_id } => format!("accepted:{seq}:order={order_id}"),
        BookEvent::Rejected {
            seq,
            order_id,
            reason,
        } => format!("rejected:{seq}:order={order_id}:reason={reason:?}"),
        event => event_line(event),
    }
}

pub fn event_line(event: &BookEvent) -> String {
    match event {
        BookEvent::Accepted { seq, order_id } => format!("accepted:{seq}:order={order_id}"),
        BookEvent::Executed { seq, execution } => format!(
            "executed:{seq}:resting={}:aggressing={}:qty={}:price={}",
            execution.resting_order_id,
            execution.aggressing_order_id,
            execution.quantity,
            execution.price.0
        ),
        BookEvent::Rested {
            seq,
            order_id,
            side,
            price,
            quantity,
        } => format!(
            "rested:{seq}:order={order_id}:side={}:qty={quantity}:price={}",
            side_name(*side),
            price.0
        ),
        BookEvent::Canceled {
            seq,
            order_id,
            side,
            price,
            quantity,
        } => format!(
            "canceled:{seq}:order={order_id}:side={}:qty={quantity}:price={}",
            side_name(*side),
            price.0
        ),
        BookEvent::Rejected {
            seq,
            order_id,
            reason,
        } => format!("rejected:{seq}:order={order_id}:reason={reason:?}"),
    }
}

pub fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

pub fn price(value: u64) -> Price {
    Price(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange_core::VenueConfig;

    #[test]
    fn durable_feed_log_replays_after_restart() {
        let path =
            std::env::temp_dir().join(format!("exch-feed-log-test-{}.log", std::process::id()));
        let _ = fs::remove_file(&path);

        let mut venue = Venue::new(VenueConfig::star("test", "USD", ["AAA"]));
        venue.credit(1, "AAA", 10);
        let runtime = ExchangeRuntime::new(venue, PublicFeedLog::durable(&path).unwrap());
        let result = runtime
            .submit_limit(
                0,
                NewOrder {
                    order_id: 1,
                    account_id: 1,
                    side: Side::Sell,
                    price: Price(10),
                    quantity: 5,
                },
            )
            .unwrap();
        assert_eq!(result.feed_entries.len(), 1);

        let reopened = PublicFeedLog::durable(&path).unwrap();
        assert_eq!(reopened.replay(0, 0).len(), 1);
        let _ = fs::remove_file(&path);
    }
}
