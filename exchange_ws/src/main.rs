use exchange_core::{BookEvent, Level, NewOrder, Price, Side, Venue, VenueConfig};
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tungstenite::{accept, Message, WebSocket};

const ORDER_COMMANDS_PER_SECOND: u32 = 100;
const FEED_CLIENT_QUEUE_CAPACITY: usize = 1024;
const PRIVATE_WRITE_TIMEOUT: Duration = Duration::from_millis(100);

fn main() -> std::io::Result<()> {
    let order_addr =
        env::var("EXCH_WS_ORDER_ADDR").unwrap_or_else(|_| "127.0.0.1:7011".to_string());
    let feed_addr = env::var("EXCH_WS_FEED_ADDR").unwrap_or_else(|_| "127.0.0.1:7012".to_string());
    let exchange = Arc::new(ExchangeState::new(default_config()));

    let feed_exchange = Arc::clone(&exchange);
    let feed_thread = thread::spawn(move || listen_feed_ws(&feed_addr, feed_exchange));

    listen_order_ws(&order_addr, exchange)?;
    feed_thread
        .join()
        .expect("feed websocket listener thread panicked")
        .map(|_| ())
}

struct ExchangeState {
    venue: Mutex<Venue>,
    api_keys: ApiKeys,
    feed_log: Mutex<BTreeMap<u32, Vec<FeedLogEntry>>>,
    feed_subscribers: Mutex<Vec<FeedSubscriber>>,
}

impl ExchangeState {
    fn new(config: VenueConfig) -> Self {
        let mut venue = Venue::new(config);
        seed_demo_accounts(&mut venue);
        Self {
            venue: Mutex::new(venue),
            api_keys: ApiKeys::load(),
            feed_log: Mutex::new(BTreeMap::new()),
            feed_subscribers: Mutex::new(Vec::new()),
        }
    }

    fn publish(&self, instrument_id: u32, events: &[BookEvent]) {
        let entries = events
            .iter()
            .filter(|event| is_public_event(event))
            .map(|event| FeedLogEntry {
                seq: event_seq(event),
                message: format!("event instrument={instrument_id} {}", event_line(event)),
            })
            .collect::<Vec<_>>();

        if entries.is_empty() {
            return;
        }

        self.feed_log
            .lock()
            .expect("feed log mutex poisoned")
            .entry(instrument_id)
            .or_default()
            .extend(entries.clone());

        let mut subscribers = self
            .feed_subscribers
            .lock()
            .expect("feed subscriber mutex poisoned");
        subscribers.retain(|subscriber| {
            if subscriber.instrument_id != instrument_id {
                return true;
            }

            entries.iter().all(
                |entry| match subscriber.sender.try_send(entry.message.clone()) {
                    Ok(()) => true,
                    Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => false,
                },
            )
        });
    }

    fn subscribe(&self, instrument_id: u32, sender: SyncSender<String>) {
        self.feed_subscribers
            .lock()
            .expect("feed subscriber mutex poisoned")
            .push(FeedSubscriber {
                instrument_id,
                sender,
            });
    }

    fn replay(&self, instrument_id: u32, after_seq: u64) -> Vec<String> {
        self.feed_log
            .lock()
            .expect("feed log mutex poisoned")
            .get(&instrument_id)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| entry.seq > after_seq)
                    .map(|entry| entry.message.clone())
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
struct FeedLogEntry {
    seq: u64,
    message: String,
}

#[derive(Debug, Clone)]
struct ApiKey {
    account_id: u64,
    label: String,
}

#[derive(Debug, Clone)]
struct ApiKeys {
    keys: BTreeMap<String, ApiKey>,
}

impl ApiKeys {
    fn load() -> Self {
        let path =
            env::var("EXCH_API_KEYS").unwrap_or_else(|_| "config/local/api_keys.txt".to_string());
        match fs::read_to_string(&path) {
            Ok(contents) => Self::parse(&contents),
            Err(_) => Self::parse(include_str!("../../config/public_access.example.txt")),
        }
    }

    fn parse(contents: &str) -> Self {
        let mut keys = BTreeMap::new();
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let parts = line.split_whitespace().collect::<Vec<_>>();
            if parts.len() < 2 {
                continue;
            }
            let Ok(account_id) = parts[1].parse() else {
                continue;
            };
            keys.insert(
                parts[0].to_string(),
                ApiKey {
                    account_id,
                    label: parts.get(2).copied().unwrap_or("unlabeled").to_string(),
                },
            );
        }

        Self { keys }
    }

    fn authenticate(&self, key: &str) -> Option<&ApiKey> {
        self.keys.get(key)
    }
}

struct FeedSubscriber {
    instrument_id: u32,
    sender: SyncSender<String>,
}

fn default_config() -> VenueConfig {
    VenueConfig::star(
        "local-equities",
        "USD",
        ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"],
    )
}

fn seed_demo_accounts(venue: &mut Venue) {
    for account_id in 1..=1_000 {
        venue.credit(account_id, "USD", 1_000_000_000);
        for asset in ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"] {
            venue.credit(account_id, asset, 1_000_000);
        }
    }
}

fn listen_order_ws(addr: &str, exchange: Arc<ExchangeState>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    println!("order websocket listening on ws://{addr}");

    for stream in listener.incoming() {
        let stream = stream?;
        let exchange = Arc::clone(&exchange);
        thread::spawn(move || {
            if let Err(error) = handle_order_ws(stream, exchange) {
                eprintln!("order websocket client error: {error}");
            }
        });
    }

    Ok(())
}

fn listen_feed_ws(addr: &str, exchange: Arc<ExchangeState>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    println!("feed websocket listening on ws://{addr}");

    for stream in listener.incoming() {
        let stream = stream?;
        let exchange = Arc::clone(&exchange);
        thread::spawn(move || {
            if let Err(error) = handle_feed_ws(stream, exchange) {
                eprintln!("feed websocket client error: {error}");
            }
        });
    }

    Ok(())
}

type WsHandlerResult = Result<(), Box<dyn Error + Send + Sync>>;

fn handle_order_ws(stream: TcpStream, exchange: Arc<ExchangeState>) -> WsHandlerResult {
    stream.set_write_timeout(Some(PRIVATE_WRITE_TIMEOUT))?;
    let mut socket = accept(stream)?;
    let mut rate_limit = RateLimit::per_second(ORDER_COMMANDS_PER_SECOND);
    let mut session = OrderSession::default();
    socket.send(Message::Text(
        "ok hello protocol=exch-ws-order commands=auth,instruments,book,order,replace,cancel,account,revenue,help".to_string(),
    ))?;

    loop {
        let message = socket.read()?;
        if message.is_close() {
            return Ok(());
        }
        if !message.is_text() {
            socket.send(Message::Text("error text-messages-only".to_string()))?;
            continue;
        }

        let result = if rate_limit.allow() {
            handle_order_command(message.to_text()?, &exchange, &mut session)
        } else {
            CommandResult::private("error rate-limit-exceeded")
        };
        socket.send(Message::Text(result.private_response))?;
        if let Some((instrument_id, events)) = result.public_events {
            exchange.publish(instrument_id, &events);
        }
    }
}

#[derive(Debug, Default)]
struct OrderSession {
    account_id: Option<u64>,
    label: Option<String>,
}

impl OrderSession {
    fn authenticate(&mut self, api_key: &ApiKey) {
        self.account_id = Some(api_key.account_id);
        self.label = Some(api_key.label.clone());
    }

    fn account_id(&self) -> Option<u64> {
        self.account_id
    }
}

struct RateLimit {
    limit: u32,
    window_started: Instant,
    used: u32,
}

impl RateLimit {
    fn per_second(limit: u32) -> Self {
        Self {
            limit,
            window_started: Instant::now(),
            used: 0,
        }
    }

    fn allow(&mut self) -> bool {
        if self.window_started.elapsed() >= Duration::from_secs(1) {
            self.window_started = Instant::now();
            self.used = 0;
        }

        if self.used >= self.limit {
            return false;
        }

        self.used += 1;
        true
    }
}

fn handle_feed_ws(stream: TcpStream, exchange: Arc<ExchangeState>) -> WsHandlerResult {
    let mut socket = accept(stream)?;
    socket.send(Message::Text(
        "ok hello protocol=exch-ws-feed commands=subscribe,replay,help".to_string(),
    ))?;

    loop {
        let message = socket.read()?;
        if message.is_close() {
            return Ok(());
        }
        if !message.is_text() {
            socket.send(Message::Text("error text-messages-only".to_string()))?;
            continue;
        }

        let parts = message.to_text()?.split_whitespace().collect::<Vec<_>>();
        match parts.as_slice() {
            ["help"] => socket.send(Message::Text(
                "ok help replay <instrument_id> <after_seq> | subscribe <instrument_id> [depth]"
                    .to_string(),
            ))?,
            ["replay", instrument_id, after_seq] => {
                replay_feed_ws(*instrument_id, *after_seq, &exchange, &mut socket)?;
            }
            ["subscribe", instrument_id] => {
                subscribe_feed_ws(*instrument_id, None, &exchange, socket)?;
                return Ok(());
            }
            ["subscribe", instrument_id, depth] => {
                subscribe_feed_ws(*instrument_id, Some(*depth), &exchange, socket)?;
                return Ok(());
            }
            _ => socket.send(Message::Text(
                "error usage subscribe <instrument_id> [depth]".to_string(),
            ))?,
        }
    }
}

fn subscribe_feed_ws(
    instrument_id: &str,
    depth: Option<&str>,
    exchange: &Arc<ExchangeState>,
    mut socket: WebSocket<TcpStream>,
) -> tungstenite::Result<()> {
    let Some(instrument_id) = parse(instrument_id) else {
        socket.send(Message::Text("error invalid-instrument-id".to_string()))?;
        return Ok(());
    };
    let depth = match depth {
        Some(depth) => {
            let Some(depth) = parse(depth) else {
                socket.send(Message::Text("error invalid-depth".to_string()))?;
                return Ok(());
            };
            depth
        }
        None => {
            exchange
                .venue
                .lock()
                .expect("venue mutex poisoned")
                .config()
                .default_snapshot_depth
        }
    };

    let snapshot = match exchange
        .venue
        .lock()
        .expect("venue mutex poisoned")
        .snapshot(instrument_id, depth)
    {
        Ok(snapshot) => snapshot,
        Err(_) => {
            socket.send(Message::Text("error unknown-instrument".to_string()))?;
            return Ok(());
        }
    };

    socket.send(Message::Text(format!(
        "ok subscribed instrument={instrument_id} depth={depth}"
    )))?;
    socket.send(Message::Text(format!(
        "snapshot instrument={instrument_id} seq={} checksum={} bids={} asks={}",
        snapshot.seq,
        snapshot.checksum,
        levels(&snapshot.bids),
        levels(&snapshot.asks)
    )))?;

    let (sender, receiver) = mpsc::sync_channel(FEED_CLIENT_QUEUE_CAPACITY);
    exchange.subscribe(instrument_id, sender);

    for message in receiver {
        socket.send(Message::Text(message))?;
    }

    Ok(())
}

fn replay_feed_ws(
    instrument_id: &str,
    after_seq: &str,
    exchange: &Arc<ExchangeState>,
    socket: &mut WebSocket<TcpStream>,
) -> tungstenite::Result<()> {
    let Some(instrument_id) = parse(instrument_id) else {
        socket.send(Message::Text("error invalid-instrument-id".to_string()))?;
        return Ok(());
    };
    let Some(after_seq) = parse(after_seq) else {
        socket.send(Message::Text("error invalid-after-seq".to_string()))?;
        return Ok(());
    };

    let messages = exchange.replay(instrument_id, after_seq);
    socket.send(Message::Text(format!(
        "ok replay instrument={instrument_id} after_seq={after_seq} count={}",
        messages.len()
    )))?;
    for message in messages {
        socket.send(Message::Text(message))?;
    }

    Ok(())
}

struct CommandResult {
    private_response: String,
    public_events: Option<(u32, Vec<BookEvent>)>,
}

impl CommandResult {
    fn private(response: impl Into<String>) -> Self {
        Self {
            private_response: response.into(),
            public_events: None,
        }
    }

    fn with_public(
        response: impl Into<String>,
        instrument_id: u32,
        events: Vec<BookEvent>,
    ) -> Self {
        Self {
            private_response: response.into(),
            public_events: Some((instrument_id, events)),
        }
    }
}

fn handle_order_command(
    line: &str,
    exchange: &Arc<ExchangeState>,
    session: &mut OrderSession,
) -> CommandResult {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    let Some(command) = parts.first().copied() else {
        return CommandResult::private("error empty-command");
    };

    match command {
        "help" => CommandResult::private("ok help auth <api_key> | instruments | book <instrument_id> [depth] | order <instrument_id> <order_id> <buy|sell> <price> <quantity> | replace <instrument_id> <old_order_id> <new_order_id> <buy|sell> <price> <quantity> | cancel <instrument_id> <order_id> | account <asset> | revenue <asset>"),
        "auth" => auth(&parts, exchange, session),
        "instruments" => CommandResult::private(instruments(exchange)),
        "book" => CommandResult::private(book(&parts, exchange)),
        "order" => order(&parts, exchange, session),
        "replace" => replace(&parts, exchange, session),
        "cancel" => cancel(&parts, exchange),
        "account" => CommandResult::private(account(&parts, exchange, session)),
        "revenue" => CommandResult::private(revenue(&parts, exchange)),
        _ => CommandResult::private(format!("error unknown-command command={command}")),
    }
}

fn auth(
    parts: &[&str],
    exchange: &Arc<ExchangeState>,
    session: &mut OrderSession,
) -> CommandResult {
    if parts.len() != 2 {
        return CommandResult::private("error usage auth <api_key>");
    }

    match exchange.api_keys.authenticate(parts[1]) {
        Some(api_key) => {
            session.authenticate(api_key);
            CommandResult::private(format!(
                "ok auth account={} label={}",
                api_key.account_id, api_key.label
            ))
        }
        None => CommandResult::private("error auth-failed"),
    }
}

fn instruments(exchange: &Arc<ExchangeState>) -> String {
    let venue = exchange.venue.lock().expect("venue mutex poisoned");
    let instruments = venue
        .config()
        .instruments
        .iter()
        .map(|instrument| format!("{}:{}", instrument.id, instrument.symbol()))
        .collect::<Vec<_>>()
        .join(",");

    format!("ok instruments venue={} {instruments}", venue.config().name)
}

fn book(parts: &[&str], exchange: &Arc<ExchangeState>) -> String {
    if parts.len() < 2 || parts.len() > 3 {
        return "error usage book <instrument_id> [depth]".to_string();
    }

    let Some(instrument_id) = parse(parts[1]) else {
        return "error invalid-instrument-id".to_string();
    };
    let depth = if parts.len() == 3 {
        let Some(depth) = parse(parts[2]) else {
            return "error invalid-depth".to_string();
        };
        depth
    } else {
        exchange
            .venue
            .lock()
            .expect("venue mutex poisoned")
            .config()
            .default_snapshot_depth
    };

    match exchange
        .venue
        .lock()
        .expect("venue mutex poisoned")
        .snapshot(instrument_id, depth)
    {
        Ok(snapshot) => format!(
            "ok book seq={} checksum={} bids={} asks={}",
            snapshot.seq,
            snapshot.checksum,
            levels(&snapshot.bids),
            levels(&snapshot.asks)
        ),
        Err(_) => "error unknown-instrument".to_string(),
    }
}

fn order(parts: &[&str], exchange: &Arc<ExchangeState>, session: &OrderSession) -> CommandResult {
    let Some(account_id) = session.account_id() else {
        return CommandResult::private("error not-authenticated");
    };
    if parts.len() != 6 {
        return CommandResult::private(
            "error usage order <instrument_id> <order_id> <buy|sell> <price> <quantity>",
        );
    }

    let Some(instrument_id) = parse(parts[1]) else {
        return CommandResult::private("error invalid-instrument-id");
    };
    let Some(order_id) = parse(parts[2]) else {
        return CommandResult::private("error invalid-order-id");
    };
    let Ok(side) = parts[3].parse::<Side>() else {
        return CommandResult::private("error invalid-side");
    };
    let Some(price) = parse(parts[4]) else {
        return CommandResult::private("error invalid-price");
    };
    let Some(quantity) = parse(parts[5]) else {
        return CommandResult::private("error invalid-quantity");
    };

    let order = NewOrder {
        order_id,
        account_id,
        side,
        price: Price(price),
        quantity,
    };

    match exchange
        .venue
        .lock()
        .expect("venue mutex poisoned")
        .submit_limit(instrument_id, order)
    {
        Ok(events) => {
            let response = format!(
                "ok events {}",
                events
                    .iter()
                    .map(private_event)
                    .collect::<Vec<_>>()
                    .join("|")
            );
            CommandResult::with_public(response, instrument_id, events)
        }
        Err(_) => CommandResult::private("error unknown-instrument"),
    }
}

fn cancel(parts: &[&str], exchange: &Arc<ExchangeState>) -> CommandResult {
    if parts.len() != 3 {
        return CommandResult::private("error usage cancel <instrument_id> <order_id>");
    }

    let Some(instrument_id) = parse(parts[1]) else {
        return CommandResult::private("error invalid-instrument-id");
    };
    let Some(order_id) = parse(parts[2]) else {
        return CommandResult::private("error invalid-order-id");
    };

    match exchange
        .venue
        .lock()
        .expect("venue mutex poisoned")
        .cancel(instrument_id, order_id)
    {
        Ok(book_event) => {
            let response = format!("ok events {}", private_event(&book_event));
            CommandResult::with_public(response, instrument_id, vec![book_event])
        }
        Err(_) => CommandResult::private("error unknown-instrument"),
    }
}

fn replace(parts: &[&str], exchange: &Arc<ExchangeState>, session: &OrderSession) -> CommandResult {
    let Some(account_id) = session.account_id() else {
        return CommandResult::private("error not-authenticated");
    };
    if parts.len() != 7 {
        return CommandResult::private("error usage replace <instrument_id> <old_order_id> <new_order_id> <buy|sell> <price> <quantity>");
    }

    let Some(instrument_id) = parse(parts[1]) else {
        return CommandResult::private("error invalid-instrument-id");
    };
    let Some(old_order_id) = parse(parts[2]) else {
        return CommandResult::private("error invalid-old-order-id");
    };
    let Some(new_order_id) = parse(parts[3]) else {
        return CommandResult::private("error invalid-new-order-id");
    };
    let Ok(side) = parts[4].parse::<Side>() else {
        return CommandResult::private("error invalid-side");
    };
    let Some(price) = parse(parts[5]) else {
        return CommandResult::private("error invalid-price");
    };
    let Some(quantity) = parse(parts[6]) else {
        return CommandResult::private("error invalid-quantity");
    };

    let order = NewOrder {
        order_id: new_order_id,
        account_id,
        side,
        price: Price(price),
        quantity,
    };

    match exchange
        .venue
        .lock()
        .expect("venue mutex poisoned")
        .replace_limit(instrument_id, old_order_id, order)
    {
        Ok(events) => {
            let response = format!(
                "ok events {}",
                events
                    .iter()
                    .map(private_event)
                    .collect::<Vec<_>>()
                    .join("|")
            );
            CommandResult::with_public(response, instrument_id, events)
        }
        Err(_) => CommandResult::private("error unknown-instrument"),
    }
}

fn account(parts: &[&str], exchange: &Arc<ExchangeState>, session: &OrderSession) -> String {
    let Some(account_id) = session.account_id() else {
        return "error not-authenticated".to_string();
    };
    if parts.len() != 2 {
        return "error usage account <asset>".to_string();
    }
    let asset = parts[1];
    let venue = exchange.venue.lock().expect("venue mutex poisoned");
    format!(
        "ok account account={} asset={} available={} reserved={}",
        account_id,
        asset,
        venue.balance(account_id, asset),
        venue.reserved(account_id, asset)
    )
}

fn revenue(parts: &[&str], exchange: &Arc<ExchangeState>) -> String {
    if parts.len() != 2 {
        return "error usage revenue <asset>".to_string();
    }
    let asset = parts[1];
    let venue = exchange.venue.lock().expect("venue mutex poisoned");
    format!("ok revenue asset={} amount={}", asset, venue.revenue(asset))
}

fn parse<T: std::str::FromStr>(value: &str) -> Option<T> {
    value.parse().ok()
}

fn levels(levels: &[Level]) -> String {
    if levels.is_empty() {
        return "-".to_string();
    }

    levels
        .iter()
        .map(|level| format!("{}@{}", level.quantity, level.price.0))
        .collect::<Vec<_>>()
        .join(",")
}

fn is_public_event(event: &BookEvent) -> bool {
    !matches!(
        event,
        BookEvent::Accepted { .. } | BookEvent::Rejected { .. }
    )
}

fn event_seq(event: &BookEvent) -> u64 {
    match event {
        BookEvent::Accepted { seq, .. }
        | BookEvent::Executed { seq, .. }
        | BookEvent::Rested { seq, .. }
        | BookEvent::Canceled { seq, .. }
        | BookEvent::Rejected { seq, .. } => *seq,
    }
}

fn private_event(event: &BookEvent) -> String {
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

fn event_line(event: &BookEvent) -> String {
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

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}
