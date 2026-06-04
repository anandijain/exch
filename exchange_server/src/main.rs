use exchange_core::{BookEvent, Level, NewOrder, Price, Side, Venue, VenueConfig};
use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const ORDER_COMMANDS_PER_SECOND: u32 = 100;
const FEED_CLIENT_QUEUE_CAPACITY: usize = 1024;

fn main() -> std::io::Result<()> {
    let order_addr = env::var("EXCH_ORDER_ADDR").unwrap_or_else(|_| "127.0.0.1:7001".to_string());
    let feed_addr = env::var("EXCH_FEED_ADDR").unwrap_or_else(|_| "127.0.0.1:7002".to_string());
    let exchange = Arc::new(ExchangeState::new(default_config()));

    let feed_exchange = Arc::clone(&exchange);
    let feed_thread = thread::spawn(move || listen_feed(&feed_addr, feed_exchange));

    listen_order_entry(&order_addr, exchange)?;
    feed_thread
        .join()
        .expect("feed listener thread panicked")
        .map(|_| ())
}

struct ExchangeState {
    venue: Mutex<Venue>,
    feed_subscribers: Mutex<Vec<FeedSubscriber>>,
}

impl ExchangeState {
    fn new(config: VenueConfig) -> Self {
        let mut venue = Venue::new(config);
        seed_demo_accounts(&mut venue);
        Self {
            venue: Mutex::new(venue),
            feed_subscribers: Mutex::new(Vec::new()),
        }
    }

    fn publish(&self, instrument_id: u32, events: &[BookEvent]) {
        if events.is_empty() {
            return;
        }

        let messages = events
            .iter()
            .filter(|event| is_public_feed_event(event))
            .map(|event| format!("event instrument={instrument_id} {}", event_line(event)))
            .collect::<Vec<_>>();

        if messages.is_empty() {
            return;
        }

        let mut subscribers = self
            .feed_subscribers
            .lock()
            .expect("feed subscriber mutex poisoned");
        subscribers.retain(|subscriber| {
            if subscriber.instrument_id != instrument_id {
                return true;
            }

            messages.iter().all(
                |message| match subscriber.sender.try_send(message.clone()) {
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
}

struct FeedSubscriber {
    instrument_id: u32,
    sender: SyncSender<String>,
}

fn listen_order_entry(addr: &str, exchange: Arc<ExchangeState>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;

    println!("order entry listening on {addr}");
    for stream in listener.incoming() {
        let stream = stream?;
        let exchange = Arc::clone(&exchange);
        thread::spawn(move || {
            if let Err(error) = handle_order_client(stream, exchange) {
                eprintln!("order client error: {error}");
            }
        });
    }

    Ok(())
}

fn listen_feed(addr: &str, exchange: Arc<ExchangeState>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;

    println!("market data feed listening on {addr}");
    for stream in listener.incoming() {
        let stream = stream?;
        let exchange = Arc::clone(&exchange);
        thread::spawn(move || {
            if let Err(error) = handle_feed_client(stream, exchange) {
                eprintln!("feed client error: {error}");
            }
        });
    }

    Ok(())
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

fn handle_order_client(stream: TcpStream, exchange: Arc<ExchangeState>) -> std::io::Result<()> {
    let mut writer = stream.try_clone()?;
    let reader = BufReader::new(stream);
    let mut rate_limit = RateLimit::per_second(ORDER_COMMANDS_PER_SECOND);

    writeln!(
        writer,
        "ok hello protocol=exch-order-entry commands=instruments,book,order,replace,cancel,account,revenue,help"
    )?;

    for line in reader.lines() {
        let line = line?;
        let response = if rate_limit.allow() {
            handle_order_command(&line, &exchange)
        } else {
            "error rate-limit-exceeded".to_string()
        };
        writeln!(writer, "{response}")?;
    }

    Ok(())
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

fn handle_feed_client(stream: TcpStream, exchange: Arc<ExchangeState>) -> std::io::Result<()> {
    let mut writer = stream.try_clone()?;
    let reader = BufReader::new(stream);

    writeln!(
        writer,
        "ok hello protocol=exch-market-data commands=subscribe,help"
    )?;

    for line in reader.lines() {
        let line = line?;
        let parts = line.split_whitespace().collect::<Vec<_>>();

        match parts.as_slice() {
            ["help"] => writeln!(writer, "ok help subscribe <instrument_id> [depth]")?,
            ["subscribe", instrument_id] => {
                subscribe_feed(*instrument_id, None, &exchange, &mut writer)?;
                break;
            }
            ["subscribe", instrument_id, depth] => {
                subscribe_feed(*instrument_id, Some(*depth), &exchange, &mut writer)?;
                break;
            }
            _ => writeln!(writer, "error usage subscribe <instrument_id> [depth]")?,
        }
    }

    Ok(())
}

fn subscribe_feed(
    instrument_id: &str,
    depth: Option<&str>,
    exchange: &Arc<ExchangeState>,
    writer: &mut TcpStream,
) -> std::io::Result<()> {
    let Some(instrument_id) = parse(instrument_id, "instrument_id") else {
        writeln!(writer, "error invalid-instrument-id")?;
        return Ok(());
    };
    let depth = match depth {
        Some(depth) => {
            let Some(depth) = parse(depth, "depth") else {
                writeln!(writer, "error invalid-depth")?;
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
            writeln!(writer, "error unknown-instrument")?;
            return Ok(());
        }
    };

    writeln!(
        writer,
        "ok subscribed instrument={instrument_id} depth={depth}"
    )?;
    writeln!(
        writer,
        "snapshot instrument={instrument_id} seq={} checksum={} bids={} asks={}",
        snapshot.seq,
        snapshot.checksum,
        levels(&snapshot.bids),
        levels(&snapshot.asks)
    )?;

    let (sender, receiver) = mpsc::sync_channel(FEED_CLIENT_QUEUE_CAPACITY);
    exchange.subscribe(instrument_id, sender);

    for message in receiver {
        writeln!(writer, "{message}")?;
    }

    Ok(())
}

fn handle_order_command(line: &str, exchange: &Arc<ExchangeState>) -> String {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    let Some(command) = parts.first().copied() else {
        return "error empty-command".to_string();
    };

    match command {
        "help" => help(),
        "instruments" => instruments(exchange),
        "book" => book(&parts, exchange),
        "order" => order(&parts, exchange),
        "replace" => replace(&parts, exchange),
        "cancel" => cancel(&parts, exchange),
        "account" => account(&parts, exchange),
        "revenue" => revenue(&parts, exchange),
        _ => format!("error unknown-command command={command}"),
    }
}

fn help() -> String {
    "ok help instruments | book <instrument_id> [depth] | order <instrument_id> <order_id> <account_id> <buy|sell> <price> <quantity> | replace <instrument_id> <old_order_id> <new_order_id> <account_id> <buy|sell> <price> <quantity> | cancel <instrument_id> <order_id> | account <account_id> <asset> | revenue <asset>".to_string()
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

    let Some(instrument_id) = parse(parts[1], "instrument_id") else {
        return "error invalid-instrument-id".to_string();
    };
    let depth = if parts.len() == 3 {
        let Some(depth) = parse(parts[2], "depth") else {
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

fn order(parts: &[&str], exchange: &Arc<ExchangeState>) -> String {
    if parts.len() != 7 {
        return "error usage order <instrument_id> <order_id> <account_id> <buy|sell> <price> <quantity>".to_string();
    }

    let Some(instrument_id) = parse(parts[1], "instrument_id") else {
        return "error invalid-instrument-id".to_string();
    };
    let Some(order_id) = parse(parts[2], "order_id") else {
        return "error invalid-order-id".to_string();
    };
    let Some(account_id) = parse(parts[3], "account_id") else {
        return "error invalid-account-id".to_string();
    };
    let Ok(side) = parts[4].parse::<Side>() else {
        return "error invalid-side".to_string();
    };
    let Some(price) = parse(parts[5], "price") else {
        return "error invalid-price".to_string();
    };
    let Some(quantity) = parse(parts[6], "quantity") else {
        return "error invalid-quantity".to_string();
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
            exchange.publish(instrument_id, &events);
            format!(
                "ok events {}",
                events
                    .iter()
                    .map(private_event)
                    .collect::<Vec<_>>()
                    .join("|")
            )
        }
        Err(_) => "error unknown-instrument".to_string(),
    }
}

fn cancel(parts: &[&str], exchange: &Arc<ExchangeState>) -> String {
    if parts.len() != 3 {
        return "error usage cancel <instrument_id> <order_id>".to_string();
    }

    let Some(instrument_id) = parse(parts[1], "instrument_id") else {
        return "error invalid-instrument-id".to_string();
    };
    let Some(order_id) = parse(parts[2], "order_id") else {
        return "error invalid-order-id".to_string();
    };

    match exchange
        .venue
        .lock()
        .expect("venue mutex poisoned")
        .cancel(instrument_id, order_id)
    {
        Ok(book_event) => {
            exchange.publish(instrument_id, std::slice::from_ref(&book_event));
            format!("ok events {}", private_event(&book_event))
        }
        Err(_) => "error unknown-instrument".to_string(),
    }
}

fn replace(parts: &[&str], exchange: &Arc<ExchangeState>) -> String {
    if parts.len() != 8 {
        return "error usage replace <instrument_id> <old_order_id> <new_order_id> <account_id> <buy|sell> <price> <quantity>".to_string();
    }

    let Some(instrument_id) = parse(parts[1], "instrument_id") else {
        return "error invalid-instrument-id".to_string();
    };
    let Some(old_order_id) = parse(parts[2], "old_order_id") else {
        return "error invalid-old-order-id".to_string();
    };
    let Some(new_order_id) = parse(parts[3], "new_order_id") else {
        return "error invalid-new-order-id".to_string();
    };
    let Some(account_id) = parse(parts[4], "account_id") else {
        return "error invalid-account-id".to_string();
    };
    let Ok(side) = parts[5].parse::<Side>() else {
        return "error invalid-side".to_string();
    };
    let Some(price) = parse(parts[6], "price") else {
        return "error invalid-price".to_string();
    };
    let Some(quantity) = parse(parts[7], "quantity") else {
        return "error invalid-quantity".to_string();
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
            exchange.publish(instrument_id, &events);
            format!(
                "ok events {}",
                events
                    .iter()
                    .map(private_event)
                    .collect::<Vec<_>>()
                    .join("|")
            )
        }
        Err(_) => "error unknown-instrument".to_string(),
    }
}

fn account(parts: &[&str], exchange: &Arc<ExchangeState>) -> String {
    if parts.len() != 3 {
        return "error usage account <account_id> <asset>".to_string();
    }
    let Some(account_id) = parse(parts[1], "account_id") else {
        return "error invalid-account-id".to_string();
    };
    let asset = parts[2];
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

fn parse<T: std::str::FromStr>(value: &str, _name: &str) -> Option<T> {
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

fn is_public_feed_event(event: &BookEvent) -> bool {
    !matches!(
        event,
        BookEvent::Accepted { .. } | BookEvent::Rejected { .. }
    )
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
