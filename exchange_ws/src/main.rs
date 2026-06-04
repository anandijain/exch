use exchange_core::{BookEvent, Level, NewOrder, Price, Side, Venue, VenueConfig};
use std::env;
use std::error::Error;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use tungstenite::{accept, Message, WebSocket};

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
    feed_subscribers: Mutex<Vec<FeedSubscriber>>,
}

impl ExchangeState {
    fn new(config: VenueConfig) -> Self {
        Self {
            venue: Mutex::new(Venue::new(config)),
            feed_subscribers: Mutex::new(Vec::new()),
        }
    }

    fn publish(&self, instrument_id: u32, events: &[BookEvent]) {
        let messages = events
            .iter()
            .filter(|event| is_public_event(event))
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

            messages
                .iter()
                .all(|message| subscriber.sender.send(message.clone()).is_ok())
        });
    }

    fn subscribe(&self, instrument_id: u32, sender: Sender<String>) {
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
    sender: Sender<String>,
}

fn default_config() -> VenueConfig {
    VenueConfig::star(
        "local-equities",
        "USD",
        ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"],
    )
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
    let mut socket = accept(stream)?;
    socket.send(Message::Text(
        "ok hello protocol=exch-ws-order commands=instruments,book,order,cancel,help".to_string(),
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

        let response = handle_order_command(message.to_text()?, &exchange);
        socket.send(Message::Text(response))?;
    }
}

fn handle_feed_ws(stream: TcpStream, exchange: Arc<ExchangeState>) -> WsHandlerResult {
    let mut socket = accept(stream)?;
    socket.send(Message::Text(
        "ok hello protocol=exch-ws-feed commands=subscribe,help".to_string(),
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
                "ok help subscribe <instrument_id> [depth]".to_string(),
            ))?,
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

    let (sender, receiver) = mpsc::channel();
    exchange.subscribe(instrument_id, sender);

    for message in receiver {
        socket.send(Message::Text(message))?;
    }

    Ok(())
}

fn handle_order_command(line: &str, exchange: &Arc<ExchangeState>) -> String {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    let Some(command) = parts.first().copied() else {
        return "error empty-command".to_string();
    };

    match command {
        "help" => "ok help instruments | book <instrument_id> [depth] | order <instrument_id> <order_id> <account_id> <buy|sell> <price> <quantity> | cancel <instrument_id> <order_id>".to_string(),
        "instruments" => instruments(exchange),
        "book" => book(&parts, exchange),
        "order" => order(&parts, exchange),
        "cancel" => cancel(&parts, exchange),
        _ => format!("error unknown-command command={command}"),
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

fn order(parts: &[&str], exchange: &Arc<ExchangeState>) -> String {
    if parts.len() != 7 {
        return "error usage order <instrument_id> <order_id> <account_id> <buy|sell> <price> <quantity>".to_string();
    }

    let Some(instrument_id) = parse(parts[1]) else {
        return "error invalid-instrument-id".to_string();
    };
    let Some(order_id) = parse(parts[2]) else {
        return "error invalid-order-id".to_string();
    };
    let Some(account_id) = parse(parts[3]) else {
        return "error invalid-account-id".to_string();
    };
    let Ok(side) = parts[4].parse::<Side>() else {
        return "error invalid-side".to_string();
    };
    let Some(price) = parse(parts[5]) else {
        return "error invalid-price".to_string();
    };
    let Some(quantity) = parse(parts[6]) else {
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

    let Some(instrument_id) = parse(parts[1]) else {
        return "error invalid-instrument-id".to_string();
    };
    let Some(order_id) = parse(parts[2]) else {
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
