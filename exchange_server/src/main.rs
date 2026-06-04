use exchange_core::{BookEvent, Level, NewOrder, Price, Side, Venue, VenueConfig};
use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

fn main() -> std::io::Result<()> {
    let addr = env::var("EXCH_ADDR").unwrap_or_else(|_| "127.0.0.1:7001".to_string());
    let venue = Arc::new(Mutex::new(Venue::new(default_config())));
    let listener = TcpListener::bind(&addr)?;

    println!("exchange_server listening on {addr}");
    for stream in listener.incoming() {
        let stream = stream?;
        let venue = Arc::clone(&venue);
        thread::spawn(move || {
            if let Err(error) = handle_client(stream, venue) {
                eprintln!("client error: {error}");
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

fn handle_client(stream: TcpStream, venue: Arc<Mutex<Venue>>) -> std::io::Result<()> {
    let mut writer = stream.try_clone()?;
    let reader = BufReader::new(stream);

    writeln!(
        writer,
        "ok hello protocol=exch-lines commands=instruments,book,order,cancel,help"
    )?;

    for line in reader.lines() {
        let line = line?;
        let response = handle_command(&line, &venue);
        writeln!(writer, "{response}")?;
    }

    Ok(())
}

fn handle_command(line: &str, venue: &Arc<Mutex<Venue>>) -> String {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    let Some(command) = parts.first().copied() else {
        return "error empty-command".to_string();
    };

    match command {
        "help" => help(),
        "instruments" => instruments(venue),
        "book" => book(&parts, venue),
        "order" => order(&parts, venue),
        "cancel" => cancel(&parts, venue),
        _ => format!("error unknown-command command={command}"),
    }
}

fn help() -> String {
    "ok help instruments | book <instrument_id> [depth] | order <instrument_id> <order_id> <account_id> <buy|sell> <price> <quantity> | cancel <instrument_id> <order_id>".to_string()
}

fn instruments(venue: &Arc<Mutex<Venue>>) -> String {
    let venue = venue.lock().expect("venue mutex poisoned");
    let instruments = venue
        .config()
        .instruments
        .iter()
        .map(|instrument| format!("{}:{}", instrument.id, instrument.symbol()))
        .collect::<Vec<_>>()
        .join(",");

    format!("ok instruments venue={} {instruments}", venue.config().name)
}

fn book(parts: &[&str], venue: &Arc<Mutex<Venue>>) -> String {
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
        venue
            .lock()
            .expect("venue mutex poisoned")
            .config()
            .default_snapshot_depth
    };

    match venue
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

fn order(parts: &[&str], venue: &Arc<Mutex<Venue>>) -> String {
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

    match venue
        .lock()
        .expect("venue mutex poisoned")
        .submit_limit(instrument_id, order)
    {
        Ok(events) => format!(
            "ok events {}",
            events.iter().map(event).collect::<Vec<_>>().join("|")
        ),
        Err(_) => "error unknown-instrument".to_string(),
    }
}

fn cancel(parts: &[&str], venue: &Arc<Mutex<Venue>>) -> String {
    if parts.len() != 3 {
        return "error usage cancel <instrument_id> <order_id>".to_string();
    }

    let Some(instrument_id) = parse(parts[1], "instrument_id") else {
        return "error invalid-instrument-id".to_string();
    };
    let Some(order_id) = parse(parts[2], "order_id") else {
        return "error invalid-order-id".to_string();
    };

    match venue
        .lock()
        .expect("venue mutex poisoned")
        .cancel(instrument_id, order_id)
    {
        Ok(book_event) => format!("ok events {}", event(&book_event)),
        Err(_) => "error unknown-instrument".to_string(),
    }
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

fn event(event: &BookEvent) -> String {
    match event {
        BookEvent::Accepted { seq, order_id } => format!("accepted:{seq}:{order_id}"),
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
