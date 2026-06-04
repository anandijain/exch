use exchange_core::{BookEvent, NewOrder, Price, Side, Venue, VenueConfig};
use std::collections::VecDeque;
use std::env;
use std::time::{Duration, Instant};

fn main() {
    let config = SimConfig::from_env();
    let result = run_single_bin_sim(config);
    println!("{result}");
}

#[derive(Debug, Clone, Copy)]
struct SimConfig {
    commands: u64,
    traders: u64,
    feed_subscribers: u64,
}

impl SimConfig {
    fn from_env() -> Self {
        let mut config = Self {
            commands: 100_000,
            traders: 100,
            feed_subscribers: 1,
        };

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--commands" => config.commands = parse_arg(&mut args, "--commands"),
                "--traders" => config.traders = parse_arg(&mut args, "--traders"),
                "--feed-subscribers" => {
                    config.feed_subscribers = parse_arg(&mut args, "--feed-subscribers")
                }
                "--help" => {
                    println!(
                        "usage: cargo run -p exchange_sim -- --commands 100000 --traders 100 --feed-subscribers 1"
                    );
                    std::process::exit(0);
                }
                unknown => panic!("unknown arg: {unknown}"),
            }
        }

        config
    }
}

#[derive(Debug)]
struct SimResult {
    commands: u64,
    private_events: u64,
    public_events: u64,
    elapsed: Duration,
    matcher_elapsed: Duration,
    feed_latency: LatencyStats,
}

impl std::fmt::Display for SimResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let elapsed_secs = self.elapsed.as_secs_f64();
        let matcher_secs = self.matcher_elapsed.as_secs_f64();

        writeln!(f, "single-bin simulation")?;
        writeln!(f, "commands={}", self.commands)?;
        writeln!(f, "private_events={}", self.private_events)?;
        writeln!(f, "public_events={}", self.public_events)?;
        writeln!(f, "elapsed_ms={:.3}", elapsed_secs * 1000.0)?;
        writeln!(
            f,
            "commands_per_sec={:.0}",
            self.commands as f64 / elapsed_secs
        )?;
        writeln!(
            f,
            "matcher_commands_per_sec={:.0}",
            self.commands as f64 / matcher_secs
        )?;
        writeln!(
            f,
            "public_events_per_sec={:.0}",
            self.public_events as f64 / elapsed_secs
        )?;
        writeln!(f, "feed_latency_ns_min={}", self.feed_latency.min)?;
        writeln!(f, "feed_latency_ns_p50={}", self.feed_latency.p50)?;
        writeln!(f, "feed_latency_ns_p95={}", self.feed_latency.p95)?;
        writeln!(f, "feed_latency_ns_p99={}", self.feed_latency.p99)?;
        writeln!(f, "feed_latency_ns_max={}", self.feed_latency.max)
    }
}

#[derive(Debug, Default)]
struct FeedSink {
    subscribers: Vec<VecDeque<String>>,
}

impl FeedSink {
    fn new(subscriber_count: u64) -> Self {
        Self {
            subscribers: (0..subscriber_count).map(|_| VecDeque::new()).collect(),
        }
    }

    fn publish(&mut self, events: &[BookEvent]) -> u64 {
        let messages = events
            .iter()
            .filter(|event| is_public_event(event))
            .map(format_public_event)
            .collect::<Vec<_>>();
        let public_events = messages.len() as u64;

        for subscriber in &mut self.subscribers {
            for message in &messages {
                subscriber.push_back(message.clone());
            }
        }

        public_events
    }
}

#[derive(Debug)]
struct LatencyStats {
    min: u128,
    p50: u128,
    p95: u128,
    p99: u128,
    max: u128,
}

fn run_single_bin_sim(config: SimConfig) -> SimResult {
    let mut venue = Venue::new(VenueConfig::star("sim", "USD", ["AAA"]));
    let mut feed = FeedSink::new(config.feed_subscribers);
    let mut latencies = Vec::new();
    let mut private_events = 0;
    let mut public_events = 0;
    let mut matcher_elapsed = Duration::ZERO;

    let started = Instant::now();

    for index in 0..config.commands {
        let order = synthetic_order(index, config.traders);
        let matcher_started = Instant::now();
        let events = venue
            .submit_limit(0, order)
            .expect("simulation instrument should exist");
        let matcher_done = Instant::now();
        matcher_elapsed += matcher_done.duration_since(matcher_started);

        private_events += events.len() as u64;
        let emitted = feed.publish(&events);
        let feed_done = Instant::now();

        if emitted > 0 {
            public_events += emitted;
            latencies.push(feed_done.duration_since(matcher_done).as_nanos());
        }
    }

    SimResult {
        commands: config.commands,
        private_events,
        public_events,
        elapsed: started.elapsed(),
        matcher_elapsed,
        feed_latency: latency_stats(latencies),
    }
}

fn synthetic_order(index: u64, traders: u64) -> NewOrder {
    let side = if index % 2 == 0 {
        Side::Sell
    } else {
        Side::Buy
    };
    let price = if index % 2 == 0 { 10_000 } else { 10_001 };

    NewOrder {
        order_id: index + 1,
        account_id: (index % traders.max(1)) + 1,
        side,
        price: Price(price),
        quantity: 1,
    }
}

fn is_public_event(event: &BookEvent) -> bool {
    !matches!(
        event,
        BookEvent::Accepted { .. } | BookEvent::Rejected { .. }
    )
}

fn format_public_event(event: &BookEvent) -> String {
    match event {
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
        BookEvent::Accepted { .. } | BookEvent::Rejected { .. } => String::new(),
    }
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn latency_stats(mut values: Vec<u128>) -> LatencyStats {
    if values.is_empty() {
        return LatencyStats {
            min: 0,
            p50: 0,
            p95: 0,
            p99: 0,
            max: 0,
        };
    }

    values.sort_unstable();
    LatencyStats {
        min: values[0],
        p50: percentile(&values, 50),
        p95: percentile(&values, 95),
        p99: percentile(&values, 99),
        max: values[values.len() - 1],
    }
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    let index = ((values.len() - 1) * percentile) / 100;
    values[index]
}

fn parse_arg(args: &mut impl Iterator<Item = String>, name: &str) -> u64 {
    args.next()
        .unwrap_or_else(|| panic!("{name} requires a value"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
}
