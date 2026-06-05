use exchange_core::{BookEvent, NewOrder, Price, Side, Venue, VenueConfig};
use std::collections::VecDeque;
use std::env;
use std::time::{Duration, Instant};

fn main() {
    let config = SimConfig::from_env();
    let result = match config.world {
        SimWorld::SingleBin => run_single_bin_sim(config),
        SimWorld::GlobalLob => run_global_lob_sim(config),
    };
    println!("{result}");
}

#[derive(Debug, Clone, Copy)]
struct SimConfig {
    world: SimWorld,
    commands: u64,
    traders: u64,
    feed_subscribers: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimWorld {
    SingleBin,
    GlobalLob,
}

impl SimConfig {
    fn from_env() -> Self {
        let mut config = Self {
            world: SimWorld::SingleBin,
            commands: 100_000,
            traders: 100,
            feed_subscribers: 1,
        };

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--world" => config.world = parse_world(&mut args),
                "--commands" => config.commands = parse_arg(&mut args, "--commands"),
                "--traders" => config.traders = parse_arg(&mut args, "--traders"),
                "--feed-subscribers" => {
                    config.feed_subscribers = parse_arg(&mut args, "--feed-subscribers")
                }
                "--help" => {
                    println!(
                        "usage: cargo run -p exchange_sim -- --world single-bin --commands 100000 --traders 100 --feed-subscribers 1\nworlds: single-bin, global-lob"
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
    world: &'static str,
    venues: usize,
    instruments: usize,
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

        writeln!(f, "{} simulation", self.world)?;
        writeln!(f, "venues={}", self.venues)?;
        writeln!(f, "instruments={}", self.instruments)?;
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
    seed_sim_accounts(&mut venue, config.traders);
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
        world: "single-bin",
        venues: 1,
        instruments: 1,
        commands: config.commands,
        private_events,
        public_events,
        elapsed: started.elapsed(),
        matcher_elapsed,
        feed_latency: latency_stats(latencies),
    }
}

#[derive(Debug)]
struct VenueRuntime {
    venue: Venue,
    instrument_count: u32,
    command_weight: u64,
    next_order_id: u64,
}

fn run_global_lob_sim(config: SimConfig) -> SimResult {
    let mut venues = global_lob_world(config.traders);
    let mut market_data =
        MarketDataFabric::with_omniscient_traders(config.feed_subscribers, &venues);
    let total_weight = venues.iter().map(|venue| venue.command_weight).sum::<u64>();
    let instrument_count = venues
        .iter()
        .map(|venue| venue.instrument_count as usize)
        .sum::<usize>();
    let mut rng = DeterministicRng::new(0x51d0_ecc0_ffee);
    let mut latencies = Vec::new();
    let mut private_events = 0;
    let mut public_events = 0;
    let mut matcher_elapsed = Duration::ZERO;

    let started = Instant::now();

    for index in 0..config.commands {
        let venue_index = weighted_venue_index(&mut rng, &venues, total_weight);
        let venue = &mut venues[venue_index];
        let instrument_id = rng.next_u32(venue.instrument_count);
        let edge = EdgeId {
            venue_index,
            instrument_id,
        };
        let order = synthetic_world_order(index, config.traders, venue.next_order_id);
        venue.next_order_id += 1;

        let matcher_started = Instant::now();
        let events = venue
            .venue
            .submit_limit(instrument_id, order)
            .expect("generated global-lob instrument should exist");
        let matcher_done = Instant::now();
        matcher_elapsed += matcher_done.duration_since(matcher_started);

        private_events += events.len() as u64;
        let emitted = market_data.publish(edge, &events);
        let feed_done = Instant::now();

        if emitted > 0 {
            public_events += emitted;
            latencies.push(feed_done.duration_since(matcher_done).as_nanos());
        }
    }

    SimResult {
        world: "global-lob",
        venues: venues.len(),
        instruments: instrument_count,
        commands: config.commands,
        private_events,
        public_events,
        elapsed: started.elapsed(),
        matcher_elapsed,
        feed_latency: latency_stats(latencies),
    }
}

fn global_lob_world(traders: u64) -> Vec<VenueRuntime> {
    let mut venues = Vec::new();
    add_venue_tier(&mut venues, "equity-global", 4, 500, 900, traders);
    add_venue_tier(&mut venues, "equity-regional", 16, 120, 180, traders);
    add_venue_tier(&mut venues, "equity-local", 32, 25, 35, traders);
    add_venue_tier(&mut venues, "crypto-major", 12, 350, 260, traders);
    add_venue_tier(&mut venues, "crypto-tail", 32, 40, 25, traders);
    venues
}

fn add_venue_tier(
    venues: &mut Vec<VenueRuntime>,
    tier: &str,
    venue_count: usize,
    symbols_per_venue: usize,
    command_weight: u64,
    traders: u64,
) {
    for index in 0..venue_count {
        let name = format!("{tier}-{index:02}");
        let symbols = (0..symbols_per_venue)
            .map(|symbol_index| {
                format!("{}{:02}{symbol_index:04}", tier_symbol_prefix(tier), index)
            })
            .collect::<Vec<_>>();
        let mut venue = Venue::new(VenueConfig::star(
            name.clone(),
            "USD",
            symbols.iter().cloned(),
        ));
        seed_world_accounts(&mut venue, symbols.iter().map(String::as_str), traders);
        venues.push(VenueRuntime {
            venue,
            instrument_count: symbols_per_venue as u32,
            command_weight,
            next_order_id: 1,
        });
    }
}

fn tier_symbol_prefix(tier: &str) -> &'static str {
    match tier {
        "equity-global" => "EG",
        "equity-regional" => "ER",
        "equity-local" => "EL",
        "crypto-major" => "CM",
        "crypto-tail" => "CT",
        _ => "SX",
    }
}

fn seed_world_accounts<'a>(
    venue: &mut Venue,
    symbols: impl IntoIterator<Item = &'a str> + Clone,
    traders: u64,
) {
    for account_id in 1..=traders.max(1) {
        venue.credit(account_id, "USD", 1_000_000_000_000);
        for asset in symbols.clone() {
            venue.credit(account_id, asset, 1_000_000_000);
        }
    }
}

fn weighted_venue_index(
    rng: &mut DeterministicRng,
    venues: &[VenueRuntime],
    total_weight: u64,
) -> usize {
    let mut draw = rng.next_u64(total_weight.max(1));
    for (index, venue) in venues.iter().enumerate() {
        if draw < venue.command_weight {
            return index;
        }
        draw -= venue.command_weight;
    }
    venues.len().saturating_sub(1)
}

fn synthetic_world_order(index: u64, traders: u64, order_id: u64) -> NewOrder {
    let side = if index % 2 == 0 {
        Side::Sell
    } else {
        Side::Buy
    };
    let price = if index % 2 == 0 { 10_000 } else { 10_001 };

    NewOrder {
        order_id,
        account_id: (index % traders.max(1)) + 1,
        side,
        price: Price(price),
        quantity: 1,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeId {
    venue_index: usize,
    instrument_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedBookEvent {
    edge: EdgeId,
    message: String,
}

#[derive(Debug, Default)]
struct TraderMarketDataView {
    subscribed_edges: Vec<EdgeId>,
    inbox: VecDeque<ObservedBookEvent>,
}

impl TraderMarketDataView {
    fn subscribe_all_edges(venues: &[VenueRuntime]) -> Self {
        let mut subscribed_edges = Vec::new();
        for (venue_index, venue) in venues.iter().enumerate() {
            for instrument_id in 0..venue.instrument_count {
                subscribed_edges.push(EdgeId {
                    venue_index,
                    instrument_id,
                });
            }
        }

        Self {
            subscribed_edges,
            inbox: VecDeque::new(),
        }
    }

    fn is_subscribed(&self, edge: EdgeId) -> bool {
        self.subscribed_edges.binary_search(&edge).is_ok()
    }

    fn receive(&mut self, event: ObservedBookEvent) {
        self.inbox.push_back(event);
    }
}

#[derive(Debug, Default)]
struct MarketDataFabric {
    traders: Vec<TraderMarketDataView>,
}

impl MarketDataFabric {
    fn with_omniscient_traders(trader_count: u64, venues: &[VenueRuntime]) -> Self {
        Self {
            traders: (0..trader_count)
                .map(|_| TraderMarketDataView::subscribe_all_edges(venues))
                .collect(),
        }
    }

    fn publish(&mut self, edge: EdgeId, events: &[BookEvent]) -> u64 {
        let messages = events
            .iter()
            .filter(|event| is_public_event(event))
            .map(format_public_event)
            .collect::<Vec<_>>();
        let public_events = messages.len() as u64;

        for trader in &mut self.traders {
            if !trader.is_subscribed(edge) {
                continue;
            }

            for message in &messages {
                trader.receive(ObservedBookEvent {
                    edge,
                    message: message.clone(),
                });
            }
        }

        public_events
    }
}

fn seed_sim_accounts(venue: &mut Venue, traders: u64) {
    for account_id in 1..=traders.max(1) {
        venue.credit(account_id, "USD", 1_000_000_000_000);
        venue.credit(account_id, "AAA", 1_000_000_000);
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

fn parse_world(args: &mut impl Iterator<Item = String>) -> SimWorld {
    match args.next().as_deref() {
        Some("single-bin") => SimWorld::SingleBin,
        Some("global-lob") => SimWorld::GlobalLob,
        Some(world) => panic!("unknown world: {world}"),
        None => panic!("--world requires a value"),
    }
}

struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self, modulo: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.0 % modulo
    }

    fn next_u32(&mut self, modulo: u32) -> u32 {
        self.next_u64(modulo.max(1) as u64) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omniscient_trader_subscribes_to_every_global_lob_edge() {
        let mut venues = global_lob_world(2);
        let mut market_data = MarketDataFabric::with_omniscient_traders(1, &venues);
        let trader = market_data
            .traders
            .first()
            .expect("one omniscient trader should be configured");

        assert_eq!(trader.subscribed_edges.len(), 10_200);
        assert!(trader.is_subscribed(EdgeId {
            venue_index: 0,
            instrument_id: 0
        }));
        assert!(trader.is_subscribed(EdgeId {
            venue_index: 95,
            instrument_id: 39
        }));

        let edge = EdgeId {
            venue_index: 0,
            instrument_id: 0,
        };
        let events = venues[edge.venue_index]
            .venue
            .submit_limit(
                edge.instrument_id,
                NewOrder {
                    order_id: 1,
                    account_id: 1,
                    side: Side::Sell,
                    price: Price(10_000),
                    quantity: 1,
                },
            )
            .expect("generated edge should exist");

        assert_eq!(market_data.publish(edge, &events), 1);
        let observed = market_data.traders[0]
            .inbox
            .pop_front()
            .expect("trader should receive the public rested event");
        assert_eq!(observed.edge, edge);
        assert!(observed.message.contains("rested:2:order=1"));
    }
}
