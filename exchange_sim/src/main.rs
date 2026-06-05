use exchange_core::{BookEvent, NewOrder, Price, Side, Venue, VenueConfig};
use exchange_runtime::{event_line, is_public_event};
use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    let config = SimConfig::from_env();
    if let Some(addr) = &config.live_addr {
        run_live_demo_server(addr).expect("run live demo server");
        return;
    }

    let result = match config.world {
        SimWorld::SingleBin => run_single_bin_sim(&config),
        SimWorld::GlobalLob => run_global_lob_sim(&config),
        SimWorld::ShockDemo => run_shock_demo(&config),
        SimWorld::LiveDemo => run_live_demo_once(&config),
    };
    println!("{result}");
}

#[derive(Debug, Clone)]
struct SimConfig {
    world: SimWorld,
    commands: u64,
    traders: u64,
    feed_subscribers: u64,
    visualization: Option<String>,
    live_addr: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimWorld {
    SingleBin,
    GlobalLob,
    ShockDemo,
    LiveDemo,
}

impl SimConfig {
    fn from_env() -> Self {
        let mut config = Self {
            world: SimWorld::SingleBin,
            commands: 100_000,
            traders: 100,
            feed_subscribers: 1,
            visualization: None,
            live_addr: None,
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
                "--visualization" => {
                    config.visualization = Some(parse_string_arg(&mut args, "--visualization"))
                }
                "--live" => config.live_addr = Some(parse_string_arg(&mut args, "--live")),
                "--help" => {
                    println!(
                        "usage: cargo run -p exchange_sim -- --world single-bin --commands 100000 --traders 100 --feed-subscribers 1\n       cargo run -p exchange_sim -- --world live-demo --live 127.0.0.1:8088\nworlds: single-bin, global-lob, shock-demo, live-demo"
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
    visualization_path: Option<String>,
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
        writeln!(f, "feed_latency_ns_max={}", self.feed_latency.max)?;
        if let Some(path) = &self.visualization_path {
            writeln!(f, "visualization={path}")?;
        }
        Ok(())
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

fn run_single_bin_sim(config: &SimConfig) -> SimResult {
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
        visualization_path: None,
    }
}

#[derive(Debug)]
struct VenueRuntime {
    name: String,
    tier: String,
    symbols: Vec<String>,
    venue: Venue,
    instrument_count: u32,
    command_weight: u64,
    next_order_id: u64,
}

fn run_global_lob_sim(config: &SimConfig) -> SimResult {
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
    let mut playback = GlobalPlayback::new();

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
        if config.visualization.is_some() {
            playback.record(index, edge, &venues[venue_index].venue, &events);
        }
    }

    let visualization_path = config.visualization.as_ref().map(|path| {
        write_global_lob_visualization(path, &venues, &playback)
            .expect("write global-lob visualization");
        path.clone()
    });

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
        visualization_path,
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
            name,
            tier: tier.to_string(),
            symbols,
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

#[derive(Debug, Clone)]
struct BookFrame {
    step: u64,
    bids: Vec<(u64, u64)>,
    asks: Vec<(u64, u64)>,
    events: Vec<String>,
}

#[derive(Debug, Default)]
struct GlobalPlayback {
    frames_by_edge: BTreeMap<EdgeId, Vec<BookFrame>>,
}

impl GlobalPlayback {
    fn new() -> Self {
        Self::default()
    }

    fn record(&mut self, step: u64, edge: EdgeId, venue: &Venue, events: &[BookEvent]) {
        let public_events = events
            .iter()
            .filter(|event| is_public_event(event))
            .map(format_public_event)
            .collect::<Vec<_>>();
        if public_events.is_empty() {
            return;
        }

        let snapshot = venue
            .snapshot(edge.instrument_id, 8)
            .expect("global-lob snapshot should exist");
        let bids = snapshot
            .bids
            .iter()
            .map(|level| (level.price.0, level.quantity))
            .collect();
        let asks = snapshot
            .asks
            .iter()
            .map(|level| (level.price.0, level.quantity))
            .collect();

        self.frames_by_edge
            .entry(edge)
            .or_default()
            .push(BookFrame {
                step,
                bids,
                asks,
                events: public_events,
            });
    }
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

#[derive(Debug, Clone, Copy)]
struct ScenarioPoint {
    step: u64,
    mid_price: u64,
    spread: u64,
    bid_depth: u64,
    ask_depth: u64,
    shock: bool,
}

fn run_shock_demo(config: &SimConfig) -> SimResult {
    let mut venue = Venue::new(VenueConfig::star("shock-demo", "USD", ["AAA"]));
    seed_shock_accounts(&mut venue, config.traders);
    seed_shock_book(&mut venue);

    let mut feed = FeedSink::new(config.feed_subscribers);
    let mut points = Vec::new();
    let mut private_events = 0;
    let mut public_events = 0;
    let mut matcher_elapsed = Duration::ZERO;
    let mut latencies = Vec::new();
    let mut order_id = 1_000_000;
    let shock_step = config.commands / 2;
    let started = Instant::now();

    for step in 0..config.commands {
        let shock = step == shock_step;
        let order = if shock {
            NewOrder {
                order_id,
                account_id: 1,
                side: Side::Buy,
                price: Price(10_120),
                quantity: 450,
            }
        } else {
            background_shock_order(step, config.traders, order_id, shock_step)
        };
        order_id += 1;

        let matcher_started = Instant::now();
        let events = venue
            .submit_limit(0, order)
            .expect("shock-demo instrument should exist");
        let matcher_done = Instant::now();
        matcher_elapsed += matcher_done.duration_since(matcher_started);

        private_events += events.len() as u64;
        let emitted = feed.publish(&events);
        let feed_done = Instant::now();
        if emitted > 0 {
            public_events += emitted;
            latencies.push(feed_done.duration_since(matcher_done).as_nanos());
        }

        if step % 5 == 0 || shock || step + 1 == config.commands {
            points.push(book_point(step, shock, &venue));
        }
    }

    let visualization_path = config.visualization.as_ref().map(|path| {
        write_shock_visualization(path, &points).expect("write shock visualization");
        path.clone()
    });

    SimResult {
        world: "shock-demo",
        venues: 1,
        instruments: 1,
        commands: config.commands,
        private_events,
        public_events,
        elapsed: started.elapsed(),
        matcher_elapsed,
        feed_latency: latency_stats(latencies),
        visualization_path,
    }
}

fn seed_shock_accounts(venue: &mut Venue, traders: u64) {
    for account_id in 1..=traders.max(12) {
        venue.credit(account_id, "USD", 1_000_000_000_000);
        venue.credit(account_id, "AAA", 1_000_000_000);
    }
}

fn seed_shock_book(venue: &mut Venue) {
    let mut order_id = 1;
    for level in 0..8 {
        let bid_price = 9_990 - level * 10;
        let ask_price = 10_010 + level * 10;
        for offset in 0..4 {
            let quantity = 50 + (level as u64 * 5);
            let bid_order = NewOrder {
                order_id,
                account_id: 10 + offset,
                side: Side::Buy,
                price: Price(bid_price),
                quantity,
            };
            order_id += 1;
            venue
                .submit_limit(0, bid_order)
                .expect("seed bid should submit");

            let ask_order = NewOrder {
                order_id,
                account_id: 20 + offset,
                side: Side::Sell,
                price: Price(ask_price),
                quantity,
            };
            order_id += 1;
            venue
                .submit_limit(0, ask_order)
                .expect("seed ask should submit");
        }
    }
}

fn background_shock_order(step: u64, traders: u64, order_id: u64, shock_step: u64) -> NewOrder {
    let replenish_after_shock = step > shock_step && step % 3 != 0;
    let side = if replenish_after_shock {
        Side::Sell
    } else if step % 2 == 0 {
        Side::Buy
    } else {
        Side::Sell
    };
    let price = match side {
        Side::Buy => 9_980 + ((step % 4) * 5),
        Side::Sell => {
            if replenish_after_shock {
                10_045 + ((step % 5) * 5)
            } else {
                10_020 + ((step % 4) * 5)
            }
        }
    };

    NewOrder {
        order_id,
        account_id: (step % traders.max(12)) + 1,
        side,
        price: Price(price),
        quantity: 8 + (step % 5),
    }
}

fn book_point(step: u64, shock: bool, venue: &Venue) -> ScenarioPoint {
    let snapshot = venue
        .snapshot(0, 5)
        .expect("shock-demo snapshot should exist");
    let best_bid = snapshot
        .bids
        .first()
        .map(|level| level.price.0)
        .unwrap_or(0);
    let best_ask = snapshot
        .asks
        .first()
        .map(|level| level.price.0)
        .unwrap_or(best_bid);
    let mid_price = if best_bid > 0 && best_ask > 0 {
        (best_bid + best_ask) / 2
    } else {
        best_bid.max(best_ask)
    };
    let spread = best_ask.saturating_sub(best_bid);
    let bid_depth = snapshot.bids.iter().map(|level| level.quantity).sum();
    let ask_depth = snapshot.asks.iter().map(|level| level.quantity).sum();

    ScenarioPoint {
        step,
        mid_price,
        spread,
        bid_depth,
        ask_depth,
        shock,
    }
}

fn write_shock_visualization(path: &str, points: &[ScenarioPoint]) -> std::io::Result<()> {
    let html = shock_visualization_html(points);
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, html)
}

fn shock_visualization_html(points: &[ScenarioPoint]) -> String {
    let mut html = String::new();
    let _ = writeln!(
        html,
        "<!doctype html><meta charset=\"utf-8\"><title>shock-demo</title><style>body{{font-family:system-ui,sans-serif;margin:24px;color:#18202a}}svg{{width:100%;max-width:980px;height:auto;border:1px solid #d6d9de}}.mid{{fill:none;stroke:#1d6fbe;stroke-width:3}}.spread{{fill:none;stroke:#b44d12;stroke-width:2}}.bid{{fill:none;stroke:#22863a;stroke-width:2}}.ask{{fill:none;stroke:#a40e26;stroke-width:2}}.shock{{stroke:#111;stroke-dasharray:5 5}}</style>"
    );
    let _ = writeln!(html, "<h1>shock-demo</h1>");
    let _ = writeln!(
        html,
        "<p>Seeded book, background flow, one large buy shock, then ask-side replenishment.</p>"
    );
    let _ = writeln!(html, "{}", svg_chart(points));
    let _ = writeln!(
        html,
        "<p>Blue: midprice. Orange: spread. Green/red: top-five bid/ask depth scaled into the chart.</p>"
    );
    html
}

fn svg_chart(points: &[ScenarioPoint]) -> String {
    const WIDTH: u64 = 980;
    const HEIGHT: u64 = 360;
    const PAD: u64 = 40;

    let max_step = points
        .iter()
        .map(|point| point.step)
        .max()
        .unwrap_or(1)
        .max(1);
    let min_mid = points
        .iter()
        .map(|point| point.mid_price)
        .min()
        .unwrap_or(0);
    let max_mid = points
        .iter()
        .map(|point| point.mid_price)
        .max()
        .unwrap_or(min_mid + 1)
        .max(min_mid + 1);
    let max_spread = points
        .iter()
        .map(|point| point.spread)
        .max()
        .unwrap_or(1)
        .max(1);
    let max_depth = points
        .iter()
        .map(|point| point.bid_depth.max(point.ask_depth))
        .max()
        .unwrap_or(1)
        .max(1);

    let mid_line = polyline(points, |point| {
        (
            scale(point.step, 0, max_step, PAD, WIDTH - PAD),
            scale_inverted(point.mid_price, min_mid, max_mid, PAD, HEIGHT - PAD),
        )
    });
    let spread_line = polyline(points, |point| {
        (
            scale(point.step, 0, max_step, PAD, WIDTH - PAD),
            scale_inverted(point.spread, 0, max_spread, HEIGHT / 2, HEIGHT - PAD),
        )
    });
    let bid_line = polyline(points, |point| {
        (
            scale(point.step, 0, max_step, PAD, WIDTH - PAD),
            scale_inverted(point.bid_depth, 0, max_depth, PAD, HEIGHT - PAD),
        )
    });
    let ask_line = polyline(points, |point| {
        (
            scale(point.step, 0, max_step, PAD, WIDTH - PAD),
            scale_inverted(point.ask_depth, 0, max_depth, PAD, HEIGHT - PAD),
        )
    });
    let shock_x = points
        .iter()
        .find(|point| point.shock)
        .map(|point| scale(point.step, 0, max_step, PAD, WIDTH - PAD));

    let mut svg = String::new();
    let _ = writeln!(
        svg,
        "<svg viewBox=\"0 0 {WIDTH} {HEIGHT}\" role=\"img\" aria-label=\"shock demo chart\">"
    );
    let _ = writeln!(
        svg,
        "<line x1=\"{PAD}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" stroke=\"#c8ccd2\"/>",
        HEIGHT - PAD,
        WIDTH - PAD,
        HEIGHT - PAD
    );
    if let Some(x) = shock_x {
        let _ = writeln!(
            svg,
            "<line class=\"shock\" x1=\"{x}\" y1=\"{PAD}\" x2=\"{x}\" y2=\"{}\"/>",
            HEIGHT - PAD
        );
    }
    let _ = writeln!(svg, "<polyline class=\"bid\" points=\"{bid_line}\"/>");
    let _ = writeln!(svg, "<polyline class=\"ask\" points=\"{ask_line}\"/>");
    let _ = writeln!(svg, "<polyline class=\"spread\" points=\"{spread_line}\"/>");
    let _ = writeln!(svg, "<polyline class=\"mid\" points=\"{mid_line}\"/>");
    let _ = writeln!(svg, "<text x=\"{PAD}\" y=\"24\">mid {min_mid}-{max_mid}, max spread {max_spread}, max depth {max_depth}</text>");
    let _ = writeln!(svg, "</svg>");
    svg
}

fn polyline(
    points: &[ScenarioPoint],
    mut project: impl FnMut(&ScenarioPoint) -> (u64, u64),
) -> String {
    points
        .iter()
        .map(|point| {
            let (x, y) = project(point);
            format!("{x},{y}")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn scale(value: u64, min_value: u64, max_value: u64, min_pixel: u64, max_pixel: u64) -> u64 {
    let value_range = max_value.saturating_sub(min_value).max(1);
    min_pixel + (value.saturating_sub(min_value) * (max_pixel - min_pixel) / value_range)
}

fn scale_inverted(
    value: u64,
    min_value: u64,
    max_value: u64,
    min_pixel: u64,
    max_pixel: u64,
) -> u64 {
    max_pixel - (scale(value, min_value, max_value, min_pixel, max_pixel) - min_pixel)
}

#[derive(Debug, Clone)]
struct LiveState {
    step: u64,
    venues: Vec<LiveVenueState>,
    tape: VecDeque<String>,
}

#[derive(Debug, Clone)]
struct LiveVenueState {
    name: String,
    tier: String,
    symbols: Vec<String>,
    books: Vec<LiveBookState>,
}

#[derive(Debug, Clone, Default)]
struct LiveBookState {
    bids: Vec<(u64, u64)>,
    asks: Vec<(u64, u64)>,
}

fn run_live_demo_once(config: &SimConfig) -> SimResult {
    let mut venues = live_demo_world(config.traders);
    let mut rng = DeterministicRng::new(0x1a11_ce55);
    let mut private_events = 0;
    let mut public_events = 0;
    let mut matcher_elapsed = Duration::ZERO;
    let mut feed = FeedSink::new(config.feed_subscribers);
    let mut latencies = Vec::new();
    let started = Instant::now();

    for step in 0..config.commands {
        let (venue_index, instrument_id) = live_target(&mut rng, &venues);
        let venue = &mut venues[venue_index];
        let order = live_demo_order(step, &mut rng, config.traders, venue.next_order_id);
        venue.next_order_id += 1;

        let matcher_started = Instant::now();
        let events = venue
            .venue
            .submit_limit(instrument_id, order)
            .expect("live-demo instrument should exist");
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
        world: "live-demo",
        venues: venues.len(),
        instruments: venues
            .iter()
            .map(|venue| venue.instrument_count as usize)
            .sum(),
        commands: config.commands,
        private_events,
        public_events,
        elapsed: started.elapsed(),
        matcher_elapsed,
        feed_latency: latency_stats(latencies),
        visualization_path: None,
    }
}

fn run_live_demo_server(addr: &str) -> std::io::Result<()> {
    let venues = live_demo_world(64);
    let state = Arc::new(Mutex::new(live_state_from_venues(
        0,
        &venues,
        VecDeque::new(),
    )));
    let sim_state = Arc::clone(&state);
    thread::spawn(move || run_live_sim_loop(venues, sim_state));

    let listener = TcpListener::bind(addr)?;
    println!("live demo listening on http://{addr}");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let state = Arc::clone(&state);
                thread::spawn(move || {
                    if let Err(error) = handle_live_http(stream, state) {
                        eprintln!("live client error: {error}");
                    }
                });
            }
            Err(error) => eprintln!("live accept error: {error}"),
        }
    }
    Ok(())
}

fn run_live_sim_loop(mut venues: Vec<VenueRuntime>, state: Arc<Mutex<LiveState>>) {
    let mut rng = DeterministicRng::new(0xfeed_f00d);
    let mut step = 0;
    let mut tape = VecDeque::new();

    loop {
        for _ in 0..120 {
            let (venue_index, instrument_id) = live_target(&mut rng, &venues);
            let venue = &mut venues[venue_index];
            let order = live_demo_order(step, &mut rng, 64, venue.next_order_id);
            venue.next_order_id += 1;
            let events = venue
                .venue
                .submit_limit(instrument_id, order)
                .expect("live-demo instrument should exist");
            for event in events.iter().filter(|event| is_public_event(event)) {
                let symbol = &venue.symbols[instrument_id as usize];
                tape.push_front(format!(
                    "{} {symbol}/USD {}",
                    venue.name,
                    format_public_event(event)
                ));
            }
            while tape.len() > 80 {
                tape.pop_back();
            }
            step += 1;
        }

        let next_state = live_state_from_venues(step, &venues, tape.clone());
        *state.lock().expect("live state mutex poisoned") = next_state;
        thread::sleep(Duration::from_millis(60));
    }
}

fn live_demo_world(traders: u64) -> Vec<VenueRuntime> {
    let specs = [
        (
            "ny-core",
            "equity-fast",
            ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"],
        ),
        (
            "ldn-ecn",
            "fx-ecn",
            ["EUR", "GBP", "CHF", "JPY", "AUD", "CAD"],
        ),
        (
            "sg-crypto",
            "crypto-major",
            ["BTC", "ETH", "SOL", "XRP", "DOGE", "ARB"],
        ),
        (
            "tail-venue",
            "crypto-tail",
            ["TAIL0", "TAIL1", "TAIL2", "TAIL3", "TAIL4", "TAIL5"],
        ),
    ];
    let mut venues = Vec::new();
    for (venue_index, (name, tier, symbols)) in specs.into_iter().enumerate() {
        let symbol_strings = symbols
            .iter()
            .map(|symbol| symbol.to_string())
            .collect::<Vec<_>>();
        let mut venue = Venue::new(VenueConfig::star(
            name.to_string(),
            "USD",
            symbol_strings.iter().cloned(),
        ));
        seed_world_accounts(
            &mut venue,
            symbol_strings.iter().map(String::as_str),
            traders.max(64),
        );
        let mut next_order_id = 1;
        for instrument_id in 0..symbol_strings.len() as u32 {
            next_order_id = seed_live_book(&mut venue, instrument_id, next_order_id, venue_index);
        }
        venues.push(VenueRuntime {
            name: name.to_string(),
            tier: tier.to_string(),
            symbols: symbol_strings,
            venue,
            instrument_count: symbols.len() as u32,
            command_weight: match tier {
                "equity-fast" => 5,
                "fx-ecn" => 4,
                "crypto-major" => 4,
                _ => 2,
            },
            next_order_id,
        });
    }
    venues
}

fn seed_live_book(
    venue: &mut Venue,
    instrument_id: u32,
    mut order_id: u64,
    venue_index: usize,
) -> u64 {
    let center = 10_000 + venue_index as u64 * 100;
    for level in 0..8 {
        let level = level as u64;
        for slot in 0..3 {
            let slot = slot as u64;
            let quantity = 80 + level * 18 + slot * 7;
            let bid = NewOrder {
                order_id,
                account_id: 2 + slot,
                side: Side::Buy,
                price: Price(center - 10 - level * 8),
                quantity,
            };
            order_id += 1;
            venue
                .submit_limit(instrument_id, bid)
                .expect("live seed bid should submit");

            let ask = NewOrder {
                order_id,
                account_id: 12 + slot,
                side: Side::Sell,
                price: Price(center + 10 + level * 8),
                quantity,
            };
            order_id += 1;
            venue
                .submit_limit(instrument_id, ask)
                .expect("live seed ask should submit");
        }
    }
    order_id
}

fn live_target(rng: &mut DeterministicRng, venues: &[VenueRuntime]) -> (usize, u32) {
    let total_weight = venues.iter().map(|venue| venue.command_weight).sum::<u64>();
    let venue_index = weighted_venue_index(rng, venues, total_weight);
    let instrument_id = rng.next_u32(venues[venue_index].instrument_count);
    (venue_index, instrument_id)
}

fn live_demo_order(step: u64, rng: &mut DeterministicRng, traders: u64, order_id: u64) -> NewOrder {
    let side = if rng.next_u64(100) < 50 {
        Side::Buy
    } else {
        Side::Sell
    };
    let marketable = rng.next_u64(100) < 72;
    let price = match (side, marketable) {
        (Side::Buy, true) => 10_500,
        (Side::Sell, true) => 9_500,
        (Side::Buy, false) => 9_970 + rng.next_u64(30),
        (Side::Sell, false) => 10_030 + rng.next_u64(30),
    };

    NewOrder {
        order_id,
        account_id: (step % traders.max(64)) + 1,
        side,
        price: Price(price),
        quantity: 6 + rng.next_u64(22),
    }
}

fn live_state_from_venues(step: u64, venues: &[VenueRuntime], tape: VecDeque<String>) -> LiveState {
    let venues = venues
        .iter()
        .map(|venue| {
            let books = (0..venue.instrument_count)
                .map(|instrument_id| {
                    let snapshot = venue
                        .venue
                        .snapshot(instrument_id, 8)
                        .expect("live snapshot should exist");
                    LiveBookState {
                        bids: snapshot
                            .bids
                            .iter()
                            .map(|level| (level.price.0, level.quantity))
                            .collect(),
                        asks: snapshot
                            .asks
                            .iter()
                            .map(|level| (level.price.0, level.quantity))
                            .collect(),
                    }
                })
                .collect();
            LiveVenueState {
                name: venue.name.clone(),
                tier: venue.tier.clone(),
                symbols: venue.symbols.clone(),
                books,
            }
        })
        .collect();
    LiveState { step, venues, tape }
}

fn handle_live_http(mut stream: TcpStream, state: Arc<Mutex<LiveState>>) -> std::io::Result<()> {
    let mut buffer = [0; 2048];
    let bytes = stream.read(&mut buffer)?;
    let request = String::from_utf8_lossy(&buffer[..bytes]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    match path {
        "/" => write_http_response(&mut stream, "text/html; charset=utf-8", live_demo_html()),
        "/state" => {
            let state = state.lock().expect("live state mutex poisoned").clone();
            write_http_response(
                &mut stream,
                "application/json; charset=utf-8",
                live_state_json(&state),
            )
        }
        _ => write_http_response(
            &mut stream,
            "text/plain; charset=utf-8",
            "not found".to_string(),
        ),
    }
}

fn write_http_response(
    stream: &mut TcpStream,
    content_type: &str,
    body: String,
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn live_state_json(state: &LiveState) -> String {
    let mut json = String::new();
    let _ = write!(json, "{{\"step\":{},\"venues\":[", state.step);
    for (venue_index, venue) in state.venues.iter().enumerate() {
        if venue_index > 0 {
            json.push(',');
        }
        let _ = write!(
            json,
            "{{\"name\":\"{}\",\"tier\":\"{}\",\"symbols\":[",
            json_escape(&venue.name),
            json_escape(&venue.tier)
        );
        for (symbol_index, symbol) in venue.symbols.iter().enumerate() {
            if symbol_index > 0 {
                json.push(',');
            }
            let _ = write!(json, "\"{}\"", json_escape(symbol));
        }
        json.push_str("],\"books\":[");
        for (book_index, book) in venue.books.iter().enumerate() {
            if book_index > 0 {
                json.push(',');
            }
            json.push_str("{\"bids\":[");
            write_level_json(&mut json, &book.bids);
            json.push_str("],\"asks\":[");
            write_level_json(&mut json, &book.asks);
            json.push_str("]}");
        }
        json.push_str("]}");
    }
    json.push_str("],\"tape\":[");
    for (index, event) in state.tape.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        let _ = write!(json, "\"{}\"", json_escape(event));
    }
    json.push_str("]}");
    json
}

fn live_demo_html() -> String {
    let mut html = String::new();
    let _ = writeln!(
        html,
        "<!doctype html><meta charset=\"utf-8\"><title>exch live demo</title><style>body{{margin:0;font-family:system-ui,sans-serif;background:#07111f;color:#ecf3ff}}#app{{display:grid;grid-template-columns:330px 1fr 360px;min-height:100vh}}aside,.tape{{padding:18px;background:#101b2c;overflow:auto}}main{{padding:18px}}button{{font:inherit;border:1px solid #34445d;background:#17243a;color:#ecf3ff;border-radius:6px;padding:8px 10px;cursor:pointer}}button.active{{background:#2f80ed;border-color:#2f80ed}}.venue-grid,.edge-grid{{display:grid;gap:8px}}.venue-grid{{grid-template-columns:1fr 1fr}}.edge-grid{{grid-template-columns:repeat(3,1fr);margin:14px 0}}.panel{{background:#0d1828;border:1px solid #26374f;border-radius:8px;padding:14px;margin-bottom:14px}}.asset-graph{{width:100%;height:330px;background:#091525;border:1px solid #203047;border-radius:8px;margin:12px 0}}.graph-edge{{stroke:#405a7c;stroke-width:7;stroke-linecap:round;cursor:pointer;opacity:.72}}.graph-edge.active,.graph-edge:hover{{stroke:#2f80ed;opacity:1}}.graph-node{{fill:#17243a;stroke:#77a7ff;stroke-width:2}}.graph-center{{fill:#2f80ed;stroke:#b7d1ff}}.graph-label{{fill:#ecf3ff;font-size:14px;text-anchor:middle;dominant-baseline:middle;pointer-events:none}}.book{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}table{{width:100%;border-collapse:collapse}}td,th{{padding:5px 8px;border-bottom:1px solid #203047;text-align:right}}td:first-child,th:first-child{{text-align:left}}.asks td{{color:#ff7b91}}.bids td{{color:#64d17a}}.bar{{display:inline-block;height:10px;background:#2f80ed;border-radius:2px}}.muted{{color:#93a4bd}}pre{{white-space:pre-wrap;font:12px ui-monospace,monospace;line-height:1.45}}</style>"
    );
    let _ = writeln!(html, "<div id=\"app\"><aside><h1>exch live</h1><p class=\"muted\">Four live venues, 24 books, synthetic high-activity order flow. Click a venue; hover a graph edge to inspect that book.</p><div id=\"venues\" class=\"venue-grid\"></div><div class=\"panel\"><div id=\"stats\"></div></div></aside><main><section class=\"panel\"><h2 id=\"venue-title\">venue</h2><svg id=\"asset-graph\" class=\"asset-graph\" viewBox=\"0 0 720 330\" role=\"img\" aria-label=\"venue asset graph\"></svg><div id=\"edges\" class=\"edge-grid\"></div></section><section class=\"panel\"><h2 id=\"edge-title\">book</h2><div class=\"book\"><div><h3>Asks</h3><table class=\"asks\"><tbody id=\"asks\"></tbody></table></div><div><h3>Bids</h3><table class=\"bids\"><tbody id=\"bids\"></tbody></table></div></div></section></main><section class=\"tape\"><h2>Event Tape</h2><pre id=\"tape\"></pre></section></div>");
    let _ = writeln!(html, "<script>{}</script>", live_demo_js());
    html
}

fn live_demo_js() -> &'static str {
    r#"
let state = null;
let venueIndex = 0;
let edgeIndex = 0;

async function tick() {
  const response = await fetch('/state', {cache: 'no-store'});
  state = await response.json();
  render();
}

function render() {
  if (!state) return;
  renderVenues();
  renderEdges();
  renderGraph();
  renderBook();
  document.getElementById('stats').innerHTML = `<b>step</b> ${state.step}<br><b>venues</b> ${state.venues.length}<br><b>books</b> ${state.venues.reduce((n,v)=>n+v.books.length,0)}`;
  document.getElementById('tape').textContent = state.tape.join('\n');
}

function renderVenues() {
  const el = document.getElementById('venues');
  el.innerHTML = '';
  state.venues.forEach((venue, index) => {
    const b = document.createElement('button');
    b.className = index === venueIndex ? 'active' : '';
    b.innerHTML = `${venue.name}<br><span class="muted">${venue.tier}</span>`;
    b.onclick = () => { venueIndex = index; edgeIndex = 0; render(); };
    el.appendChild(b);
  });
}

function renderEdges() {
  const venue = state.venues[venueIndex];
  document.getElementById('venue-title').textContent = `${venue.name} (${venue.tier})`;
  const el = document.getElementById('edges');
  el.innerHTML = '';
  venue.symbols.forEach((symbol, index) => {
    const b = document.createElement('button');
    b.className = index === edgeIndex ? 'active' : '';
    b.textContent = `${symbol}/USD`;
    b.onmouseenter = () => selectEdge(index);
    b.onclick = () => selectEdge(index);
    el.appendChild(b);
  });
}

function renderGraph() {
  const venue = state.venues[venueIndex];
  const svg = document.getElementById('asset-graph');
  const cx = 360;
  const cy = 165;
  const r = 112;
  const points = venue.symbols.map((symbol, index) => {
    const angle = -Math.PI / 2 + index * Math.PI * 2 / venue.symbols.length;
    return {symbol, index, x: cx + Math.cos(angle) * r, y: cy + Math.sin(angle) * r};
  });

  svg.innerHTML = '';
  points.forEach(point => {
    const edge = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    edge.setAttribute('x1', cx);
    edge.setAttribute('y1', cy);
    edge.setAttribute('x2', point.x);
    edge.setAttribute('y2', point.y);
    edge.setAttribute('class', 'graph-edge' + (point.index === edgeIndex ? ' active' : ''));
    edge.onmouseenter = () => selectEdge(point.index);
    edge.onclick = () => selectEdge(point.index);
    svg.appendChild(edge);
  });

  points.forEach(point => {
    node(svg, point.x, point.y, point.symbol, 'graph-node');
    const label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    label.setAttribute('x', point.x);
    label.setAttribute('y', point.y);
    label.setAttribute('class', 'graph-label');
    label.textContent = point.symbol;
    svg.appendChild(label);
  });
  node(svg, cx, cy, 'USD', 'graph-node graph-center');
  const label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
  label.setAttribute('x', cx);
  label.setAttribute('y', cy);
  label.setAttribute('class', 'graph-label');
  label.textContent = 'USD';
  svg.appendChild(label);
}

function node(svg, x, y, label, className) {
  const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
  circle.setAttribute('cx', x);
  circle.setAttribute('cy', y);
  circle.setAttribute('r', label === 'USD' ? 28 : 24);
  circle.setAttribute('class', className);
  svg.appendChild(circle);
}

function selectEdge(index) {
  edgeIndex = index;
  renderEdges();
  renderGraph();
  renderBook();
}

function renderBook() {
  const venue = state.venues[venueIndex];
  const book = venue.books[edgeIndex];
  const symbol = venue.symbols[edgeIndex];
  document.getElementById('edge-title').textContent = `${venue.name} ${symbol}/USD`;
  document.getElementById('asks').innerHTML = rows([...book.asks].reverse(), 'ask');
  document.getElementById('bids').innerHTML = rows(book.bids, 'bid');
}

function rows(levels, side) {
  const max = Math.max(1, ...levels.map(([, q]) => q));
  return levels.map(([price, qty]) => {
    const width = Math.max(4, Math.round(qty * 100 / max));
    return `<tr><td>${qty}</td><td><span class="bar" style="width:${width}%"></span></td><td>${price}</td></tr>`;
  }).join('');
}

tick();
setInterval(tick, 250);
"#
}

fn write_global_lob_visualization(
    path: &str,
    venues: &[VenueRuntime],
    playback: &GlobalPlayback,
) -> std::io::Result<()> {
    let html = global_lob_visualization_html(venues, playback);
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, html)
}

fn global_lob_visualization_html(venues: &[VenueRuntime], playback: &GlobalPlayback) -> String {
    let mut html = String::new();
    let _ = writeln!(
        html,
        "<!doctype html><meta charset=\"utf-8\"><title>global-lob viewer</title>"
    );
    let _ = writeln!(
        html,
        "<style>body{{margin:0;font-family:system-ui,sans-serif;background:#f5f7fa;color:#17202a}}#app{{display:grid;grid-template-columns:360px 1fr;min-height:100vh}}aside{{background:#18202a;color:white;padding:18px;overflow:auto}}main{{padding:18px;display:grid;grid-template-rows:auto 1fr;gap:14px}}button{{font:inherit;border:1px solid #c8ced8;background:white;border-radius:6px;padding:7px 9px;cursor:pointer}}button.active{{background:#1d6fbe;color:white;border-color:#1d6fbe}}.venue-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}}.venue{{height:46px}}.panel{{background:white;border:1px solid #d8dde6;border-radius:8px;padding:14px}}.edge-list{{display:flex;flex-wrap:wrap;gap:6px;max-height:180px;overflow:auto}}.book{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}table{{width:100%;border-collapse:collapse}}td,th{{padding:4px 6px;border-bottom:1px solid #edf0f4;text-align:right}}th:first-child,td:first-child{{text-align:left}}.asks td{{color:#a40e26}}.bids td{{color:#22863a}}.tape{{max-height:160px;overflow:auto;font-family:ui-monospace,monospace;font-size:12px;background:#101820;color:#d6f0ff;padding:10px;border-radius:6px}}.muted{{color:#667085}}.row{{display:flex;gap:8px;align-items:center;flex-wrap:wrap}}</style>"
    );
    let _ = writeln!(html, "<div id=\"app\"><aside><h1>global-lob</h1><p>Click a venue, then an edge. Press play to step through recorded public events for that book.</p><div id=\"venues\" class=\"venue-grid\"></div></aside><main><section class=\"panel\"><h2 id=\"venue-title\">Choose a venue</h2><p id=\"venue-meta\" class=\"muted\"></p><div id=\"edges\" class=\"edge-list\"></div></section><section class=\"panel\"><div class=\"row\"><h2 id=\"edge-title\">No edge selected</h2><button id=\"prev\">Prev</button><button id=\"play\">Play</button><button id=\"next\">Next</button><span id=\"frame-label\" class=\"muted\"></span></div><div class=\"book\"><div><h3>Asks</h3><table class=\"asks\"><tbody id=\"asks\"></tbody></table></div><div><h3>Bids</h3><table class=\"bids\"><tbody id=\"bids\"></tbody></table></div></div><h3>Event tape</h3><div id=\"tape\" class=\"tape\"></div></section></main></div>");
    let _ = writeln!(
        html,
        "<script>const DATA={};\n{}</script>",
        global_lob_json(venues, playback),
        global_lob_viewer_js()
    );
    html
}

fn global_lob_json(venues: &[VenueRuntime], playback: &GlobalPlayback) -> String {
    let mut json = String::new();
    let _ = write!(json, "{{\"venues\":[");
    for (venue_index, venue) in venues.iter().enumerate() {
        if venue_index > 0 {
            json.push(',');
        }
        let _ = write!(
            json,
            "{{\"name\":\"{}\",\"tier\":\"{}\",\"symbols\":[",
            json_escape(&venue.name),
            json_escape(&venue.tier)
        );
        for (symbol_index, symbol) in venue.symbols.iter().enumerate() {
            if symbol_index > 0 {
                json.push(',');
            }
            let _ = write!(json, "\"{}\"", json_escape(symbol));
        }
        json.push_str("]}");
    }
    json.push_str("],\"playback\":{");
    for (index, (edge, frames)) in playback.frames_by_edge.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        let _ = write!(json, "\"{}:{}\":[", edge.venue_index, edge.instrument_id);
        for (frame_index, frame) in frames.iter().enumerate() {
            if frame_index > 0 {
                json.push(',');
            }
            let _ = write!(json, "{{\"step\":{},\"bids\":[", frame.step);
            write_level_json(&mut json, &frame.bids);
            json.push_str("],\"asks\":[");
            write_level_json(&mut json, &frame.asks);
            json.push_str("],\"events\":[");
            for (event_index, event) in frame.events.iter().enumerate() {
                if event_index > 0 {
                    json.push(',');
                }
                let _ = write!(json, "\"{}\"", json_escape(event));
            }
            json.push_str("]}");
        }
        json.push(']');
    }
    json.push_str("}}");
    json
}

fn write_level_json(json: &mut String, levels: &[(u64, u64)]) {
    for (index, (price, quantity)) in levels.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        let _ = write!(json, "[{price},{quantity}]");
    }
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn global_lob_viewer_js() -> &'static str {
    r#"
let venueIndex = 0;
let edgeKey = null;
let frameIndex = 0;
let timer = null;

const venuesEl = document.getElementById('venues');
const edgesEl = document.getElementById('edges');
const venueTitle = document.getElementById('venue-title');
const venueMeta = document.getElementById('venue-meta');
const edgeTitle = document.getElementById('edge-title');
const frameLabel = document.getElementById('frame-label');
const bidsEl = document.getElementById('bids');
const asksEl = document.getElementById('asks');
const tapeEl = document.getElementById('tape');
const playEl = document.getElementById('play');

function renderVenues() {
  venuesEl.innerHTML = '';
  DATA.venues.forEach((venue, index) => {
    const button = document.createElement('button');
    button.className = 'venue' + (index === venueIndex ? ' active' : '');
    button.textContent = index + 1;
    button.title = `${venue.name} (${venue.tier})`;
    button.onclick = () => selectVenue(index);
    venuesEl.appendChild(button);
  });
}

function selectVenue(index) {
  venueIndex = index;
  const venue = DATA.venues[index];
  venueTitle.textContent = venue.name;
  venueMeta.textContent = `${venue.tier}, ${venue.symbols.length} edges`;
  edgesEl.innerHTML = '';
  venue.symbols.forEach((symbol, instrumentId) => {
    const key = `${index}:${instrumentId}`;
    const button = document.createElement('button');
    button.textContent = symbol + '/USD';
    button.className = key === edgeKey ? 'active' : '';
    button.title = DATA.playback[key] ? `${DATA.playback[key].length} recorded frames` : 'No recorded events in this run';
    button.onclick = () => selectEdge(key, symbol);
    edgesEl.appendChild(button);
  });
  renderVenues();
}

function selectEdge(key, symbol) {
  stop();
  edgeKey = key;
  frameIndex = 0;
  edgeTitle.textContent = `${DATA.venues[venueIndex].name} ${symbol}/USD`;
  selectVenue(venueIndex);
  renderFrame();
}

function frames() {
  return edgeKey ? (DATA.playback[edgeKey] || []) : [];
}

function renderFrame() {
  const current = frames();
  if (!current.length) {
    bidsEl.innerHTML = '';
    asksEl.innerHTML = '';
    tapeEl.textContent = 'No public events were recorded for this edge in the loaded run.';
    frameLabel.textContent = '';
    return;
  }
  const frame = current[frameIndex];
  bidsEl.innerHTML = rows(frame.bids);
  asksEl.innerHTML = rows(frame.asks);
  tapeEl.textContent = frame.events.join('\n');
  frameLabel.textContent = `frame ${frameIndex + 1}/${current.length}, step ${frame.step}`;
}

function rows(levels) {
  return levels.map(([price, qty]) => `<tr><td>${qty}</td><td>${price}</td></tr>`).join('');
}

function step(delta) {
  const current = frames();
  if (!current.length) return;
  frameIndex = (frameIndex + delta + current.length) % current.length;
  renderFrame();
}

function stop() {
  if (timer) clearInterval(timer);
  timer = null;
  playEl.textContent = 'Play';
}

function togglePlay() {
  if (timer) {
    stop();
  } else {
    timer = setInterval(() => step(1), 350);
    playEl.textContent = 'Pause';
  }
}

document.getElementById('prev').onclick = () => step(-1);
document.getElementById('next').onclick = () => step(1);
playEl.onclick = togglePlay;
renderVenues();
selectVenue(0);
"#
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

fn format_public_event(event: &BookEvent) -> String {
    if is_public_event(event) {
        event_line(event)
    } else {
        String::new()
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

fn parse_string_arg(args: &mut impl Iterator<Item = String>, name: &str) -> String {
    args.next()
        .unwrap_or_else(|| panic!("{name} requires a value"))
}

fn parse_world(args: &mut impl Iterator<Item = String>) -> SimWorld {
    match args.next().as_deref() {
        Some("single-bin") => SimWorld::SingleBin,
        Some("global-lob") => SimWorld::GlobalLob,
        Some("shock-demo") => SimWorld::ShockDemo,
        Some("live-demo") => SimWorld::LiveDemo,
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

    #[test]
    fn shock_demo_writes_visualization() {
        let path = "/tmp/exch-shock-demo-test.html";
        let config = SimConfig {
            world: SimWorld::ShockDemo,
            commands: 80,
            traders: 20,
            feed_subscribers: 1,
            visualization: Some(path.to_string()),
            live_addr: None,
        };

        let result = run_shock_demo(&config);
        assert_eq!(result.world, "shock-demo");
        assert_eq!(result.visualization_path.as_deref(), Some(path));

        let html = fs::read_to_string(path).expect("shock visualization should be readable");
        assert!(html.contains("<svg"));
        assert!(html.contains("shock-demo"));
        assert!(html.contains("large buy shock"));
    }

    #[test]
    fn global_lob_writes_clickable_viewer() {
        let path = "/tmp/exch-global-lob-viewer-test.html";
        let config = SimConfig {
            world: SimWorld::GlobalLob,
            commands: 120,
            traders: 20,
            feed_subscribers: 1,
            visualization: Some(path.to_string()),
            live_addr: None,
        };

        let result = run_global_lob_sim(&config);
        assert_eq!(result.world, "global-lob");
        assert_eq!(result.visualization_path.as_deref(), Some(path));

        let html = fs::read_to_string(path).expect("global-lob viewer should be readable");
        assert!(html.contains("global-lob viewer"));
        assert!(html.contains("equity-global-00"));
        assert!(html.contains("\"playback\""));
        assert!(html.contains("function selectVenue"));
    }

    #[test]
    fn live_demo_state_contains_active_books() {
        let venues = live_demo_world(16);
        let state = live_state_from_venues(42, &venues, VecDeque::from(["event".to_string()]));
        let json = live_state_json(&state);

        assert_eq!(state.venues.len(), 4);
        assert_eq!(state.venues[0].books.len(), 6);
        assert!(json.contains("\"step\":42"));
        assert!(json.contains("ny-core"));
        assert!(json.contains("\"bids\""));
        let html = live_demo_html();
        assert!(html.contains("exch live"));
        assert!(html.contains("asset-graph"));
        assert!(html.contains("graph-edge"));
        assert!(html.contains("function renderGraph"));
    }
}
