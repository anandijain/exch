use exchange_core::{BookEvent, NewOrder, Price, Side, Venue, VenueConfig};
use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::time::{Duration, Instant};

fn main() {
    let config = SimConfig::from_env();
    let result = match config.world {
        SimWorld::SingleBin => run_single_bin_sim(&config),
        SimWorld::GlobalLob => run_global_lob_sim(&config),
        SimWorld::ShockDemo => run_shock_demo(&config),
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimWorld {
    SingleBin,
    GlobalLob,
    ShockDemo,
}

impl SimConfig {
    fn from_env() -> Self {
        let mut config = Self {
            world: SimWorld::SingleBin,
            commands: 100_000,
            traders: 100,
            feed_subscribers: 1,
            visualization: None,
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
                "--help" => {
                    println!(
                        "usage: cargo run -p exchange_sim -- --world single-bin --commands 100000 --traders 100 --feed-subscribers 1\nworlds: single-bin, global-lob, shock-demo"
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

fn parse_string_arg(args: &mut impl Iterator<Item = String>, name: &str) -> String {
    args.next()
        .unwrap_or_else(|| panic!("{name} requires a value"))
}

fn parse_world(args: &mut impl Iterator<Item = String>) -> SimWorld {
    match args.next().as_deref() {
        Some("single-bin") => SimWorld::SingleBin,
        Some("global-lob") => SimWorld::GlobalLob,
        Some("shock-demo") => SimWorld::ShockDemo,
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
}
