use std::collections::{BTreeMap, VecDeque};

pub type OrderId = u64;
pub type AccountId = u64;
pub type InstrumentId = u32;
pub type Quantity = u64;
pub type Sequence = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Asset {
    pub symbol: String,
}

impl Asset {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instrument {
    pub id: InstrumentId,
    pub base: Asset,
    pub quote: Asset,
    pub price_tick: Price,
    pub quantity_step: Quantity,
}

impl Instrument {
    pub fn symbol(&self) -> String {
        format!("{}/{}", self.base.symbol, self.quote.symbol)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueConfig {
    pub name: String,
    pub instruments: Vec<Instrument>,
    pub default_snapshot_depth: usize,
}

impl VenueConfig {
    pub fn star(
        name: impl Into<String>,
        center: impl Into<String>,
        spokes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let center = Asset::new(center);
        let instruments = spokes
            .into_iter()
            .enumerate()
            .map(|(index, spoke)| Instrument {
                id: index as InstrumentId,
                base: Asset::new(spoke),
                quote: center.clone(),
                price_tick: Price(1),
                quantity_step: 1,
            })
            .collect();

        Self {
            name: name.into(),
            instruments,
            default_snapshot_depth: 10,
        }
    }

    pub fn complete_currency_graph(
        name: impl Into<String>,
        currencies: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let assets = currencies.into_iter().map(Asset::new).collect::<Vec<_>>();
        let mut instruments = Vec::new();

        for base_index in 0..assets.len() {
            for quote_index in (base_index + 1)..assets.len() {
                instruments.push(Instrument {
                    id: instruments.len() as InstrumentId,
                    base: assets[base_index].clone(),
                    quote: assets[quote_index].clone(),
                    price_tick: Price(1),
                    quantity_step: 1,
                });
            }
        }

        Self {
            name: name.into(),
            instruments,
            default_snapshot_depth: 10,
        }
    }

    pub fn deterministic_sparse_currency_graph(
        name: impl Into<String>,
        currencies: impl IntoIterator<Item = impl Into<String>>,
        edge_count: usize,
        seed: u64,
    ) -> Self {
        let assets = currencies.into_iter().map(Asset::new).collect::<Vec<_>>();
        let mut pairs = Vec::new();

        for base_index in 0..assets.len() {
            for quote_index in (base_index + 1)..assets.len() {
                pairs.push((base_index, quote_index));
            }
        }

        shuffle_pairs(&mut pairs, seed);
        pairs.truncate(edge_count.min(pairs.len()));

        let instruments = pairs
            .into_iter()
            .enumerate()
            .map(|(index, (base_index, quote_index))| Instrument {
                id: index as InstrumentId,
                base: assets[base_index].clone(),
                quote: assets[quote_index].clone(),
                price_tick: Price(1),
                quantity_step: 1,
            })
            .collect();

        Self {
            name: name.into(),
            instruments,
            default_snapshot_depth: 10,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl std::str::FromStr for Side {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "buy" | "BUY" | "bid" | "BID" => Ok(Side::Buy),
            "sell" | "SELL" | "ask" | "ASK" => Ok(Side::Sell),
            _ => Err(()),
        }
    }
}

impl Side {
    fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOrder {
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub side: Side,
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug)]
pub struct Venue {
    config: VenueConfig,
    books: BTreeMap<InstrumentId, OrderBook>,
}

impl Venue {
    pub fn new(config: VenueConfig) -> Self {
        let books = config
            .instruments
            .iter()
            .map(|instrument| (instrument.id, OrderBook::new()))
            .collect();

        Self { config, books }
    }

    pub fn config(&self) -> &VenueConfig {
        &self.config
    }

    pub fn submit_limit(
        &mut self,
        instrument_id: InstrumentId,
        order: NewOrder,
    ) -> Result<Vec<BookEvent>, VenueError> {
        self.book_mut(instrument_id)
            .map(|book| book.submit_limit(order))
    }

    pub fn cancel(
        &mut self,
        instrument_id: InstrumentId,
        order_id: OrderId,
    ) -> Result<BookEvent, VenueError> {
        self.book_mut(instrument_id)
            .map(|book| book.cancel(order_id))
    }

    pub fn snapshot(
        &self,
        instrument_id: InstrumentId,
        depth: usize,
    ) -> Result<BookSnapshot, VenueError> {
        self.books
            .get(&instrument_id)
            .map(|book| book.snapshot(depth))
            .ok_or(VenueError::UnknownInstrument)
    }

    fn book_mut(&mut self, instrument_id: InstrumentId) -> Result<&mut OrderBook, VenueError> {
        self.books
            .get_mut(&instrument_id)
            .ok_or(VenueError::UnknownInstrument)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VenueError {
    UnknownInstrument,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestingOrder {
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execution {
    pub resting_order_id: OrderId,
    pub aggressing_order_id: OrderId,
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookEvent {
    Accepted {
        seq: Sequence,
        order_id: OrderId,
    },
    Executed {
        seq: Sequence,
        execution: Execution,
    },
    Rested {
        seq: Sequence,
        order_id: OrderId,
        side: Side,
        price: Price,
        quantity: Quantity,
    },
    Canceled {
        seq: Sequence,
        order_id: OrderId,
        side: Side,
        price: Price,
        quantity: Quantity,
    },
    Rejected {
        seq: Sequence,
        order_id: OrderId,
        reason: RejectReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    DuplicateOrderId,
    UnknownOrderId,
    ZeroQuantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshot {
    pub seq: Sequence,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub checksum: u32,
}

#[derive(Debug, Default)]
pub struct OrderBook {
    seq: Sequence,
    bids: BTreeMap<Price, VecDeque<RestingOrder>>,
    asks: BTreeMap<Price, VecDeque<RestingOrder>>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn submit_limit(&mut self, order: NewOrder) -> Vec<BookEvent> {
        let mut events = Vec::new();

        if order.quantity == 0 {
            events.push(self.rejected(order.order_id, RejectReason::ZeroQuantity));
            return events;
        }

        if self.contains_order(order.order_id) {
            events.push(self.rejected(order.order_id, RejectReason::DuplicateOrderId));
            return events;
        }

        events.push(self.accepted(order.order_id));

        let mut remaining = order.quantity;
        while remaining > 0 {
            let Some(best_price) = self.matchable_price(order.side, order.price) else {
                break;
            };

            let fill = self.fill_at_price(
                order.side.opposite(),
                best_price,
                order.order_id,
                &mut remaining,
            );
            events.extend(fill);
        }

        if remaining > 0 {
            self.side_mut(order.side)
                .entry(order.price)
                .or_default()
                .push_back(RestingOrder {
                    order_id: order.order_id,
                    account_id: order.account_id,
                    quantity: remaining,
                });
            events.push(self.rested(order.order_id, order.side, order.price, remaining));
        }

        events
    }

    pub fn cancel(&mut self, order_id: OrderId) -> BookEvent {
        if let Some((side, price, quantity)) = self.remove_order(order_id) {
            self.canceled(order_id, side, price, quantity)
        } else {
            self.rejected(order_id, RejectReason::UnknownOrderId)
        }
    }

    pub fn snapshot(&self, depth: usize) -> BookSnapshot {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(depth)
            .map(|(price, orders)| Level {
                price: *price,
                quantity: orders.iter().map(|order| order.quantity).sum(),
            })
            .collect::<Vec<_>>();
        let asks = self
            .asks
            .iter()
            .take(depth)
            .map(|(price, orders)| Level {
                price: *price,
                quantity: orders.iter().map(|order| order.quantity).sum(),
            })
            .collect::<Vec<_>>();
        let checksum = checksum(&bids, &asks);

        BookSnapshot {
            seq: self.seq,
            bids,
            asks,
            checksum,
        }
    }

    fn matchable_price(&self, side: Side, limit_price: Price) -> Option<Price> {
        match side {
            Side::Buy => self
                .asks
                .first_key_value()
                .and_then(|(price, _)| (*price <= limit_price).then_some(*price)),
            Side::Sell => self
                .bids
                .last_key_value()
                .and_then(|(price, _)| (*price >= limit_price).then_some(*price)),
        }
    }

    fn fill_at_price(
        &mut self,
        resting_side: Side,
        price: Price,
        aggressing_order_id: OrderId,
        remaining: &mut Quantity,
    ) -> Vec<BookEvent> {
        let mut executions = Vec::new();
        let mut remove_level = false;

        if let Some(level) = self.side_mut(resting_side).get_mut(&price) {
            while *remaining > 0 {
                let Some(resting) = level.front_mut() else {
                    remove_level = true;
                    break;
                };

                let fill_qty = (*remaining).min(resting.quantity);
                resting.quantity -= fill_qty;
                *remaining -= fill_qty;

                executions.push(Execution {
                    resting_order_id: resting.order_id,
                    aggressing_order_id,
                    price,
                    quantity: fill_qty,
                });

                if resting.quantity == 0 {
                    level.pop_front();
                }
            }

            if level.is_empty() {
                remove_level = true;
            }
        }

        if remove_level {
            self.side_mut(resting_side).remove(&price);
        }

        executions
            .into_iter()
            .map(|execution| self.executed(execution))
            .collect()
    }

    fn remove_order(&mut self, order_id: OrderId) -> Option<(Side, Price, Quantity)> {
        Self::remove_from_side(&mut self.bids, order_id)
            .map(|(price, quantity)| (Side::Buy, price, quantity))
            .or_else(|| {
                Self::remove_from_side(&mut self.asks, order_id)
                    .map(|(price, quantity)| (Side::Sell, price, quantity))
            })
    }

    fn remove_from_side(
        book_side: &mut BTreeMap<Price, VecDeque<RestingOrder>>,
        order_id: OrderId,
    ) -> Option<(Price, Quantity)> {
        let price = book_side.iter().find_map(|(price, orders)| {
            orders
                .iter()
                .any(|order| order.order_id == order_id)
                .then_some(*price)
        })?;

        let orders = book_side.get_mut(&price)?;
        let index = orders.iter().position(|order| order.order_id == order_id)?;
        let removed = orders.remove(index)?;
        if orders.is_empty() {
            book_side.remove(&price);
        }

        Some((price, removed.quantity))
    }

    fn contains_order(&self, order_id: OrderId) -> bool {
        self.bids
            .values()
            .chain(self.asks.values())
            .any(|orders| orders.iter().any(|order| order.order_id == order_id))
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<Price, VecDeque<RestingOrder>> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn accepted(&mut self, order_id: OrderId) -> BookEvent {
        self.seq += 1;
        BookEvent::Accepted {
            seq: self.seq,
            order_id,
        }
    }

    fn executed(&mut self, execution: Execution) -> BookEvent {
        self.seq += 1;
        BookEvent::Executed {
            seq: self.seq,
            execution,
        }
    }

    fn rested(
        &mut self,
        order_id: OrderId,
        side: Side,
        price: Price,
        quantity: Quantity,
    ) -> BookEvent {
        self.seq += 1;
        BookEvent::Rested {
            seq: self.seq,
            order_id,
            side,
            price,
            quantity,
        }
    }

    fn canceled(
        &mut self,
        order_id: OrderId,
        side: Side,
        price: Price,
        quantity: Quantity,
    ) -> BookEvent {
        self.seq += 1;
        BookEvent::Canceled {
            seq: self.seq,
            order_id,
            side,
            price,
            quantity,
        }
    }

    fn rejected(&mut self, order_id: OrderId, reason: RejectReason) -> BookEvent {
        self.seq += 1;
        BookEvent::Rejected {
            seq: self.seq,
            order_id,
            reason,
        }
    }
}

pub fn checksum(bids: &[Level], asks: &[Level]) -> u32 {
    let mut crc = Crc32::new();

    for level in asks.iter().chain(bids.iter()) {
        crc.update_u64(level.price.0);
        crc.update_u64(level.quantity);
    }

    crc.finish()
}

struct Crc32(u32);

impl Crc32 {
    fn new() -> Self {
        Self(0xffff_ffff)
    }

    fn update_u64(&mut self, value: u64) {
        for byte in value.to_ascii_bytes() {
            self.update_byte(byte);
        }
    }

    fn update_byte(&mut self, byte: u8) {
        self.0 ^= byte as u32;
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(self.0 & 1);
            self.0 = (self.0 >> 1) ^ (0xedb8_8320 & mask);
        }
    }

    fn finish(self) -> u32 {
        !self.0
    }
}

trait AsciiU64 {
    fn to_ascii_bytes(self) -> Vec<u8>;
}

impl AsciiU64 for u64 {
    fn to_ascii_bytes(self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

fn shuffle_pairs(pairs: &mut [(usize, usize)], seed: u64) {
    let mut rng = DeterministicRng::new(seed);
    for index in (1..pairs.len()).rev() {
        let swap_with = rng.next_usize(index + 1);
        pairs.swap(index, swap_with);
    }
}

struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_usize(&mut self, modulo: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        (self.0 as usize) % modulo
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rests_non_marketable_limit_order() {
        let mut book = OrderBook::new();

        let events = book.submit_limit(NewOrder {
            order_id: 1,
            account_id: 10,
            side: Side::Buy,
            price: Price(100),
            quantity: 25,
        });

        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], BookEvent::Accepted { order_id: 1, .. }));
        assert!(matches!(
            events[1],
            BookEvent::Rested {
                order_id: 1,
                quantity: 25,
                ..
            }
        ));
        assert_eq!(
            book.snapshot(10).bids,
            vec![Level {
                price: Price(100),
                quantity: 25
            }]
        );
    }

    #[test]
    fn matches_price_time_priority_and_reports_executions_on_book_feed() {
        let mut book = OrderBook::new();
        book.submit_limit(order(1, Side::Sell, 101, 10));
        book.submit_limit(order(2, Side::Sell, 101, 15));

        let events = book.submit_limit(order(3, Side::Buy, 101, 20));

        assert!(matches!(
            &events[1],
            BookEvent::Executed {
                execution: Execution {
                    resting_order_id: 1,
                    aggressing_order_id: 3,
                    price: Price(101),
                    quantity: 10,
                },
                ..
            }
        ));
        assert!(matches!(
            &events[2],
            BookEvent::Executed {
                execution: Execution {
                    resting_order_id: 2,
                    aggressing_order_id: 3,
                    price: Price(101),
                    quantity: 10,
                },
                ..
            }
        ));
        assert_eq!(
            book.snapshot(10).asks,
            vec![Level {
                price: Price(101),
                quantity: 5
            }]
        );
    }

    #[test]
    fn cancels_resting_order() {
        let mut book = OrderBook::new();
        book.submit_limit(order(1, Side::Buy, 99, 7));

        let event = book.cancel(1);

        assert!(matches!(
            event,
            BookEvent::Canceled {
                order_id: 1,
                side: Side::Buy,
                price: Price(99),
                quantity: 7,
                ..
            }
        ));
        assert!(book.snapshot(10).bids.is_empty());
    }

    #[test]
    fn cancellation_searches_sell_side_after_buy_side() {
        let mut book = OrderBook::new();
        book.submit_limit(order(1, Side::Sell, 101, 7));

        let event = book.cancel(1);

        assert!(matches!(
            event,
            BookEvent::Canceled {
                order_id: 1,
                side: Side::Sell,
                price: Price(101),
                quantity: 7,
                ..
            }
        ));
        assert!(book.snapshot(10).asks.is_empty());
    }

    #[test]
    fn builds_star_venue_config_for_equities_style_markets() {
        let config = VenueConfig::star("equities", "USD", ["AAA", "BBB", "CCC"]);

        assert_eq!(config.instruments.len(), 3);
        assert_eq!(config.instruments[0].symbol(), "AAA/USD");
        assert_eq!(config.instruments[2].symbol(), "CCC/USD");
    }

    #[test]
    fn builds_deterministic_sparse_currency_graph() {
        let left = VenueConfig::deterministic_sparse_currency_graph(
            "fx",
            ["USD", "EUR", "JPY", "GBP"],
            4,
            99,
        );
        let right = VenueConfig::deterministic_sparse_currency_graph(
            "fx",
            ["USD", "EUR", "JPY", "GBP"],
            4,
            99,
        );

        assert_eq!(left.instruments, right.instruments);
        assert_eq!(left.instruments.len(), 4);
    }

    fn order(order_id: OrderId, side: Side, price: u64, quantity: Quantity) -> NewOrder {
        NewOrder {
            order_id,
            account_id: 1,
            side,
            price: Price(price),
            quantity,
        }
    }
}
