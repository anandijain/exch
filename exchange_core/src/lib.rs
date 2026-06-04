use std::collections::{BTreeMap, VecDeque};

pub type OrderId = u64;
pub type AccountId = u64;
pub type Quantity = u64;
pub type Sequence = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
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
