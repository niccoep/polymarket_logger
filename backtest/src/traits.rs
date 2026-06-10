
use crate::reconstruction::OrderBook;

pub trait BookProcessor {
    type Output;
    fn new() -> Self;
    fn process(&mut self, timestamp: i64, books: &[OrderBook; 2]);
    fn finalize(self) -> Self::Output;
}
