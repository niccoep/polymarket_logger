
use crate::traits::BookProcessor;
use crate::reconstruction::OrderBook;

pub struct StatsAccumulator {
    pub count: usize,
}


impl BookProcessor for StatsAccumulator {
    type Output = usize;

    fn new() -> Self {
        Self {
            count: 0
        }
    }

    fn process(&mut self, timestamp: i64, books: &[OrderBook; 2]) {
        self.count += 1;
    }

    fn finalize(self) -> Self::Output {
        self.count
    }
}
