use crate::normalized::MarketEventBlock;

pub enum LoggedMarketEvent {
    // Tags to ensure that the separate market data files overlap
    Snapshot(MarketEventBlock),
    Event(MarketEventBlock)
}
