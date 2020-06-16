use crate::displacement::Premium;
use crate::ema::Ema;
use crate::fair_value::FairValue;
use crate::local_book::BookImprovedSignal;
use crate::remote_venue_aggregator::RemoteVenueAggregator;
use crate::signal_graph::graph_error::GraphError;
use crate::signal_graph::graph_registrar::*;

pub fn generate_registrar() -> Result<GraphRegistrar, GraphError> {
    GraphRegistrar::new(&[
        ("book_fair", make_signal_for::<FairValue>()),
        ("aggregator", make_signal_for::<RemoteVenueAggregator>()),
        ("ema", make_signal_for::<Ema>()),
        ("bbo_improved", make_signal_for::<BookImprovedSignal>()),
        ("premium", make_signal_for::<Premium>()),
    ])
}
