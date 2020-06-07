use std::any::Any;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::rc::Rc;

use super::graph_error::GraphError;
use super::graph_registrar::*;
use super::graph_sort::{find_seen_signals, topological_sort};
use super::interface_types::*;
use super::security_data::SecurityVector;
use super::security_index::{SecurityIndex, SecurityMap};

use dynstack::DynStack;

pub struct GraphInnerMem {
    pub output_values: Vec<Cell<f64>>,
    pub mark_as_written: Vec<Cell<u64>>,
    pub mark_as_valid: Vec<Cell<u64>>,
    pub(crate) aggregate_mapping_array: Vec<u16>,
    pub(crate) signal_name_to_index: HashMap<String, u16>,
    pub(crate) signal_name_to_aggregate: HashMap<(String, String), Range<u16>>,
    pub(crate) signal_name_to_consumer: HashMap<(String, String), u16>,
    pub(crate) signal_name_to_instance: HashMap<String, SignalInstantiation>,
}

pub struct GraphAggregateData {
    pub aggregate_signals: Vec<AggregateSignal>,
    pub(crate) signal_name_to_aggregate_index: HashMap<(String, String), u16>,
}

pub(crate) struct GraphObjectStore {
    // TODO TODO TODO write our own which separates out the vtables from the objects
    // BIG todo for nontrivial use cases
    pub(crate) objects: DynStack<RawObj>,
    pub(crate) raw_pointers: Vec<*mut u8>,
}

// returns bitmap_offset
#[inline(always)]
pub(crate) fn index_to_bitmap(index: u16) -> (u16, u16) {
    let offset = index / 64;
    let bit = index % 64;
    (offset, bit)
}

pub(crate) const SET_BIT: usize = 1;
pub(crate) const VALID_BIT: usize = 1; 

// This could actually pack 24 bits as the aggregate offset, BUT
// that doesn't seem needed
#[derive(Copy, Clone)]
pub enum ConsumerOrAggregate {
    Aggregate(u32, u16),
    Consumer(i32),
}

pub(crate) struct GraphCallList {
    pub(crate) book_calls: Vec<(BookCallback, *mut u8, *const Cell<f64>)>,
    pub(crate) inner_calls: Vec<(
        InnerCallback,
        ConsumerOrAggregate,
        *mut u8,
        *const Cell<f64>,
    )>,
    pub(crate) hold_objects: Rc<GraphObjectStore>,
    pub(crate) mem: Rc<GraphInnerMem>,
    pub(crate) mark_as_clean: Vec<u16>,
}

pub struct Graph {
    pub(crate) book_updates: SecurityVector<Option<GraphCallList>>,
    pub(crate) agg_data: GraphAggregateData,
    pub(crate) objects: Rc<GraphObjectStore>,
    pub(crate) mem: Rc<GraphInnerMem>,
}

impl NamedSignalType {
    fn is_book(&self) -> bool {
        match self {
            NamedSignalType::Book(_) => true,
            _ => false,
        }
    }
}

impl GraphInnerMem {
    pub(crate) fn new(
        signal_name_to_instance: HashMap<String, SignalInstantiation>,
        layout: &[(String, SignalCall)],
        security_map: &SecurityMap,
    ) -> Result<Rc<GraphInnerMem>, GraphError> {
        assert!(signal_name_to_instance.len() < std::u16::MAX as usize);

        // TODO code duplication here and registrar
        let requested_book_signals: HashSet<_> = signal_name_to_instance
            .iter()
            .map(|(_, b)| b)
            .flat_map(|sc| sc.inputs.values())
            .filter_map(|nst| match nst {
                NamedSignalType::Book(sec) => Some(sec),
                _ => None,
            })
            .collect();

        for security in &requested_book_signals {
            let security = *security;
            if security_map.to_index(security).is_none() {
                return Err(GraphError::BookNotFound {
                    security: security.clone(),
                });
            }
        }

        let security_call_list_justnames = SecurityVector::new_with_err(security_map, |sec, _| {
            if requested_book_signals.contains(sec) {
                let (seen_signals, book_signals) =
                    find_seen_signals(sec, &signal_name_to_instance)?;
                let sorted_order = topological_sort(
                    &seen_signals,
                    book_signals.into_iter().collect(),
                    &signal_name_to_instance,
                );
                Ok(Some(sorted_order))
            } else {
                Ok(None)
            }
        })?;

        let mut signal_name_to_index = HashMap::new();
        let mut index_so_far: u16 = 0;

        security_call_list_justnames.for_each(|sigs| {
            if let Some(sigs) = sigs {
                for (sig, input) in sigs {
                    if !signal_name_to_index.contains_key(sig) {
                        signal_name_to_index.insert(sig.clone(), index_so_far);
                        index_so_far += 1;
                    }
                }
            }
        });

        for signal in signal_name_to_instance.keys() {
            if !signal_name_to_index.contains_key(signal) {
                signal_name_to_index.insert(signal.clone(), index_so_far);
                index_so_far += 1;
            }
        }

        // TODO what to do about unseen signals? Does it even matter?
        // // TODO handle unused signals

        assert_eq!(signal_name_to_index.len(), signal_name_to_instance.len());

        let output_values: Vec<_> = signal_name_to_index
            .iter()
            .map(|_| Cell::new(0.0))
            .collect();

        let bitmap_estimate = 1 + output_values.len() / 64;

        let mark_as_written: Vec<_> = output_values
            .iter()
            .map(|_| Cell::new(0))
            .take(bitmap_estimate)
            .collect();

        let mut aggregate_mapping_array = Vec::new();
        let mut signal_name_to_aggregate = HashMap::<_, _>::new();
        for ((name, signal), parents) in signal_name_to_instance
            .iter()
            .flat_map(|(signal, parents)| {
                let signal = signal.clone();
                parents
                    .inputs
                    .iter()
                    .map(move |(name, sig)| ((name, signal.clone()), sig))
            })
            .map(|(tag, parents)| {
                (
                    tag,
                    match parents {
                        NamedSignalType::Inner(true, parents) => parents.clone(),
                        _ => vec![],
                    },
                )
            })
        {
            let range_start = aggregate_mapping_array.len() as u16;
            for parent in parents {
                aggregate_mapping_array.push(
                    if let Some(index) = signal_name_to_index.get(&parent) {
                        *index
                    } else {
                        return Err(GraphError::ParentNotFound {
                            child: signal.clone(),
                            parent: parent.clone(),
                            input: name.clone(),
                        });
                    },
                )
            }
            let range_end = aggregate_mapping_array.len() as u16;
            signal_name_to_aggregate.insert((name.clone(), signal.clone()), range_start..range_end);
        }

        let mut signal_name_to_consumer = HashMap::new();
        for ((name, signal), parent) in signal_name_to_instance
            .iter()
            .flat_map(|(signal, parents)| {
                // for god's sake borrowchecker this is valid without the extra clones
                let signal = signal.clone();
                parents
                    .inputs
                    .iter()
                    .map(move |(name, sig)| ((name, signal.clone()), sig))
            })
            .filter_map(|(tag, parents)| match parents {
                NamedSignalType::Inner(false, parent) => Some((tag, &parent[0])),
                _ => None,
            })
        {
            let key = (name.clone(), signal.clone());
            if let Some(index) = signal_name_to_index.get(parent) {
                signal_name_to_consumer.insert(key, *index)
            } else {
                return Err(GraphError::ParentNotFound {
                    child: signal.clone(),
                    parent: parent.clone(),
                    input: name.clone(),
                });
            };
        }

        let mut rval = Rc::new(GraphInnerMem {
            output_values,
            mark_as_valid: mark_as_written.clone(),
            mark_as_written,
            aggregate_mapping_array,
            signal_name_to_index,
            signal_name_to_aggregate,
            signal_name_to_consumer,
            signal_name_to_instance,
        });

        // for each signal, now give it an aggregate signal

        Ok(rval)
    }
}

impl GraphAggregateData {
    pub fn new(mem: Rc<GraphInnerMem>) -> GraphAggregateData {
        let mut aggregate_signals = Vec::new();
        let mut signal_name_to_aggregate_index = HashMap::new();

        for (index, (key, range)) in mem.signal_name_to_aggregate.iter().enumerate() {
            let agg_signal = AggregateSignal {
                graph: mem.clone(),
                offsets: range.clone(),
            };

            assert_eq!(index, aggregate_signals.len());
            aggregate_signals.push(agg_signal);
            signal_name_to_aggregate_index.insert(key.clone(), index as u16);
        }

        GraphAggregateData {
            aggregate_signals,
            signal_name_to_aggregate_index,
        }
    }
}

impl GraphObjectStore {
    pub fn new(graph: Rc<GraphInnerMem>) -> Result<GraphObjectStore, GraphError> {
        let mut ordered_signals: Vec<_> = graph.signal_name_to_index.iter().collect();

        ordered_signals.sort_by(|(_, ind), (_, ind2)| ind.cmp(ind2));

        let mut objects = DynStack::new();

        for (signal_name, signal_index) in ordered_signals {
            let signal_inst = graph
                .signal_name_to_instance
                .get(signal_name)
                // This isn't a returned error since it implies a core inconsistency in the
                // graph emmory object
                .expect("Must find signal name deep in building");
            let mut consumer_hooks = HashMap::new();
            let mut aggregate_hooks = HashMap::new();

            for name in signal_inst.definition.inputs.keys() {
                let parents_of_inst = if let Some(parents) = signal_inst.inputs.get(*name) {
                    parents
                } else {
                    return Err(GraphError::InputNotFound {
                        input: *name,
                        signal: signal_name.clone(),
                    });
                };
                let key = (name.to_string(), signal_name.clone());
                match parents_of_inst {
                    NamedSignalType::Book(_) => (),
                    NamedSignalType::Inner(false, _) => {
                        // TODO can this happen while passing the previous checks?
                        // I think so but am not sure
                        let consumer = *graph
                            .signal_name_to_consumer
                            .get(&key)
                            .expect("name-key pair missing late");
                        let consumer_signal = ConsumerSignal {
                            graph: graph.clone(),
                            which: consumer,
                        };
                        consumer_hooks.insert(*name, consumer_signal);
                    }
                    NamedSignalType::Inner(true, _) => {
                        let aggregate = graph
                            .signal_name_to_aggregate
                            .get(&key)
                            .expect("name-key pair missing late")
                            .clone();
                        let aggregate_signal = AggregateSignal {
                            graph: graph.clone(),
                            offsets: aggregate,
                        };
                        aggregate_hooks
                            .entry(*name)
                            .or_insert(vec![])
                            .push(aggregate_signal);
                    }
                }
            }

            // TODO verify that every expected input name has a generated hook

            let index =
                (signal_inst.definition.creator)(consumer_hooks, aggregate_hooks, &mut objects);
            assert!(index == *signal_index);
        }
        let mut raw_pointers = Vec::new();
        for object in objects.iter() {
            raw_pointers.push(object.get_addr());
        }
        assert!(objects.len() == raw_pointers.len());
        for (obj, ptr) in objects.iter().zip(raw_pointers.iter()) {
            obj.check_addr(*ptr)
        }
        Ok(GraphObjectStore {
            objects,
            raw_pointers,
        })
    }
}

impl GraphCallList {
    // TODO check types
    fn trigger(&self, update: BookUpdate, aggregate: &GraphAggregateData, time: u64) {
        // It turns out, holding indices/keeping operations inside this function
        // drastically increases register pressure and stack usage, potentially to the point
        // of becoming comparably expensive to simple operators
        let book_param = (update.book as *const _, update.updated_mask);
        for (call, ptr, value) in self.book_calls.iter() {
            call(*ptr, book_param, time, *value);
        }

        for (call, cons_agg, ptr, value) in self.inner_calls.iter() {
            call(*ptr, *cons_agg, time, *value, aggregate);
        }

        let mark_slice = &self.mem.mark_as_written[..];
        for to_mark in &self.mark_as_clean {
            let to_mark = *to_mark as usize;
            debug_assert!(mark_slice.len() > to_mark);
            unsafe {
                mark_slice.get_unchecked(to_mark)
            }.set(0);
        }
    }
}

impl Graph {
    pub fn trigger_book(&mut self, update: BookUpdate, security: SecurityIndex, time: u64) {
        if let Some(calls) = self.book_updates.get(security) {
            calls.trigger(update, &self.agg_data, time);
        }
    }

    pub fn signal_listener(&self, signal: &str) -> Option<ConsumerSignal> {
        self.mem
            .signal_name_to_index
            .get(signal)
            .map(|ind| ConsumerSignal {
                graph: self.mem.clone(),
                which: *ind,
            })
    }
}

#[no_mangle]
pub fn check_graph_code(graph: &mut Graph, update: BookUpdate, security: SecurityIndex, time: u64) {
    graph.trigger_book(update, security, time);
}
