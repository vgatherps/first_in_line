use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::ops::Range;
use std::rc::Rc;

use super::graph_error::GraphError;
use super::graph_registrar::*;
use super::interface_types::*;
use super::security_data::SecurityVector;
use super::security_index::SecurityIndex;

use dynstack::DynStack;

pub struct GraphInnerMem {
    pub output_values: Vec<Cell<f64>>,
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
    pub(crate) objects: DynStack<RawObj>,
    pub(crate) raw_pointers: Vec<*mut u8>,
}

#[derive(Copy, Clone)]
pub enum ConsumerOrAggregate {
    Aggregate(u32, u16),
    Consumer(i32),
}

// TODO allocate objects in order of calls, not in order of objects passed in
pub(crate) struct GraphCallList {
    pub(crate) book_calls: Vec<(BookCallback, *mut u8, *const Cell<f64>)>,
    pub(crate) inner_calls: Vec<(
        InnerCallback,
        ConsumerOrAggregate,
        *mut u8,
        *const Cell<f64>,
    )>,
    pub(crate) hold_objects: Rc<GraphObjectStore>,
    pub(crate) hold_mem: Rc<GraphInnerMem>,
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
    ) -> Result<Rc<GraphInnerMem>, GraphError> {
        assert!(signal_name_to_instance.len() < std::u16::MAX as usize);
        let signal_name_to_index: HashMap<_, _> = signal_name_to_instance
            .iter()
            .enumerate()
            .map(|(ind, (k, _))| (k.clone(), ind as u16))
            .collect();

        let output_values: Vec<_> = signal_name_to_index
            .iter()
            .map(|_| Cell::new(0.0))
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

            // OBVIOUSLY! The bug happens right here, I get the address while the vector is still
            // getting constructed, so of course it gets corrput later on

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
        //
        // Keeping parameters directly in the iteration lists increases memory usage, BUT
        // is very prefetcher friendly and isn't too much compared to books. On the other hand,
        // the stack manipulation would have created a lot of dependency chains and
        // read-after-write conflicts
        let book_param = (update.book as *const _, update.updated_mask);
        for (call, ptr, value) in self.book_calls.iter() {
            // This is already a safety/correctness requirement, so I debug_check and assert
            // do it unchecked
            call(*ptr, book_param, time, *value);
        }

        for (call, cons_agg, ptr, value) in self.inner_calls.iter() {
            call(*ptr, *cons_agg, time, *value, aggregate);
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
