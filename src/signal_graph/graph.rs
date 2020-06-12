use std::any::Any;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::rc::Rc;

use crate::order_book::OrderBook;

use super::graph_error::GraphError;
use super::graph_registrar::*;
use super::graph_sort::{find_seen_signals, topological_sort};
use super::interface_types::*;
use super::security_data::SecurityVector;
use super::security_index::{SecurityIndex, SecurityMap};

use dynstack::DynStack;

// TODO reduce god-object ness of this here
// Make InnerMem only refer to memory used by the signals,
// and not by arbitrary metadata
pub struct GraphInnerMem {
    pub(crate) output_values: Vec<Cell<f64>>,
    pub(crate) mark_as_written: Vec<Cell<u64>>,
    pub(crate) mark_as_valid: Vec<Cell<u64>>,
    pub(crate) books: SecurityVector<Rc<OrderBook>>,
    pub(crate) signal_output_to_index: HashMap<(String, String), u16>,
    pub(crate) signal_name_to_index: HashMap<String, u16>,
    pub(crate) signal_name_to_instance: HashMap<String, SignalInstantiation>,
    pub(crate) aggregate_mapping_array: Vec<u16>,
    pub(crate) objects: DynStack<dyn CallSignal>,
}

// returns bitmap_offset
#[inline(always)]
pub(crate) fn index_to_bitmap(index: u16) -> (u16, u16) {
    let offset = index / 64;
    let bit = index % 64;
    (offset, bit)
}

pub(crate) struct GraphCallList {
    pub(crate) calls: Vec<*mut CallSignal>,
    pub(crate) mem: Rc<GraphInnerMem>,
    pub(crate) mark_as_clean: Vec<u16>,
}

pub struct Graph {
    pub(crate) book_updates: SecurityVector<Option<GraphCallList>>,
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
                let seen_signals = find_seen_signals(sec, &signal_name_to_instance)?;
                let sorted_order = topological_sort(&seen_signals, &signal_name_to_instance);
                Ok(Some(sorted_order))
            } else {
                Ok(None)
            }
        })?;

        let mut signal_output_to_index = HashMap::new();
        let mut index_so_far: u16 = 0;

        security_call_list_justnames.for_each(|sigs| {
            if let Some(sigs) = sigs {
                for sig in sigs {
                    let instance = signal_name_to_instance
                        .get(sig)
                        .expect("Missing signal instance");
                    for output in &instance.definition.outputs {
                        let key = (sig.clone(), output.to_string());
                        if !signal_output_to_index.contains_key(&key) {
                            signal_output_to_index.insert(key, index_so_far);
                            index_so_far += 1;
                        }
                    }
                }
            }
        });

        for (signal, instance) in &signal_name_to_instance {
            for output in &instance.definition.outputs {
                let key = (signal.clone(), output.to_string());
                if !signal_output_to_index.contains_key(&key) {
                    signal_output_to_index.insert(key, index_so_far);
                    index_so_far += 1;
                }
            }
        }

        let output_values: Vec<_> = signal_output_to_index
            .iter()
            .map(|_| Cell::new(0.0))
            .collect();

        let bitmap_estimate = 1 + output_values.len() / 64;

        let mark_as_written: Vec<_> = output_values
            .iter()
            .map(|_| Cell::new(0))
            .take(bitmap_estimate)
            .collect();

        let books = SecurityVector::new_with(security_map, |_, _| Rc::new(OrderBook::new()));

        let mut ordered_signals: Vec<_> = signal_output_to_index.iter().collect();

        ordered_signals.sort_by(|(_, ind), (_, ind2)| ind.cmp(ind2));
        let mut aggregate_offsets = Vec::new();

        let mut built_signals = HashSet::new();

        let mut objects = DynStack::new();
        let mut signal_name_to_index = HashMap::new();

        for ((signal_name, _), _) in ordered_signals {
            if built_signals.contains(signal_name) {
                continue;
            }
            built_signals.insert(signal_name);
            let signal_inst = &signal_name_to_instance
                .get(signal_name)
                // This isn't a returned error since it implies a core inconsistency in the
                // graph memory object
                .expect("Must find signal name deep in building");
            let mut book_hooks = HashMap::new();
            let mut consumer_hooks = HashMap::new();
            let mut aggregate_hooks = HashMap::new();

            for (name, item) in signal_inst.inputs.iter() {
                if let Some(def) = signal_inst.definition.inputs.get(name.as_str()) {
                    match (item, def) {
                        (NamedSignalType::Book(_), SignalType::Book)
                        | (NamedSignalType::Consumer(_), SignalType::Consumer)
                        | (NamedSignalType::Aggregate(_), SignalType::Aggregate) => (),
                        (named, sig_type) => {
                            return Err(GraphError::InputWrongType {
                                input: name.clone(),
                                signal: signal_name.clone(),
                                given: named.clone(),
                                wants: sig_type.clone(),
                            });
                        }
                    }
                } else {
                    return Err(GraphError::InputNotExist {
                        input: name.clone(),
                        signal: signal_name.clone(),
                    });
                }
            }

            for name in signal_inst.definition.inputs.keys() {
                let parents_of_inst = if let Some(parents) = signal_inst.inputs.get(*name) {
                    parents
                } else {
                    return Err(GraphError::InputNotGiven {
                        input: *name,
                        signal: signal_name.clone(),
                    });
                };
                match parents_of_inst {
                    NamedSignalType::Book(sec) => {
                        if let Some(index) = security_map.to_index(sec) {
                            book_hooks.insert(
                                *name,
                                BookViewer {
                                    book: books.get(index).clone(),
                                },
                            );
                        } else {
                            return Err(GraphError::BookNotFound {
                                security: sec.clone(),
                            });
                        }
                    }
                    NamedSignalType::Consumer((parent_signal, parent_output)) => {
                        let consumer = get_index_for(
                            &signal_output_to_index,
                            &parent_signal,
                            &parent_output,
                            signal_name,
                            name,
                        )?;
                        let consumer_signal = ConsumerInput { which: consumer };
                        consumer_hooks.insert(*name, consumer_signal);
                    }
                    NamedSignalType::Aggregate(parents) => {
                        if parents.len() == 0 {
                            return Err(GraphError::AggregateNoInputs {
                                signal: signal_name.clone(),
                                name: name,
                            });
                        }
                        if parents.len() > MAX_SIGNALS_PER_AGGREGATE {
                            return Err(GraphError::AggregateTooLarge {
                                instance: signal_name.clone(),
                                input: name,
                                entries: parents.len(),
                            });
                        }
                        let range_start = aggregate_offsets.len();
                        for (parent, output) in parents {
                            let consumer = get_index_for(
                                &signal_output_to_index,
                                &parent,
                                &output,
                                signal_name,
                                name,
                            )?;
                            aggregate_offsets.push(consumer);
                        }
                        let range_end = aggregate_offsets.len();
                        let aggregate_signal = AggregateSignal {
                            offsets: (range_start as u16)..(range_end as u16),
                        };
                        aggregate_hooks.insert(*name, aggregate_signal);
                    }
                }
            }

            // TODO verify that every expected input name has a generated hook

            let mut output_hooks = HashMap::new();
            for output in &signal_inst.definition.outputs {
                let key = (signal_name.clone(), output.to_string());
                let index = signal_output_to_index
                    .get(&key)
                    .expect("Missing generated output key");
                output_hooks.insert(
                    *output,
                    ConsumerOutput {
                        inner: ConsumerInput { which: *index },
                    },
                );
            }

            let index = (signal_inst.definition.creator)(
                output_hooks,
                book_hooks,
                consumer_hooks,
                aggregate_hooks,
                &mut objects,
            );
            assert_eq!(
                signal_name_to_index.insert(signal_name.clone(), index),
                None
            );
        }

        let mut rval = Rc::new(GraphInnerMem {
            output_values,
            books,
            mark_as_valid: mark_as_written.clone(),
            mark_as_written,
            signal_output_to_index,
            signal_name_to_index,
            signal_name_to_instance,
            aggregate_mapping_array: aggregate_offsets,
            objects,
        });

        Ok(rval)
    }
}

fn get_index_for(
    signal_output_to_index: &HashMap<(String, String), u16>,
    parent: &str,
    output: &str,
    signal: &str,
    name: &str,
) -> Result<u16, GraphError> {
    let key = (parent.to_string(), output.to_string());
    if let Some(parent) = signal_output_to_index.get(&key) {
        Ok(*parent)
    } else {
        Err(GraphError::ParentNotFound {
            parent: parent.to_string(),
            output: output.to_string(),
            input: name.to_string(),
            child: signal.to_string(),
        })
    }
}

impl GraphCallList {
    // TODO check types
    fn trigger(&mut self, time: u128, graph: &GraphInnerMem) {
        for call in self.calls.iter() {
            let call = unsafe { &mut **call };
            call.call_signal(time, graph);
        }

        let mark_slice = &self.mem.mark_as_written[..];
        println!("Marking {:?}", mark_slice);
        for to_mark in &self.mark_as_clean {
            let to_mark = *to_mark as usize;
            debug_assert!(mark_slice.len() > to_mark);
            unsafe { mark_slice.get_unchecked(to_mark) }.set(0);
        }
    }
}

impl Graph {
    pub fn handle_md_events(&mut self, events: &MarketEventBlock, time: u128) {}
    pub fn trigger_book(&mut self, security: SecurityIndex, time: u128) {
        println!("written_bits: {:?}", self.mem.mark_as_written);
        if let Some(calls) = self.book_updates.get_mut(security) {
            calls.trigger(time, &self.mem);
        }
    }

    // TODO make safe
    pub fn signal_listener(&self, signal: &str, output: &str) -> Option<ConsumerInput> {
        self.mem
            .signal_output_to_index
            .get(&(signal.to_string(), output.to_string()))
            .map(|ind| ConsumerInput { which: *ind })
    }
}
