use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::rc::Rc;

use crate::exchange::normalized::{MarketEvent, SmallVec};

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
    // This could be compressed even more into a real bitmask,
    // BUT this has significant code size and performance costs given how much it's called
    pub(crate) mark_bitmask: Vec<Cell<u8>>,
    pub(crate) books: SecurityVector<Rc<RefCell<OrderBook>>>,
    pub(crate) signal_output_to_index: HashMap<(String, String), u16>,
    pub(crate) signal_name_to_index: HashMap<String, u16>,
    pub(crate) signal_name_to_instance: HashMap<String, SignalInstantiation>,
    pub(crate) aggregate_mapping_array: Vec<u16>,
    pub(crate) objects: DynStack<dyn CallSignal>,
}

pub(crate) struct GraphCallList {
    // TODO pull out specific functions from vtable
    // not high importance, only worth doing after proper testing is in place
    pub(crate) calls: Vec<(fn(*mut u8, u128, &GraphInnerMem), *mut u8)>,
    pub(crate) cleanup: Vec<(fn(*mut u8, u128, &GraphInnerMem), *mut u8)>,
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
        params: &HashMap<String, String>,
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

        let mut mark_bitmask: Vec<_> = signal_output_to_index
            .iter()
            .map(|_| Cell::new(0))
            .collect();

        // REQUIRED! extra padding for bulk de-mask operations
        for _ in 0..32 {
            mark_bitmask.push(Cell::new(0));
        }

        let books =
            SecurityVector::new_with(security_map, |_, _| Rc::new(RefCell::new(OrderBook::new())));

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
            let mut hooks = HashMap::<_, Box<dyn Any>>::new();

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
                            hooks.insert(
                                *name,
                                Box::new(BookViewer {
                                    book: books.get(index).clone(),
                                }),
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
                        hooks.insert(*name, Box::new(consumer_signal));
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
                        let aggregate_signal = AggregateInputGenerator {
                            offsets: (range_start as u16)..(range_end as u16),
                            mapping: aggregate_offsets.clone(),
                        };
                        hooks.insert(*name, Box::new(aggregate_signal));
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

            let input: Option<&str> = params.get(signal_name).map(|s| s.as_str());

            let index = (signal_inst.definition.creator)(
                output_hooks,
                InputLoader { all_inputs: hooks },
                input,
                signal_name,
                &mut objects,
            )?;
            assert_eq!(
                signal_name_to_index.insert(signal_name.clone(), index),
                None
            );
        }

        let mut rval = Rc::new(GraphInnerMem {
            output_values,
            books,
            mark_bitmask,
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
        for (call, ptr) in self.calls.iter() {
            call(*ptr, time, graph);
        }
    }

    fn cleanup(&mut self, time: u128, graph: &GraphInnerMem) {

        for (call, ptr) in self.cleanup.iter() {
            call(*ptr, time, graph)
        }

        let mark_slice = &self.mem.mark_bitmask[..];
        for to_mark in &self.mark_as_clean {
            let to_mark = *to_mark as usize;
            use std::arch::x86_64::*;
            // This depends on Cell's transparent representation, if it's right
            // this is compiled out, otherwise just compiles to a panic
            assert_eq!(std::mem::size_of::<Cell<u8>>(), std::mem::size_of::<u8>());
            debug_assert!(mark_slice.len() > (to_mark + 1) * 16);
            let addr = mark_slice.as_ptr() as *mut __m128i;
            unsafe {
                let real_addr = addr.add(to_mark);
                let block_mask = _mm_set1_epi8(!1);
                let loaded = _mm_loadu_si128(real_addr);
                let loaded = _mm_and_si128(block_mask, loaded);
                _mm_store_si128(real_addr, loaded);
            }
        }

    }
}

impl Graph {
    pub fn trigger_book<F: Fn(u128, &GraphInnerMem)>(
        &mut self,
        security: SecurityIndex,
        events: &SmallVec<MarketEvent>,
        time: u128,
        fnc: F,
    ) {
        {
            let mut book = self.mem.books.get(security).borrow_mut();
            for event in events {
                book.handle_book_event(event);
            }
        }

        // TODO benchmark the performance issues here - 
        // Is it just since my laptop has no cores/weird scheduling?
        // results are wayyyyy too expensive by any interpretation?
        // even for empty calls takes ~1-2k cycles with high variance
        if let Some(calls) = self.book_updates.get_mut(security) {
            calls.trigger(time, &self.mem);
            fnc(time, &self.mem);
            calls.cleanup(time, &self.mem);
        }
    }

    pub fn signal_listener(&self, signal: &str, output: &str) -> Option<ConsumerWatcher> {
        self.mem
            .signal_output_to_index
            .get(&(signal.to_string(), output.to_string()))
            .map(|ind| ConsumerWatcher {
                inner: ConsumerInput { which: *ind },
                graph: self.mem.clone(),
            })
    }

    pub fn load_outputs(&self) -> Vec<((String, String), Option<f64>)> {
        self.mem
            .signal_output_to_index
            .iter()
            .map(|((name, output), index)| {
                (
                    (name.clone(), output.clone()),
                    ConsumerInput { which: *index }.get(&self.mem),
                )
            })
            .collect()
    }
}
