use super::interface_types::*;
use std::any::Any;
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use super::graph::{
    ConsumerOrAggregate, Graph, GraphAggregateData, GraphCallList, GraphInnerMem, GraphObjectStore,
};
use super::graph_error::GraphError;
use super::graph_registrar::*;
use super::security_data::SecurityVector;
use super::security_index::{Security, SecurityMap};

fn find_seen_signals(
    security: &Security,
    mem: Rc<GraphInnerMem>,
) -> Result<(HashMap<String, String>, HashMap<String, String>), GraphError> {
    let mut seen_signals: HashMap<String, String> = HashMap::new();
    // First, find the direct dependencies of the security
    for (signal_name, instance) in &mem.signal_name_to_instance {
        let signal_name = signal_name;
        for (input_name, signal_type) in &instance.inputs {
            match signal_type {
                NamedSignalType::Book(sec) if sec == security => {
                    if let Some(old_name) = seen_signals.get(signal_name) {
                        return Err(GraphError::MultipleSubscription {
                            signal: signal_name.to_string(),
                            name1: old_name.to_string(),
                            name2: input_name.clone(),
                            security: security.clone(),
                        });
                    } else {
                        seen_signals.insert(signal_name.clone(), input_name.clone());
                    }
                }
                _ => (),
            }
        }
    }

    let book_signals = seen_signals.clone();

    // now, keep adding new signals to the map until we find nothing new
    // This is not an efficient algorithm, but it's simple
    loop {
        let starting_size = seen_signals.len();

        for (signal_name, instance) in &mem.signal_name_to_instance {
            let signal_name: &str = signal_name;
            for (input_name, signal_type) in &instance.inputs {
                let parents = match signal_type {
                    NamedSignalType::Inner(_, parents) => parents.clone(),
                    _ => vec![],
                };

                // If there are any parents in the set of seen signals
                // we first check if the signal has already been inserted into the seen set
                // If it has, and it's with the same trigger name, then we ignore it.
                // If it has, and this is a new trigger name, then we return an error
                // If it hasn't, we insert it into the seen set
                if parents
                    .into_iter()
                    .filter(|p| seen_signals.contains_key(p))
                    .next()
                    .is_some()
                {
                    match seen_signals.get(signal_name) {
                        Some(old_name) if old_name != input_name => {
                            return Err(GraphError::MultipleSubscription {
                                signal: signal_name.to_string(),
                                name1: old_name.to_string(),
                                name2: input_name.clone(),
                                security: security.clone(),
                            });
                        }
                        None => seen_signals.insert(signal_name.to_string(), input_name.clone()),
                        _ => None,
                    };
                }
            }
        }

        if starting_size == seen_signals.len() {
            return Ok((seen_signals, book_signals));
        }
    }
}

fn topological_sort(
    seen_signals: &HashMap<String, String>,
    starting_signals: Vec<(String, String)>,
    mem: Rc<GraphInnerMem>,
) -> Vec<(String, String)> {
    let mut ordered_signals = starting_signals;

    let mut already_in_order: HashSet<_> =
        ordered_signals.iter().map(|(sig, _)| sig.clone()).collect();

    let mut dependencies = HashMap::new();
    // gather the dependencies for every signal in the seen_signals set
    for (signal, name) in seen_signals {
        let parents = mem
            .signal_name_to_instance
            .get(signal)
            .expect("Missing signal late in process")
            .inputs
            .get(name)
            .expect("Missing name late in process");
        let parents = match parents {
            NamedSignalType::Inner(_, parents) => {
                let parents: Vec<_> = parents
                    .iter()
                    .filter(|p| {
                        let p: &str = p;
                        seen_signals.contains_key(p)
                    })
                    .collect();
                assert!(parents.len() > 0);
                parents
            }
            NamedSignalType::Book(_) => vec![],
        };
        if parents.len() > 0 {
            let parents: HashSet<_> = parents.into_iter().collect();
            dependencies.insert(signal, parents);
        }
    }

    // sort the signals, starting by clearing out the book signals
    let mut signals_to_clear: Vec<_> = ordered_signals.iter().map(|(a, _)| a.clone()).collect();
    loop {
        let mut new_empty_signals = Vec::new();
        let starting_size = ordered_signals.len();

        for signal in signals_to_clear.iter() {
            for (child, parents) in dependencies.iter_mut() {
                parents.remove(signal);
                if parents.len() == 0 {
                    new_empty_signals.push((*child).clone());
                }
            }
        }

        for signal in &new_empty_signals {
            dependencies.remove(signal);
            let signal: &str = signal;
            ordered_signals.push((
                signal.to_string(),
                seen_signals.get(signal).unwrap().clone(),
            ));
        }

        signals_to_clear = new_empty_signals;

        if ordered_signals.len() == starting_size {
            if dependencies.len() != 0 {
                panic!("had a cycle with signals {:?}", dependencies.keys());
            }
            return ordered_signals;
        }
    }
}

fn generate_mask_for_call(
    mem: &GraphInnerMem,
    signal: &str,
    input: &str,
    seen_signals: &HashMap<String, String>,
) -> u32 {
    let instance = mem
        .signal_name_to_instance
        .get(signal)
        .expect("Missing instance late");
    let inputs = instance
        .inputs
        .get(input)
        .expect("Missing input name late in process");
    assert_eq!(input, seen_signals.get(signal).expect("Missing seen late"));
    let val = match inputs {
        NamedSignalType::Inner(true, parents) => {
            assert!(parents.len() <= 32);
            let mut val = 0u32;
            for (index, parent) in parents.iter().enumerate() {
                if seen_signals.contains_key(parent) {
                    val |= 1 << index;
                }
            }
            val
        }
        _ => panic!("non-aggregate signal asked for mask"),
    };
    val
}

pub(crate) fn generate_calls_for(
    security: &Security,
    mem: Rc<GraphInnerMem>,
    agg: &GraphAggregateData,
    objects: Rc<GraphObjectStore>,
) -> Result<GraphCallList, GraphError> {
    let (seen_signals, book_signals) = find_seen_signals(security, mem.clone())?;
    let sorted = topological_sort(
        &seen_signals,
        book_signals.into_iter().collect(),
        mem.clone(),
    );

    // now generate list of actual calls

    let mut inner_call_list = Vec::new();
    let mut book_call_list = Vec::new();

    for (signal, call_name) in sorted {
        let call_name: &str = &call_name;
        let instance = mem
            .signal_name_to_instance
            .get(&signal)
            .expect("Missing call late");
        let call = instance
            .definition
            .inputs
            .get(call_name)
            .expect("Missing actual call late");
        let signal_offset = *mem
            .signal_name_to_index
            .get(&signal)
            .expect("MIssing index late");
        match call.sig_type {
            InputSignalType::Book(callback) => {
                book_call_list.push((
                    callback,
                    objects.raw_pointers[signal_offset as usize],
                    &mem.output_values[signal_offset as usize] as *const _,
                ));
            }
            InputSignalType::Inner(true, callback) => {
                let mask = generate_mask_for_call(&*mem, &signal, call_name, &seen_signals);
                let aggregate_offset = *agg
                    .signal_name_to_aggregate_index
                    .get(&(call_name.to_string(), signal.to_string()))
                    .expect("Missing aggregate offset late");
                let agg = ConsumerOrAggregate::Aggregate(mask, aggregate_offset);
                inner_call_list.push((
                    callback,
                    agg,
                    objects.raw_pointers[signal_offset as usize],
                    &mem.output_values[signal_offset as usize] as *const _,
                ));
            }
            InputSignalType::Inner(false, callback) => {
                let parent_index = *mem
                    .signal_name_to_consumer
                    .get(&(call_name.to_string(), signal.to_string()))
                    .expect("Missing consumer offset late");
                assert!(parent_index != signal_offset);
                let actual_offset = if parent_index < signal_offset {
                    (signal_offset - parent_index) as i32 * -1
                } else {
                    (parent_index - signal_offset) as i32
                };
                inner_call_list.push((
                    callback,
                    ConsumerOrAggregate::Consumer(actual_offset),
                    objects.raw_pointers[signal_offset as usize],
                    &mem.output_values[signal_offset as usize] as *const _,
                ));
            }
        }
    }

    Ok(GraphCallList {
        book_calls: book_call_list,
        inner_calls: inner_call_list,
        hold_objects: objects,
        hold_mem: mem,
    })
}
