#![allow(warnings)]
use exchange::ftx_connection;
use std::collections::hash_map::DefaultHasher;
use std::collections::VecDeque;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::BufWriter;
use std::io::Write;
use std::sync::Arc;

use crossbeam::queue::ArrayQueue;

mod exchange;

#[derive(Copy, Clone, Debug)]
struct SequenceVal {
    hash: u64,
    timestamp: u64,
    sequence_id: usize,
}

// Checks for an alignment of the two sequences and returns that there's a match
fn align_sequence(master: &[SequenceVal], new: &[SequenceVal]) -> Option<(usize, usize)> {
    let mut master_base = 0;
    while master_base + SEQUENCE_MESSAGES_MATCH <= master.len() {
        let mut new_base = 0;
        while new_base + SEQUENCE_MESSAGES_MATCH <= new.len() {
            let new_slice = &new[new_base..new_base + SEQUENCE_MESSAGES_MATCH];
            let master_slice = &master[master_base..master_base + SEQUENCE_MESSAGES_MATCH];
            if new_slice
                .iter()
                .zip(master_slice.iter())
                .all(|(a, b)| a.hash == b.hash)
            {
                return Some((master_base, new_base));
            }
            new_base += 1;
        }
        master_base += 1;
    }
    None
}

const SEQUENCE_MESSAGES_MATCH: usize = 10;

async fn read_one_stream(sequence_id: usize, queue: Arc<ArrayQueue<SequenceVal>>) {
    let mut stream = ftx_connection().await;

    loop {
        let msg = stream.next().await;
        let mut hasher = DefaultHasher::new();
        msg.events.hash(&mut hasher);
        let sequence = SequenceVal {
            hash: hasher.finish(),
            timestamp: msg.received_time,
            sequence_id,
        };
        queue
            .push(sequence)
            .expect("logging thread very backlogged");
    }
}

fn run_a_stream(sequence_id: usize, queue: Arc<ArrayQueue<SequenceVal>>) {
    let mut rt = tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .expect("Couldn't build executor");
    rt.block_on(read_one_stream(sequence_id, queue));
}

const MASTER_INDEX: usize = 0;
const CHILD_INDEX: usize = 1;

fn main() {
    let message_queue = Arc::new(ArrayQueue::new(10000));

    // first, spawn the master queue with index 0
    let master_queue = message_queue.clone();
    let child_queue = message_queue.clone();

    let mut master_aligner = Vec::new();
    let mut child_aligner = Vec::new();

    let master_thread = std::thread::spawn(move || run_a_stream(MASTER_INDEX, master_queue));
    let child_thread = std::thread::spawn(move || run_a_stream(CHILD_INDEX, child_queue));

    let mut master_record = VecDeque::new();
    let mut child_record = VecDeque::new();

    // now we try and find an alignment
    loop {
        match message_queue.pop() {
            Ok(seq) => {
                match seq.sequence_id {
                    MASTER_INDEX => master_aligner.push(seq),
                    CHILD_INDEX => child_aligner.push(seq),
                    _ => panic!("Got bogus sequence id"),
                }
                if seq.sequence_id != MASTER_INDEX {
                    println!("Got message {:?}", seq);
                }
                if master_aligner.len() > 10000 {
                    println!(
                        "got 10000 messages and no alignment, lengths: {}, {}",
                        master_aligner.len(),
                        child_aligner.len()
                    );
                }
                if let Some((master_offset, child_offset)) =
                    align_sequence(&master_aligner, &child_aligner)
                {
                    for seq in master_aligner[master_offset..].into_iter() {
                        master_record.push_back(*seq);
                    }
                    for seq in child_aligner[child_offset..].into_iter() {
                        child_record.push_back(*seq);
                    }
                    break;
                }
            }
            Err(_) => (),
        }
    }

    loop {
        while master_record.len() > 0 && child_record.len() > 0 {
            match (master_record.pop_front(), child_record.pop_front()) {
                (Some(master), Some(child)) => {
                    assert_eq!(master.hash, child.hash);
                    let diff = if master.timestamp > child.timestamp {
                        (master.timestamp - child.timestamp) as i64
                    } else {
                        (child.timestamp - master.timestamp) as i64 * -1
                    };
                    println!("Difference is {} us", diff);
                }
                _ => panic!("Got missing data even with lengths"),
            }
        }
        match message_queue.pop() {
            Ok(seq) => match seq.sequence_id {
                MASTER_INDEX => master_record.push_back(seq),
                CHILD_INDEX => child_record.push_back(seq),
                _ => panic!("Got bogus sequence id"),
            },
            Err(_) => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_sequencer() {
        let master_sequence: Vec<_> = (0..15)
            .map(|ind| SequenceVal {
                hash: ind,
                timestamp: 0,
                sequence_id: 0,
            })
            .collect();

        assert_eq!(
            align_sequence(&master_sequence, &master_sequence[2..]),
            Some((2, 0))
        );
        assert_eq!(
            align_sequence(&master_sequence[1..], &master_sequence[2..]),
            Some((1, 0))
        );
        assert_eq!(
            align_sequence(&master_sequence[3..], &master_sequence[2..]),
            Some((0, 1))
        );
        assert_eq!(
            align_sequence(&master_sequence[5..], &master_sequence[2..]),
            Some((0, 3))
        );
        assert_eq!(
            align_sequence(&master_sequence[5..], &master_sequence[..14]),
            None
        );
    }
}
