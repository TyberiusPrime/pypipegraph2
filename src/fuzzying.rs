/*
* This doesn't fuzz.
* It creates *all* possible graphs up to 6 nodes,
* with all meaningfull edge variations
* (ie. if you ordered the nodes left to right,
* all edges point right. Since we permutate all nodes,
* that's all the DAGs, or at least their iso-equivalences)
*
*  At 6 that's already a lot of nodes,
*  but it does finish in under 10 minutes in release.
*  Running at a cool 40k iterations on my desktop.
*
*  5 is       248.832 variations
   6 is    23.887.872
   7 is 4.586.471.424
*/
use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

use pypipegraph2::{JobKind, PPGEvaluator, StrategyForTesting, TestGraphRunner};

#[derive(Clone)]
struct AllNodes {
    state: Vec<u8>,
}

impl AllNodes {
    fn new(node_count: usize) -> AllNodes {
        AllNodes {
            state: vec![0; node_count as usize],
        }
    }
    fn apply(&self, g: &mut PPGEvaluator<StrategyForTesting>) {
        for (node_id, node_state) in self.state.iter().enumerate() {
            let kind = match node_state {
                0 => JobKind::Always,
                1 => JobKind::Ephemeral,
                2 => JobKind::Output,
                _ => panic!(),
            };
            g.add_node(&format!("N{}", node_id), kind);
        }
    }

    fn advance(&mut self) -> bool {
        let mut pos = self.state.len() - 1;
        self.state[pos] += 1;
        while self.state[pos] == 3 {
            self.state[pos] = 0;
            if pos == 0 {
                break;
            }
            pos = pos - 1;
            self.state[pos] += 1;
        }
        if self.state.iter().all(|x| *x == 0) {
            return false;
        } else {
            return true;
        }
    }

    fn len(&self) -> usize {
        return 3usize.pow(self.state.len() as u32);
        /*
        let mut m = AllNodes::new(self.state.len());
        let mut counter = 1;
        while (m.advance()) {
            counter += 1;
        }
        return counter;
        */
    }
}

#[derive(Clone)]
struct AllEdges {
    state: Vec<u8>,
    node_count: usize,
}

impl AllEdges {
    fn new(node_count: usize) -> AllEdges {
        let edge_count = (1..node_count).sum();
        AllEdges {
            state: vec![0; edge_count],
            node_count,
        }
    }
    fn apply(&self, g: &mut PPGEvaluator<StrategyForTesting>) {
        let mut edge_pos = 0;
        for n in 0..self.node_count {
            for m in (n + 1)..self.node_count {
                if self.state[edge_pos] == 1 {
                    g.depends_on(&format!("N{m}"), &format!("N{n}"));
                }
                edge_pos += 1;
            }
        }
    }
    fn advance(&mut self) -> bool {
        let mut pos = self.state.len() - 1;
        self.state[pos] += 1;
        while self.state[pos] == 2 {
            self.state[pos] = 0;
            if pos == 0 {
                break;
            }
            pos = pos - 1;
            self.state[pos] += 1;
        }
        if self.state.iter().all(|x| *x == 0) {
            return false;
        } else {
            return true;
        }
    }

    fn len(&self) -> usize {
        2usize.pow(self.state.len() as u32)
    }
}

fn write_error_to_file(count: usize, message: &str) {
    std::fs::write(format!("fuzzying_error_{}.txt", count), message).ok();
}

fn main() {
    println!("running fuzz test.");
    //read first command line argument into integer
    let args: Vec<String> = std::env::args().collect();
    let start = args
        .get(1)
        .unwrap_or(&"0".to_string())
        .parse::<usize>()
        .unwrap_or(0);

    let node_count = 6;

    //

    let mut n = AllNodes::new(node_count);
    let total = {
        let m = AllEdges::new(node_count);
        let nv = n.len();
        let ev = m.len();

        println!("node variations: {}", nv);
        println!("edge variations: {}", ev);
        let total = ev * nv;
        println!("Together: {}", total);
        total
    };
    let start_time = std::time::Instant::now();
    let mut count = 0;
    let error_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let mut threads = std::collections::VecDeque::new();
    let max_thread_count = 100;

    loop {
        let mut m = AllEdges::new(node_count);
        loop {
            {
                if threads.len() > max_thread_count {
                    let t: JoinHandle<_> = threads.pop_front().unwrap();
                    match t.join() {
                        Ok(_) => {}
                        Err(e) => println!("A thread failed. {:?}", e),
                    }
                }
                if count >= start {
                    if (count % 1000) == 0 {
                        let rate = count as f32 / start_time.elapsed().as_secs_f32();
                        let remaining_seconds = (total - count - start) as f32 / rate;
                        println!(
                            "{} tested. {:.2}%. Rate: {:.2}/s. Errors: {}, Remaining: {:.2}s",
                            count,
                            count as f32 / total as f32 * 100.,
                            rate,
                            *error_count.lock().unwrap(),
                            remaining_seconds
                        );
                    }
                    let n2 = n.clone();
                    let m2 = m.clone();
                    //spawn in seperate thread
                    let inner_count = count;
                    let error_count = error_count.clone();
                    threads.push_back(std::thread::spawn(move || {
                        let res = std::panic::catch_unwind(|| {
                            let mut t = TestGraphRunner::new(Box::new(move |g| {
                                n2.apply(g);
                                m2.apply(g);
                            }));
                            let g = t.run(&[]);
                            match g {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("error at count {}: {}", e.1, inner_count);
                                    let d = e.0.debug_();
                                    println!("{}", d);
                                    write_error_to_file(inner_count, &d[..]);
                                    *error_count.lock().unwrap() += 1;
                                }
                            }
                            match t.run(&[]) {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("error at count {} 2nd stage: {}", e.1, inner_count);
                                    let d = e.0.debug_();
                                    println!("{}", d);
                                    write_error_to_file(inner_count, &d[..]);
                                    *error_count.lock().unwrap() += 1;
                                }
                            }
                        });
                        match res {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Caught panic! at count {}: {:?}", inner_count, e);
                            }
                                    *error_count.lock().unwrap() += 1;
                        }
                    }));
                }
                count += 1;
            }
            if !m.advance() {
                break;
            }
        }

        let b = n.advance();
        if !b {
            break;
        }
    }
    let stop_time = std::time::Instant::now();
    if *error_count.lock().unwrap() > 0 {
        panic!("{} errors occured", error_count.lock().unwrap());
    }
    let duration = stop_time.duration_since(start_time);
    println!(
        "tested {} variations in {}s. Rate: {}",
        count,
        duration.as_secs(),
        count / (duration.as_secs() as usize)
    );
    println!("done running fuzz {count}");
}
