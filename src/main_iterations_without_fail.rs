/*
* This doesn't fuzz.
* It creates *all* possible graphs up to 6 nodes,
* with all meaningfull edge variations
* (ie. if you ordered the nodes left to right,
* all edges point right. Since we permutate all nodes,
* that's all the DAGs, or at least their iso-equivalences)
*
*  At 6 that's already a lot of nodes,
*  but it does finish in under 3 minutes in release.
*  Running at a cool 400k iterations on my desktop.
*
*  5 is       248.832 variations
   6 is    23.887.872
   7 is 4.586.471.424
*/
use std::{
    sync::{Arc, Mutex},
    thread,
};

use pypipegraph2::{JobKind, PPGEvaluator, StrategyForTesting, TestGraphRunner};

use backtrace::Backtrace;
use std::cell::RefCell;

thread_local! {
    static BACKTRACE: RefCell<Option<Backtrace>> = RefCell::new(None);
}

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

#[derive(Clone, Debug)]
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

fn write_error_to_file(node_count: usize, edge_count: usize, message: &str) {
    std::fs::write(
        format!(
            "iteration_without_fail_error_{}_{}.txt",
            node_count, edge_count
        ),
        message,
    )
    .ok();
}

fn main() {
    std::panic::set_hook(Box::new(|_| {
        let trace = Backtrace::new();
        BACKTRACE.with(move |b| b.borrow_mut().replace(trace));
    }));

    println!("running fuzz test.");
    //read first command line argument into integer
    let args: Vec<String> = std::env::args().collect();
    let problem_size = args
        .get(1)
        .unwrap_or(&"5".to_string())
        .parse::<usize>()
        .unwrap_or(5);

    //let problem_size = 6;

    //

    let n = AllNodes::new(problem_size);
    let node_total = n.len();
    let edges_per_node_variation = {
        let m = AllEdges::new(problem_size);
        m.len()
    };
    let total = {
        println!("node variations: {}", node_total);
        println!("edge variations: {}", edges_per_node_variation);
        let total = edges_per_node_variation * node_total;
        println!("Together: {}", total);
        total
    };
    let start_time = std::time::Instant::now();
    let errors: Arc<Mutex<Vec<(usize, usize)>>> = Arc::new(Mutex::new(Vec::new()));

    let mut threads = Vec::new();
    let max_thread_count = num_cpus::get(); //system cpu count ;
    let done_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));

    let chunk_size = n.len() / max_thread_count;
    //now space from 0... n.len() into max_thread_count chunks
    let starts = (0..max_thread_count)
        .map(|i| i * chunk_size)
        .collect::<Vec<_>>();
    println!("{:?}, chunk_size {}", starts, chunk_size);

    for start in starts {
        let mut n = n.clone();
        let errors = errors.clone();
        let done_count = done_count.clone();
        threads.push(thread::spawn(move || {
            let mut node_count = 0;

            while node_count < start {
                n.advance();
                node_count += 1;
            }
            while node_count < start + chunk_size {
                *done_count.lock().unwrap() += 1;
                if start == 0 {
                    let done = *done_count.lock().unwrap();
                    let elapsed = start_time.elapsed().as_secs().max(1);
                    let rate = done as f64 / elapsed as f64;
                    let eta = (node_total - done) as f64 / rate;
                    println!(
                        "done: {} of {}, elapsed: {}s. Rate {:.2}, estimated remaining: {:.2} s",
                        done, node_total, elapsed, rate * edges_per_node_variation as f64, eta
                    );
                } else
                {
                }
                let mut m = AllEdges::new(problem_size);
                let mut edge_count = 0;
                loop {
                    let n2b = n.clone();
                    let m2b = m.clone();
                    let res = std::panic::catch_unwind(|| {
                        let mut t = TestGraphRunner::new(Box::new(move |g| {
                            n2b.apply(g);
                            m2b.apply(g);
                        }));
                        let g = t.run(&[]);
                        /*if (edge_count == 23) {
                            panic!("twentythree");
                        }
if (edge_count == 24) {
                            panic!("twentyfour");
                        }
                        */

                        match g {
                            Ok(_) => {}
                            Err(e) => {
                                println!(
                                    "error at node_count: {}, edge_count: {}",
                                    node_count, edge_count
                                );
                                let d = e.0.debug_();
                                println!("{}", d);
                                write_error_to_file(node_count, edge_count, &d[..]);
                                errors.lock().unwrap().push((node_count, edge_count));
                            }
                        }
                        match t.run(&[]) {
                            Ok(_) => {}
                            Err(e) => {
                                println!(
                                    "error at node_count: {}, edge_count: {}",
                                    node_count, edge_count
                                );
                                let d = e.0.debug_();
                                println!("{}", d);
                                write_error_to_file(node_count, edge_count, &d[..]);
                                errors.lock().unwrap().push((node_count, edge_count));
                            }
                        }
                    });
                    match res {
                        Ok(_) => {}
                        Err(info) => {
                            let msg = match info.downcast_ref::<&'static str>() {
                                Some(s) => *s,
                                None => match info.downcast_ref::<String>() {
                                    Some(s) => &s[..],
                                    None => "Box<dyn Any>",
                                },
                            };

                            println!("Caught panic! at node_count {}, edge_count: {}: error {:?}", node_count, edge_count, msg);
                            errors.lock().unwrap().push((node_count, edge_count));
                            let n2 = n.clone();
                            let m2 = m.clone();
                            let t = TestGraphRunner::new(Box::new(move |g| {
                                n2.apply(g);
                                m2.apply(g);
                            }));

                            let mut d = format!(
                                "Fuzzying error at node_count {node_count}, edge_count: {edge_count}\nerror: {:?}",
                                msg
                            );
                            let b = BACKTRACE.with(|b| b.borrow_mut().take()).unwrap();
                            d.push_str(&format!("Backtrace: {:?}", b));
                            d.push_str(&t.debug_());
                            println!("{}", d);
                            write_error_to_file(node_count, edge_count, &d[..]);
                        }
                    }
                    if !m.advance() {
                        break;
                    };
                    edge_count += 1;
                }
                node_count += 1;
                if !n.advance() {
                    break;
                };
            }
        }));
    }

    println!("waiting for threads to finish");
    for t in threads.into_iter() {
        t.join().unwrap();
    }
    println!("joined all");
    let elapsed = start_time.elapsed().as_secs_f32();
    let rate = total as f32 / elapsed;
    println!(
        "done running fuzz. Variations: {} (expected: {}). Time: {:.2}s. Rate {:.2}/s",
        total,
        node_total * edges_per_node_variation,
        elapsed,
        rate
    );
}
