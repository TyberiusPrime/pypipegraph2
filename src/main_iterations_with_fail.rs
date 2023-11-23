/*
* This doesn't fuzz.
* It creates *all* possible graphs up to 6 nodes,
* with all meaningfull edge variations
* (ie. if you ordered the nodes left to right,
* all edges point right. Since we permutate all nodes,
* that's all the DAGs, or at least their iso-equivalences)
*
* And it makes them fail 1..n-1 jobs by first running it without
* those jobs, then adding them in but having them fail.
*
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
    fn apply(&self, g: &mut PPGEvaluator<StrategyForTesting>, fails: &AllFails) {
        for (node_id, node_state) in self.state.iter().enumerate() {
            if fails.state[node_id] == 0 {
                let kind = match node_state {
                    0 => JobKind::Always,
                    1 => JobKind::Ephemeral,
                    2 => JobKind::Output,
                    _ => panic!(),
                };
                g.add_node(&format!("N{}", node_id), kind);
            }
        }
    }
    fn apply_(&self, g: &mut PPGEvaluator<StrategyForTesting>) {
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
    fn apply(&self, g: &mut PPGEvaluator<StrategyForTesting>, fails: &AllFails) {
        let mut edge_pos = 0;
        for n in 0..self.node_count {
            for m in (n + 1)..self.node_count {
                if self.state[edge_pos] == 1 && fails.state[n] == 0 && fails.state[m] == 0 {
                    g.depends_on(&format!("N{m}"), &format!("N{n}"));
                }
                edge_pos += 1;
            }
        }
    }
    fn apply_(&self, g: &mut PPGEvaluator<StrategyForTesting>) {
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

#[derive(Clone, Debug)]
struct AllFails {
    state: Vec<u8>,
}

impl AllFails {
    fn new(node_count: usize) -> AllFails {
        AllFails {
            state: vec![0; node_count],
        }
    }
    fn get_jobs_to_fail(&self) -> Vec<String> {
        let mut jobs_to_fail = Vec::new();
        for (node_id, node_state) in self.state.iter().enumerate() {
            if *node_state == 1 {
                jobs_to_fail.push(format!("N{}", node_id));
            }
        }
        return jobs_to_fail;
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

fn write_error_to_file(node_count: usize, edge_count: usize, fail_count: usize, message: &str) {
    std::fs::write(
        format!(
            "iteration_with_fail_error_{}_{}_{}.txt",
            node_count, edge_count, fail_count
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
    let skip = args
        .get(2)
        .unwrap_or(&"0".to_string())
        .parse::<usize>()
        .unwrap_or(0);
    println!("problem size: {}", problem_size);
    println!("skipping {}", skip);

    //let problem_size = 6;

    //

    let all_nodes = AllNodes::new(problem_size);
    let node_total = all_nodes.len();
    let edges_per_node_variation = AllEdges::new(problem_size).len();
    let fail_variations_per = AllFails::new(problem_size).len();
    let total = {
        println!("node variations: {}", node_total);
        println!("edge variations: {}", edges_per_node_variation);
        println!("fail variations: {}", fail_variations_per);
        let total = edges_per_node_variation * node_total * fail_variations_per;
        println!("Together: {}", total);
        total
    };
    let start_time = std::time::Instant::now();
    let errors: Arc<Mutex<Vec<(usize, usize)>>> = Arc::new(Mutex::new(Vec::new()));

    let mut threads = Vec::new();
    let max_thread_count = num_cpus::get(); //system cpu count ;
    let done_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));

    let chunk_size = all_nodes.len() / max_thread_count;
    println!("chunk size: {}", chunk_size);
    //now space from 0... n.len() into max_thread_count chunks
    let starts = (0..max_thread_count)
        .map(|i| i * chunk_size)
        .collect::<Vec<_>>();
    println!("{:?}, chunk_size {}", starts, chunk_size);

    for start in starts {
        let mut all_nodes = all_nodes.clone();
        let errors = errors.clone();
        let done_count = done_count.clone();
        threads.push(thread::spawn(move || {
            let mut node_count = 0;

            while node_count < start + skip {
                all_nodes.advance();
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
                let mut all_edges = AllEdges::new(problem_size);
                let mut edge_count = 0;
                loop { //for every edge...
                    let mut all_fails = AllFails::new(problem_size);
                    let mut fail_count = 0;
                    loop { //for all fails
                        let part1_ok = Arc::new(Mutex::new(false));
                        let part1_ok_outer = part1_ok.clone();
                        let res = std::panic::catch_unwind(|| {

                            //part1 is where the failing nodes are not in the graph?
                            let n2b = all_nodes.clone();
                            let m2b = all_edges.clone();
                            let f2b = all_fails.clone();
                            let mut t = TestGraphRunner::new(Box::new(move |g| {
                                n2b.apply(g, &f2b);
                                m2b.apply(g, &f2b);
                            }));
                            let failures = all_fails.get_jobs_to_fail();
                            let failures_ = failures.iter().map(|x| x.as_str()).collect::<Vec<_>>();
                            let g = t.run(&failures_[..]);
                            match g {
                                Ok(_) => {}
                                Err(e) => {
                                    println!(
                                        "error at node_count: {}, edge_count: {}",
                                        node_count, edge_count
                                    );
                                      let mut d = format!(
                                    "iteration_with_fail_part1 error at node_count {node_count}, edge_count: {edge_count}\nerror: {:?}",
                                    e
                                );
                                d.push_str(&t.debug_());
                                d.push_str(&format!("{:?}", all_fails.get_jobs_to_fail()));
                                //println!("{}", d);
                                write_error_to_file(node_count, edge_count, fail_count, &d[..]);
                                    errors.lock().unwrap().push((node_count, edge_count));
                                }
                            }
                            *part1_ok.lock().unwrap() = true;
                            //part2: now all nodes are in the graph
                            let n2b = all_nodes.clone();
                            let m2b = all_edges.clone();
                            t.setup_graph = Box::new(move |g| {
                                n2b.apply_(g);
                                m2b.apply_(g);
                            });
                                                       //convert Vec<String> into &[&str]
                            match t.run(&failures_[..]) {
                                Ok(_) => {}
                                Err(e) => {
                                    println!(
                                        "error at node_count: {}, edge_count: {}",
                                        node_count, edge_count
                                    );
                                      let mut d = format!(
                                    "iteration_with_fail_part2 error at node_count {node_count}, edge_count: {edge_count}\nerror: {:?}",
                                    e
                                );
                                d.push_str(&t.debug_());
                                d.push_str(&format!("{:?}", all_fails.get_jobs_to_fail()));
                                //println!("{}", d);
                                write_error_to_file(node_count, edge_count, fail_count, &d[..]);
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

                                println!("Caught panic! at node_count {}, edge_count: {}. fail_count: {}: error {:?}", node_count, edge_count, fail_count, msg);
                                errors.lock().unwrap().push((node_count, edge_count));
                                let n2 = all_nodes.clone();
                                let m2 = all_edges.clone();
                                let f2 = all_fails.clone();
                                //se we can print them.
                                let t1 = TestGraphRunner::new(Box::new(move |g| {
                                n2.apply(g, &f2);
                                m2.apply(g, &f2);
                                }));
                                let n2 = all_nodes.clone();
                                let m2 = all_edges.clone();

                                 let t2 = TestGraphRunner::new(Box::new(move |g| {
                                    n2.apply_(g);
                                    m2.apply_(g);
                                 }));

                                let mut d = format!(
                                    "Fuzzying error at node_count {node_count}, edge_count: {edge_count}\nerror: {:?}",
                                    msg
                                );
                                let b = BACKTRACE.with(|b| b.borrow_mut().take()).unwrap();
                                d.push_str(&format!("Backtrace: {:?}\n", b));
                                d.push_str("1st\n");
                                d.push_str(&t1.debug_());
                                d.push_str("2nd\n");
                                d.push_str(&t2.debug_());
                                d.push_str(&format!("fails: {:?}\n", all_fails.get_jobs_to_fail()).replace("N", ""));
                                d.push_str(&format!("part 1 ok {}\n", part1_ok_outer.lock().unwrap()));
                                //println!("{}", d);
                                write_error_to_file(node_count, edge_count, fail_count, &d[..]);
                            }
                        }
                        if !all_fails.advance() {
                            break;
                        };
                        fail_count += 1;
                    }
                    if !all_edges.advance() {
                        break;
                    };
                    edge_count += 1;
                }
                node_count += 1;
                if !all_nodes.advance() {
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
