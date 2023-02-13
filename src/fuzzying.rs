use pypipegraph2::{
    JobKind, PPGEvaluator, PPGEvaluatorStrategy, StrategyForTesting, TestGraphRunner,
};

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
}

fn main() {
    println!("running fuzz test.");

    let node_count = 3;

    let mut n = AllNodes::new(node_count);
    let mut count = 0;


    loop {
        let mut m = AllEdges::new(node_count);
        loop {
            {
                let n2 = n.clone();
                let m2 = m.clone();
                let mut t = TestGraphRunner::new(Box::new(move |g| {
                    n2.apply(g);
                    m2.apply(g);
                }));
                t.run(&[]).unwrap();
                t.run(&[]).expect("failuer");
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
    println!("done running fuzz {count}");
}
