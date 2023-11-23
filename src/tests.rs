#![allow(unused_variables)]
/* ↓ ←➔ ↑ */
#![allow(unused_macros)]
use std::collections::{HashMap, HashSet};
use std::{cell::RefCell, rc::Rc};

use crate::*;

macro_rules! set {
    ( $( $x:expr ),* ) => {  // Match zero or more comma delimited items
        {
            let mut temp_set = HashSet::new();  // Create a mutable HashSet
            $(
                temp_set.insert($x.to_string()); // Insert each item matched into the HashSet
            )*
            temp_set // Return the populated HashSet
        }
    };
}
#[test]
fn test_one_output() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(new_history.contains_key("A"));
    assert!(ro.run_counters.get("A") == Some(&1));
    error!("Run again");
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&1));
}

#[test]
pub fn test_three_outputs() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("out", JobKind::Output);
    g.add_node("out2", JobKind::Output);
    g.add_node("out3", JobKind::Output);
    g.depends_on("out2", "out");
    g.depends_on("out3", "out");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["out"]);
    g.event_now_running("out").unwrap();
    assert!(g.query_ready_to_run().is_empty());
    g.event_job_finished_success("out", "outAResult".to_string())
        .unwrap();
    assert_eq!(g.query_ready_to_run(), set!["out2", "out3"]);
    assert!(!g.is_finished());
    g.event_now_running("out2").unwrap();
    g.event_now_running("out3").unwrap();
    assert!(!g.is_finished());
    g.event_job_finished_success("out2", "out2output".to_string())
        .unwrap();
    assert!(!g.is_finished());
    g.event_job_finished_success("out3", "out3output".to_string())
        .unwrap();
    assert!(g.is_finished());
    let history = g.new_history().unwrap();
    dbg!(&history);
    assert!(history.get(&("out!!!out2".to_string())).unwrap() == "outAResult");
    assert!(history.get(&("out!!!out3".to_string())).unwrap() == "outAResult");
    dbg!(&history);
    assert!(history.len() == 2 + 3 + 3);

    // ok, but the history is not out2-> None, out3->None.
    // When I add a job, and I don't need to rerun out,
    //
    //assert!(history.get(&("out".to_string(), "out2".to_string())).unwrap() == "outAResult")
}

#[test]
#[should_panic]
pub fn test_simple_cycle() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("out", JobKind::Output);
    g.depends_on("out", "out");
}

#[test]
pub fn test_failure() {
    let mut his = HashMap::new();
    his.insert("Job_not_present".to_string(), "hello".to_string());
    let mut g = PPGEvaluator::new_with_history(his, StrategyForTesting::new());
    g.add_node("out", JobKind::Output);
    g.add_node("out2", JobKind::Output);
    g.add_node("out3", JobKind::Output);
    g.depends_on("out2", "out");
    g.depends_on("out3", "out2");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["out"]);
    g.event_now_running("out").unwrap();
    assert!(g.query_ready_to_run().is_empty());
    g.event_job_finished_failure("out").unwrap();
    assert!(g.query_ready_to_run().is_empty());
    assert!(g.is_finished());
    //we keep history that for jobs tha are currently not present
    assert!(g.new_history().unwrap().get("Job_not_present").is_some());
    assert!(g.new_history().unwrap().len() == 1);
}

#[test]
pub fn test_job_already_done() {
    let strat = StrategyForTesting::new();
    let mut his = HashMap::new();
    strat.already_done.borrow_mut().insert("out".to_string());
    his.insert("out".to_string(), "out".to_string());
    his.insert("out!!!".to_string(), "".to_string()); //the list of input jobs.
    let mut g = PPGEvaluator::new_with_history(his, strat);
    g.add_node("out", JobKind::Output);
    g.event_startup().unwrap();
    assert!(g.is_finished());
}

#[test]
pub fn simplest_ephemeral() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("out", JobKind::Output);
    g.add_node("in", JobKind::Ephemeral);
    g.depends_on("out", "in");
    g.event_startup().unwrap();
    assert!(!g.is_finished());
    assert_eq!(g.query_ready_to_run(), set!["in"]);
    g.event_now_running("in").unwrap();
    g.event_job_finished_success("in", "".to_string()).unwrap();

    assert!(!g.is_finished());
    assert_eq!(g.query_ready_to_run(), set!["out"]);
    dbg!(g.query_ready_for_cleanup());
    assert!(g.query_ready_for_cleanup().is_empty());

    g.event_now_running("out").unwrap();
    g.event_job_finished_success("out", "".to_string()).unwrap();
    assert!(g.is_finished());
    assert_eq!(g.query_ready_for_cleanup(), set!["in"]);
    g.event_job_cleanup_done("in").unwrap();
    dbg!(g.query_ready_for_cleanup());
    assert!(g.query_ready_for_cleanup().is_empty());
    assert!(g.query_ready_to_run().is_empty())
}

#[test]
pub fn ephemeral_output_already_done() {
    let mut his = HashMap::new();
    his.insert("in!!!out".to_string(), "".to_string());
    his.insert("in".to_string(), "".to_string());
    his.insert("in!!!".to_string(), "".to_string());
    his.insert("out!!!".to_string(), "in".to_string());
    his.insert("out".to_string(), "".to_string());
    let strat = StrategyForTesting::new();
    strat.already_done.borrow_mut().insert("out".to_string());
    let mut g = PPGEvaluator::new_with_history(his, strat);
    g.add_node("out", JobKind::Output);
    g.add_node("in", JobKind::Ephemeral);
    g.depends_on("out", "in");
    g.event_startup().unwrap();
    assert!(g.query_ready_to_run().is_empty());
    assert!(g.is_finished());
}

#[test]
pub fn output_of_leafs_captured() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Ephemeral);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "A");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[""]).unwrap();
    let history = g.new_history().unwrap();
    assert!(history.contains_key("A"));
    assert!(history.contains_key("A!!!"));
    assert!(history.contains_key("A!!!B"));
    assert!(history.contains_key("B!!!"));
    assert!(history.contains_key("B")); //the one we named the test after
}

#[test]
pub fn ephemeral_nested() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("A", JobKind::Output);
    g.add_node("B", JobKind::Ephemeral);
    g.add_node("C", JobKind::Output);
    g.add_node("D", JobKind::Ephemeral);
    g.add_node("E", JobKind::Output);
    g.depends_on("E", "D");
    g.depends_on("D", "C");
    g.depends_on("C", "B");
    g.depends_on("B", "A");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["A"]);
    assert!(!g.is_finished());

    g.event_now_running("A").unwrap();
    g.event_job_finished_success("A", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["B"]);
    assert!(!g.is_finished());

    g.event_now_running("B").unwrap();
    g.event_job_finished_success("B", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["C"]);
    assert!(!g.is_finished());

    g.event_now_running("C").unwrap();
    g.event_job_finished_success("C", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["D"]);
    assert!(!g.is_finished());

    g.event_now_running("D").unwrap();
    g.event_job_finished_success("D", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["E"]);
    assert!(!g.is_finished());

    g.event_now_running("E").unwrap();
    g.event_job_finished_success("E", "".to_string()).unwrap();
    assert!(g.query_ready_to_run().is_empty());
    assert!(g.is_finished());
}

#[test]
pub fn ephemeral_nested_first_already_present() {
    let strat = StrategyForTesting::new();
    strat.already_done.borrow_mut().insert("A".to_string());
    let mut hist = mk_history(&[
        (("E", "D"), ""),
        (("D", "C"), ""),
        (("C", "B"), ""),
        (("B", "A"), ""),
    ]);
    hist.insert("A!!!".to_string(), "".to_string());
    hist.insert("A".to_string(), "".to_string());
    let mut g = PPGEvaluator::new_with_history(hist, strat);
    //dbg!(&g.history);
    g.add_node("A", JobKind::Output);
    g.add_node("B", JobKind::Ephemeral);
    g.add_node("C", JobKind::Output);
    g.add_node("D", JobKind::Ephemeral);
    g.add_node("E", JobKind::Output);
    g.depends_on("E", "D");
    g.depends_on("D", "C");
    g.depends_on("C", "B");
    g.depends_on("B", "A");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["B"]);
    assert!(!g.is_finished());

    g.event_now_running("B").unwrap();
    g.event_job_finished_success("B", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["C"]);
    assert!(!g.is_finished());

    g.event_now_running("C").unwrap();
    g.event_job_finished_success("C", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["D"]);
    assert!(!g.is_finished());

    g.event_now_running("D").unwrap();
    g.event_job_finished_success("D", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["E"]);
    assert!(!g.is_finished());

    g.event_now_running("E").unwrap();
    g.event_job_finished_success("E", "".to_string()).unwrap();
    assert!(g.query_ready_to_run().is_empty());
    assert!(g.is_finished());
}
#[test]
pub fn ephemeral_nested_last() {
    /* ↓ ←➔ ↑
    *
    *
    Eo ←  De ← Co ← Be ← Ao


    E and C Are done.
    History is available for everything.
    We need to run A (which is missing it's output,
    but does not invalidate B.)

    * */
    let strat = StrategyForTesting::new();
    strat.already_done.borrow_mut().insert("E".to_string());
    strat.already_done.borrow_mut().insert("C".to_string());
    let mut g = PPGEvaluator::new_with_history(
        mk_history(&[
            (("B", "A"), ""),
            (("C", "B"), ""),
            (("D", "C"), ""),
            (("E", "D"), ""),
            //(("F", "E"), ""),
        ]),
        strat,
    );
    g.add_node("A", JobKind::Output);
    g.add_node("B", JobKind::Ephemeral);
    g.add_node("C", JobKind::Output);
    g.add_node("D", JobKind::Ephemeral);
    g.add_node("E", JobKind::Output);
    g.depends_on("E", "D");
    g.depends_on("D", "C");
    g.depends_on("C", "B");
    g.depends_on("B", "A");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["A"]);
    assert!(!g.is_finished());

    g.event_now_running("A").unwrap();
    g.event_job_finished_success("A", "".to_string()).unwrap();
    assert!(g.query_ready_to_run().is_empty());
    assert!(g.is_finished());
}

fn mk_history(input: &[((&str, &str), &str)]) -> HashMap<String, String> {
    let mut res: HashMap<String, String> = input
        .iter()
        .map(|((downstream, upstream), c)| {
            (format!("{}!!!{}", upstream, downstream), c.to_string())
        })
        .collect();
    res.extend(
        input
            .iter()
            .map(|((a, b), c)| (a.to_string(), c.to_string())),
    );
    res
}

#[test]
pub fn ephemeral_nested_inner() {
    let strat = StrategyForTesting::new();
    strat.already_done.borrow_mut().insert("C".to_string());
    let mut g = PPGEvaluator::new_with_history(
        mk_history(&[
            (("B", "A"), ""),
            (("D", "C"), ""),
            (("C", "B"), ""),
            (("B", "A"), ""),
        ]),
        strat,
    );
    g.add_node("A", JobKind::Output);
    g.add_node("B", JobKind::Ephemeral);
    g.add_node("C", JobKind::Output);
    g.add_node("D", JobKind::Ephemeral);
    g.add_node("E", JobKind::Output);
    g.depends_on("E", "D");
    g.depends_on("D", "C");
    g.depends_on("C", "B");
    g.depends_on("B", "A");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["A"]); // this changes with the 'ephemerals cant invalidate' rule. Case can invalidate, I presume
    assert!(!g.is_finished());

    g.event_now_running("A").unwrap();
    g.event_job_finished_success("A", "".to_string()).unwrap();
    assert!(!g.is_finished());
    // C was done. B only runs if C needs to be made, or if B is invalidated
    // but A's output didn't change, B did not get invalidated, and therefore,
    // B can not invalidate C.
    // But D is needed by E, which is a missing output.
    assert_eq!(g.query_ready_to_run(), set!["D"]);

    g.event_now_running("D").unwrap();
    g.event_job_finished_success("D", "".to_string()).unwrap();
    assert!(!g.is_finished());

    g.event_now_running("E").unwrap();
    g.event_job_finished_failure("E").unwrap();

    assert!(g.query_ready_to_run().is_empty());
    assert_eq!(g.query_failed(), set!["E"]);
    assert!(g.query_upstream_failed().is_empty());
}
#[test]
pub fn ephemeral_nested_upstream_failure() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("B", JobKind::Ephemeral);
    g.add_node("D", JobKind::Ephemeral);
    g.add_node("C", JobKind::Output);
    g.add_node("E", JobKind::Output);
    g.add_node("A", JobKind::Output);
    g.depends_on("B", "A");
    g.depends_on("E", "D");
    g.depends_on("D", "C");
    g.depends_on("C", "B");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["A"]);
    assert!(!g.is_finished());

    g.event_now_running("A").unwrap();
    g.event_job_finished_failure("A").unwrap();
    assert!(g.is_finished());

    assert_eq!(g.query_failed(), set!["A"]);
    assert_eq!(g.query_upstream_failed(), set!["B", "C", "D", "E"]);
}

#[test]
pub fn disjoint_and_twice() {
    let strat = StrategyForTesting::new();
    let already_done = strat.already_done.clone();
    let init = |history| {
        let strat = strat.clone();
        let mut g = PPGEvaluator::new_with_history(history, strat);
        g.add_node("A", JobKind::Output);
        g.add_node("B", JobKind::Output);
        g.add_node("C", JobKind::Output);
        g.depends_on("B", "A");
        g.depends_on("C", "B");
        g.add_node("d", JobKind::Output);
        g.add_node("e", JobKind::Output);
        g.depends_on("d", "e");
        g
    };

    error!("part 1");
    let mut g = init(HashMap::new());

    let history = {
        g.event_startup().unwrap();
        assert_eq!(g.query_ready_to_run(), set!["A", "e"]);
        g.event_now_running("A").unwrap();
        assert_eq!(g.query_ready_to_run(), set!["e"]);
        g.event_now_running("e").unwrap();
        assert!(g.query_ready_to_run().is_empty());
        g.event_job_finished_success("e", "histe".to_string())
            .unwrap();
        already_done.borrow_mut().insert("e".to_string());
        assert_eq!(g.query_ready_to_run(), set!["d"]);
        g.event_now_running("d").unwrap();
        g.event_job_finished_success("d", "histd".to_string())
            .unwrap();
        already_done.borrow_mut().insert("d".to_string());
        assert!(g.query_ready_to_run().is_empty());
        g.event_job_finished_success("A", "histA".to_string())
            .unwrap();
        already_done.borrow_mut().insert("A".to_string());
        assert_eq!(g.query_ready_to_run(), set!["B"]);
        g.event_now_running("B").unwrap();
        assert!(g.query_ready_to_run().is_empty());
        g.event_job_finished_success("B", "histB".to_string())
            .unwrap();
        already_done.borrow_mut().insert("B".to_string());
        assert_eq!(g.query_ready_to_run(), set!["C"]);
        g.event_now_running("C").unwrap();
        assert!(g.query_ready_to_run().is_empty());
        g.event_job_finished_success("C", "histC".to_string())
            .unwrap();
        already_done.borrow_mut().insert("C".to_string());
        g.abort_remaining().unwrap();
        g.new_history().unwrap()
    };
    error!("part 2");

    {
        let mut g2 = init(history.clone());
        g2.event_startup().unwrap();
        assert!(g2.is_finished());
    }

    error!("part 3");
    {
        already_done.borrow_mut().remove("C");
        let mut g2 = init(history.clone());
        g2.event_startup().unwrap();
        assert!(!g2.is_finished());
        assert_eq!(g2.query_ready_to_run(), set!["C"]);
        g2.event_now_running("C").unwrap();
        g2.event_job_finished_success("C", "histC".to_string())
            .unwrap();
        assert!(g2.is_finished());
    }
    debug!("part 4");
    {
        already_done.borrow_mut().insert("C".to_string());
        already_done.borrow_mut().remove("A");
        let mut g2 = init(history.clone());
        g2.event_startup().unwrap();
        assert!(!g2.is_finished());
        assert_eq!(g2.query_ready_to_run(), set!["A"]);
        g2.event_now_running("A").unwrap();
        g2.event_job_finished_success("A", "histA2".to_string())
            .unwrap();
        assert_eq!(g2.query_ready_to_run(), set!["B"]); // A history changed
        g2.event_now_running("B").unwrap();
        g2.event_job_finished_success("B", "histB".to_string())
            .unwrap(); // but b not changed
        assert!(g2.is_finished());
    }
}

#[test]
fn cant_start_twice() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    assert!(g.event_startup().is_ok());
    assert!(g.event_startup().is_err());
}

#[test]
fn terminal_ephemeral_singleton() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("B", JobKind::Ephemeral);

    g.event_startup().unwrap();
    assert!(g.query_ready_to_run().is_empty());
    assert!(g.query_ready_for_cleanup().is_empty());
    assert!(g.is_finished());
}

#[test]
fn terminal_ephemeral_24() {
    /*
    A ➔ TB

    So TB does not get run.
    Supposedly
    */

    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("A", JobKind::Output);
    g.add_node("TB", JobKind::Ephemeral);
    g.depends_on("TB", "A");
    info!("now startup");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["A"]);
    g.event_now_running("A").unwrap();
    g.event_job_finished_success("A", "histA2".to_string())
        .unwrap();
    assert!(g.query_ready_to_run().is_empty());
    assert!(g.query_ready_for_cleanup().is_empty());
    assert!(g.is_finished());
}

fn run_graph(
    mut g: PPGEvaluator<StrategyForTesting>,
    done_log: Rc<RefCell<HashSet<String>>>,
) -> HashMap<String, String> {
    g.event_startup().unwrap();
    while !g.is_finished() {
        for job_id in g.query_ready_to_run().iter() {
            g.event_now_running(job_id).unwrap();
            g.event_job_finished_success(job_id, format!("history_{}", job_id))
                .unwrap();
            done_log.borrow_mut().insert(job_id.clone());
        }
    }
    g.new_history().unwrap()
}

#[test]
fn test_run_then_add_jobs() {
    let strat = StrategyForTesting::new();
    let init = |history| {
        let strat = strat.clone();
        let mut g = PPGEvaluator::new_with_history(history, strat);
        g.add_node("A", JobKind::Output);
        g
    };
    let g = init(HashMap::new());
    let history = run_graph(g, strat.already_done.clone());

    error!("part2");
    let mut g = init(history);
    g.add_node("B", JobKind::Output);
    g.depends_on("B", "A");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["B"]);
    g.event_now_running("B").unwrap();
    g.event_job_finished_success("B", "history_b".to_string())
        .unwrap();
    assert!(g.is_finished());
    let history = g.new_history().unwrap();
    dbg!(&history);
    assert_eq!(history.get("A!!!B"), Some(&"history_A".to_string()));
}

#[test]
fn test_issue_20210726a() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("J0", JobKind::Output);
    g.add_node("J2", JobKind::Ephemeral);
    g.add_node("J3", JobKind::Ephemeral);
    g.add_node("J76", JobKind::Output);

    g.depends_on("J0", "J2");
    g.depends_on("J2", "J3");
    g.depends_on("J2", "J76");
    g.depends_on("J76", "J3");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["J3"]);
    g.event_now_running("J3").unwrap();
    g.event_job_finished_success("J3", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["J76"]);
    g.event_now_running("J76").unwrap();
    g.event_job_finished_success("J76", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["J2"]);

    g.event_now_running("J2").unwrap();
    g.event_job_finished_success("J2", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["J0"]);
    g.event_now_running("J0").unwrap();
    g.event_job_finished_success("J0", "".to_string()).unwrap();
    assert!(g.is_finished())
}
#[test]
fn test_issue_20211001() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("J3", JobKind::Ephemeral);
    g.add_node("J48", JobKind::Ephemeral);
    g.add_node("J61", JobKind::Output);
    g.add_node("J67", JobKind::Always);

    g.depends_on("J61", "J48");
    g.depends_on("J67", "J48");
    g.depends_on("J61", "J3");

    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["J3", "J48"]);
    g.event_now_running("J3").unwrap();
    g.event_job_finished_success("J3", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["J48"]);
    g.event_now_running("J48").unwrap();
    g.event_job_finished_success("J48", "".to_string()).unwrap();
    assert_eq!(g.query_ready_to_run(), set!["J61", "J67"]);
    g.event_now_running("J67").unwrap();
    g.event_job_finished_success("J67", "".to_string()).unwrap();
    g.event_now_running("J61").unwrap();
    g.event_job_finished_success("J61", "".to_string()).unwrap();

    assert!(g.is_finished())
}
#[test]
#[should_panic]
fn test_adding_node_twice() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("J3", JobKind::Ephemeral);
    g.add_node("J3", JobKind::Ephemeral);
}
#[test]
fn test_ephemeral_not_running_without_downstreams() {
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("J3", JobKind::Ephemeral);
    g.event_startup().unwrap();
    assert!(g.is_finished()); // it's not running...

    debug!("part 2");
    let mut g = PPGEvaluator::new(StrategyForTesting::new());
    g.add_node("J3", JobKind::Ephemeral);
    g.add_node("J4", JobKind::Ephemeral);
    g.add_node("J5", JobKind::Ephemeral);
    g.add_node("J6", JobKind::Ephemeral);
    g.add_node("A1", JobKind::Always);
    g.depends_on("J3", "J4");
    g.depends_on("J5", "J6");
    g.depends_on("J6", "J3");
    g.event_startup().unwrap();
    assert!(!g.is_finished());
    g.event_now_running("A1").unwrap();
    g.event_job_finished_failure("A1").unwrap();
    assert!(g.is_finished());
    let his = g.new_history().unwrap();
    assert_eq!(his.len(), 0); //since nothing succeeded
    for k in his.keys() {
        assert!(k.ends_with("!!!"));
    }
}
#[test]
fn test_simple_graph_runner() {
    let mut ro = TestGraphRunner::new(Box::new(|g| {
        g.add_node("A", JobKind::Output);
    }));
    let g = ro.run(&Vec::new());
    assert_eq!(*ro.run_counters.get("A").unwrap(), 1);
    //does not get rerun
    error!("part2");
    let g = ro.run(&Vec::new());
    assert_eq!(*ro.run_counters.get("A").unwrap(), 1);
    ro.already_done.remove("A");
    error!("part3");
    let g = ro.run(&Vec::new());
    assert_eq!(*ro.run_counters.get("A").unwrap(), 2);

    ro.setup_graph = Box::new(|g| {
        g.add_node("A", JobKind::Output);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "A");
    });
    error!("part4");
    let g = ro.run(&Vec::new());
    assert_eq!(*ro.run_counters.get("A").unwrap(), 2);
    assert_eq!(*ro.run_counters.get("B").unwrap(), 1);

    error!("part5");
    let g = ro.run(&Vec::new());
    assert_eq!(*ro.run_counters.get("A").unwrap(), 2);
    assert_eq!(*ro.run_counters.get("B").unwrap(), 1);
    ro.already_done.remove("A"); //which then gives a different history

    error!("part6");
    let g = ro.run(&Vec::new());
    assert_eq!(*ro.run_counters.get("A").unwrap(), 3);
    assert_eq!(*ro.run_counters.get("B").unwrap(), 1); //b has some a->b history, no trigger

    ro.history
        .insert("A!!!B".to_string(), "changedA".to_string());
    ro.already_done.remove("A"); //which then gives a different history
    error!("final part");
    let g = ro.run(&Vec::new());
    assert_eq!(*ro.run_counters.get("A").unwrap(), 4);
    assert_eq!(*ro.run_counters.get("B").unwrap(), 2); // now we trigger
}

#[test]
fn test_nested_too_deply_detection() {
    static MAX_NEST: u32 = 25;
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        let c = MAX_NEST - 1;
        for ii in 0..c {
            g.add_node(&format!("A{}", ii), JobKind::Output);
        }
        for ii in 1..c {
            g.depends_on(&format!("A{}", ii - 1), &format!("A{}", ii));
        }
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    ro.allowed_nesting = MAX_NEST;
    let g = ro.run(&Vec::new()).unwrap();
}

#[test]
fn test_bigish_linear_graph() {
    crate::test_big_linear_graph(1000);
    crate::test_big_linear_graph_half_ephemeral(1000);
}

#[test]
fn test_ephemeral_triangle_just() {
    /* ↓ ←➔ ↑
      *
      *
      TA         TB
       |➔  TC  ← |
           ↓
           D
    */

    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("TC", JobKind::Ephemeral);
        g.add_node("D", JobKind::Output);
        g.depends_on("TC", "TA");
        g.depends_on("TC", "TB");
        g.depends_on("D", "TC");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("TC") == Some(&1));
    assert!(ro.run_counters.get("D") == Some(&1));

    error!("part2");
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("TC") == Some(&1));
    assert!(ro.run_counters.get("D") == Some(&1));
}

#[test]
fn test_ephemeral_triangle_plus() {
    /*
          TA         TB
           |➔  TC  ← |
                ↓
                D ← Ea
    */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("TC", JobKind::Ephemeral);
        g.add_node("D", JobKind::Output);
        g.add_node("E", JobKind::Always);
        g.depends_on("TC", "TA");
        g.depends_on("TC", "TB");
        g.depends_on("D", "TC");
        g.depends_on("D", "E");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("TC") == Some(&1));
    assert!(ro.run_counters.get("D") == Some(&1));
    assert!(ro.run_counters.get("E") == Some(&1));

    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("TC") == Some(&1));
    assert!(ro.run_counters.get("D") == Some(&1));
    assert!(ro.run_counters.get("E") == Some(&2));

    ro.outputs
        .insert("E".to_string(), "trigger_inval".to_string());
    let g = ro.run(&Vec::new()).unwrap();

    dbg!(&ro.run_counters);
    assert!(ro.run_counters.get("TA") == Some(&2));
    assert!(ro.run_counters.get("TB") == Some(&2));
    assert!(ro.run_counters.get("TC") == Some(&2));
    assert!(ro.run_counters.get("D") == Some(&2));
    assert!(ro.run_counters.get("E") == Some(&3));
}

#[test]
fn test_ephemeral_output_triangle_plus() {
    //same situation as test_ephemeral_triangle_plus, but TC is an now output!
    /*
      TA         TB
       |➔  C  ← |
            ↓
            D ← Ea
    */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output); // !!!
        g.add_node("D", JobKind::Output);
        g.add_node("E", JobKind::Always);
        g.depends_on("C", "TA");
        g.depends_on("C", "TB");
        g.depends_on("D", "C");
        g.depends_on("D", "E");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));
    assert!(ro.run_counters.get("D") == Some(&1));
    assert!(ro.run_counters.get("E") == Some(&1));

    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));
    assert!(ro.run_counters.get("D") == Some(&1));
    assert!(ro.run_counters.get("E") == Some(&2));

    ro.outputs
        .insert("E".to_string(), "trigger_inval".to_string());
    let g = ro.run(&Vec::new()).unwrap();

    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1)); // an output does not get rerun..
    assert!(ro.run_counters.get("D") == Some(&2));
    assert!(ro.run_counters.get("E") == Some(&3));
}

#[test]
fn test_ephemeral_downstream_invalidated() {
    /*
       TA ➔  B
       becomes


       TA ➔  B
             ↑
            FI52

    which triggers an invalidation on B
    and a rebuild on TA,
     * */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "TA");
    }
    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "TA");

        g.add_node("FI52", JobKind::Always);
        g.depends_on("B", "FI52");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(new_history.contains_key("TA"));
    assert!(new_history.contains_key("B"));
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));
    error!("Part2");

    ro.setup_graph = Box::new(create_graph2);
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(new_history.contains_key("FI52"));
    dbg!(&ro.run_counters);
    assert!(ro.run_counters.get("FI52") == Some(&1));
    assert!(ro.run_counters.get("TA") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&2));
}

#[test]
fn test_ephemeral_leaf_invalidated() {
    /*
        TA ➔ TB ➔ C
        becomes
        TA ➔ TB ➔ C
                  ↑
                FI52
    */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output);
        g.depends_on("C", "TB");
        g.depends_on("TB", "TA");
    }
    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output);
        g.add_node("FI52", JobKind::Always);
        g.depends_on("C", "FI52");
        g.depends_on("C", "TB");
        g.depends_on("TB", "TA");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(new_history.contains_key("TA"));
    assert!(new_history.contains_key("TB"));
    assert!(new_history.contains_key("C"));
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));

    error!("Part2");

    ro.setup_graph = Box::new(create_graph2);
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(new_history.contains_key("FI52"));
    dbg!(&ro.run_counters);
    assert!(ro.run_counters.get("FI52") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&2));
    assert!(ro.run_counters.get("TA") == Some(&2));
    assert!(ro.run_counters.get("TB") == Some(&2));
}

#[test]

fn test_loosing_an_input_is_invalidating() {
    /*

      A -> C
      becomes
      C

    */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Always);
        g.add_node("C", JobKind::Output);
        g.depends_on("C", "A");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));

    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("C", JobKind::Output);
    }
    error!("part2");
    ro.setup_graph = Box::new(create_graph2);
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(new_history.contains_key("C"));
    assert!(new_history.contains_key("A"));
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&2));
}

#[test]
fn test_changing_inputs_when_leaf_was_missing() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "A");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));

    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
        g.add_node("C", JobKind::Always);
        g.depends_on("A", "C")
    }
    ro.setup_graph = Box::new(create_graph2);

    //we change the outut. because of C this actually takes effect.
    ro.outputs.insert("A".to_string(), "new".to_string());
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(new_history.contains_key("B"));
    assert!(new_history.contains_key("B!!!"));
    assert!(new_history.contains_key("A!!!B"));
    assert!(new_history.contains_key("A"));
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));

    // running it again doesn't run anything but the always job.
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    //assert!(new_history.contains_key("B"));
    assert!(new_history.contains_key("A"));
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&2));

    fn create_graph3(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
        g.add_node("C", JobKind::Always);
        g.depends_on("A", "C");
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "A");
    }
    error!("part3");
    ro.setup_graph = Box::new(create_graph3);
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&2)); // A's output changed between runs where we had
                                                   // B.
                                                   // So rerun is appropriate
    assert!(ro.run_counters.get("C") == Some(&3));

    //now run again without B.
    error!("part4");
    ro.setup_graph = Box::new(create_graph2);
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&2));
    assert!(ro.run_counters.get("C") == Some(&4));

    //and readding B, but A's output did not change.
    error!("part5");
    ro.setup_graph = Box::new(create_graph2);
    ro.setup_graph = Box::new(create_graph3);
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&2));
    assert!(ro.run_counters.get("C") == Some(&5));

    //now retriggering A, by removing C, while B is missing
    fn create_graph4(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
    }
    ro.setup_graph = Box::new(create_graph4);
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&3));

    //and readding B, but keeping the same A output.
    error!("part7");
    ro.setup_graph = Box::new(create_graph);
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&3));
    assert!(ro.run_counters.get("B") == Some(&2));
}
#[test]
fn test_replacing_an_input_then_restoring() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Always);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "A");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));

    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&1));

    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("B", JobKind::Output);
        g.add_node("C", JobKind::Always);
        g.depends_on("B", "C")
    }
    ro.setup_graph = Box::new(create_graph2);

    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(new_history.contains_key("C"));
    assert!(new_history.contains_key("B"));
    //assert!(new_history.contains_key("A"));
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&2));
    assert!(ro.run_counters.get("C") == Some(&1));

    let g = ro.run(&Vec::new()).unwrap();
    dbg!(&ro.run_counters);
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&2)); // is this
    assert!(ro.run_counters.get("C") == Some(&2));

    ro.setup_graph = Box::new(create_graph); // back to the original one.

    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&3));
    assert!(ro.run_counters.get("B") == Some(&3));
    assert!(ro.run_counters.get("C") == Some(&2));
}

#[test]
fn test_two_ephemerals_one_output_straight() {
    /*
         /* ↓ ← ➔ ↑ */


        A(e)      B(e)
           |        |
           |➔ C(o) ←|

         *

    */
    //
    // actually A diamend...
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Ephemeral);
        g.add_node("B", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output);
        g.depends_on("C", "A");
        g.depends_on("C", "B");
        //g.depends_on("B", "A");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));

    error!("part2");
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));
}

#[test]
fn test_two_ephemerals_one_output_crosslinked() {
    //
    // actually A diamend...
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Ephemeral);
        g.add_node("B", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output);
        g.depends_on("C", "A");
        g.depends_on("C", "B");
        g.depends_on("B", "A");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));

    error!("part2");
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));
}

/* #[test] some as test_one_ephemeral_two_outputs part 1 & 2
fn test_ephemeral_one_ephemeral_two_downstreams() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("B", JobKind::Output);
        g.add_node("C", JobKind::Output);
        g.depends_on("B", "TA");
        g.depends_on("C", "TA");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    let new_history = g.new_history().unwrap();
    assert!(new_history.contains_key("TA"));
    assert!(new_history.contains_key("B"));
    assert!(new_history.contains_key("C"));
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));

    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));
} */

#[test]
fn test_one_ephemeral_two_outputs() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("B", JobKind::Output);
        g.add_node("C", JobKind::Output);
        g.depends_on("B", "TA");
        g.depends_on("C", "TA");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    //    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));

    //let's add a third output...
    //
    error!("part3");
    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("B", JobKind::Output);
        g.add_node("C", JobKind::Output);
        g.add_node("D", JobKind::Output);
        g.depends_on("B", "TA");
        g.depends_on("C", "TA");
        g.depends_on("D", "TA");
    }
    ro.setup_graph = Box::new(create_graph2);
    let g = ro.run(&Vec::new()).unwrap();
    //    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("TA") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));
    assert!(ro.run_counters.get("D") == Some(&1));
}
#[test]
fn test_epheremal_chained_invalidate_intermediate() {
    /*

       TA ➔  TB ➔  C
       becomes
       TA ➔  TB ➔  C
              ↑
              D
    */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output);
        g.depends_on("TB", "TA");
        g.depends_on("C", "TB");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    //    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));
    error!("part2");
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));

    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output);
        g.depends_on("TB", "TA");
        g.depends_on("C", "TB");

        g.add_node("D", JobKind::Always);
        g.depends_on("TB", "D");
    }

    ro.setup_graph = Box::new(create_graph2);

    error!("part 3");

    let g = ro.run(&Vec::new()).unwrap();
    debug!("{:?}", &ro.run_counters);
    assert!(ro.run_counters.get("D") == Some(&1));
    assert!(ro.run_counters.get("TA") == Some(&2));
    assert!(ro.run_counters.get("TB") == Some(&2));
    assert!(ro.run_counters.get("C") == Some(&1));

    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("D") == Some(&2));
    assert!(ro.run_counters.get("TA") == Some(&2));
    assert!(ro.run_counters.get("TB") == Some(&2));
    assert!(ro.run_counters.get("C") == Some(&1));
}

#[test]
fn test_ephemeral_adding_output() {
    /*

       TA ➔  TB ➔  C
       becomes
       TA ➔  TB ➔  C
              ↓
              D
    */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output);
        g.depends_on("TB", "TA");
        g.depends_on("C", "TB");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    //    let new_history = g.new_history().unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));
    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        create_graph(g);

        g.add_node("D", JobKind::Output);
        g.depends_on("D", "TB");
    }

    ro.setup_graph = Box::new(create_graph2);

    error!("part 3");

    let g = ro.run(&Vec::new()).unwrap();
    debug!("{:?}", &ro.run_counters);
    assert!(ro.run_counters.get("D") == Some(&1));
    assert!(ro.run_counters.get("TA") == Some(&2));
    assert!(ro.run_counters.get("TB") == Some(&2));
    assert!(ro.run_counters.get("C") == Some(&1));

    error!("part 4");
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("D") == Some(&1));
    assert!(ro.run_counters.get("TA") == Some(&2));
    assert!(ro.run_counters.get("TB") == Some(&2));
    assert!(ro.run_counters.get("C") == Some(&1));
}

#[test]
fn test_ephemeral_tritri() {
    // actually A diamend...
    /*
        /* ↓ ←➔ ↑ */

        TA ➔   TB ➔ TC
        ➔  TD  ←
            ↓
            E
    * */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("TC", JobKind::Ephemeral);
        g.add_node("TD", JobKind::Ephemeral);
        g.add_node("E", JobKind::Output);
        g.depends_on("TB", "TA");
        g.depends_on("TC", "TB");
        g.depends_on("TD", "TB");
        g.depends_on("TD", "TA");
        g.depends_on("E", "TD");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    dbg!(&ro.run_counters);
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("TC") == None); // no downstream, no running
    assert!(ro.run_counters.get("TD") == Some(&1));
    assert!(ro.run_counters.get("E") == Some(&1));

    error!("part2");
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("TC") == None);
    assert!(ro.run_counters.get("TD") == Some(&1));
    assert!(ro.run_counters.get("E") == Some(&1));
}
#[test]
fn test_ephemeral_diamond_networked() {
    // actually A diamend...
    /*

           TA
         ↓     ↓
         TB ➔ TC
         ↓     ↓
           TD
            ↓
            E
    * */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("TC", JobKind::Ephemeral);
        g.add_node("TD", JobKind::Ephemeral);
        g.add_node("E", JobKind::Output);
        g.depends_on("TB", "TA");
        g.depends_on("TC", "TA");
        g.depends_on("TD", "TB");
        g.depends_on("TD", "TC");
        g.depends_on("E", "TD");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    dbg!(&ro.run_counters);
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("TC") == Some(&1)); // no downstream, no running
    assert!(ro.run_counters.get("TD") == Some(&1));
    assert!(ro.run_counters.get("E") == Some(&1));

    error!("part2");
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("TC") == Some(&1)); // no downstream, no running
    assert!(ro.run_counters.get("TD") == Some(&1));
    assert!(ro.run_counters.get("E") == Some(&1));
}

#[test]
pub fn test_adding_ephemeral_triggers_rebuild() {
    /* ↓ ←➔ ↑
        TB ➔  C
        becomes
        TA ➔ TB ➔  C

    *
     * */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        //g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output);
        g.depends_on("C", "TB");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));

    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output);
        g.depends_on("C", "TB");
        g.depends_on("TB", "TA");
    }

    ro.setup_graph = Box::new(create_graph2);

    error!("part2");
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&2)); // insulated
    assert!(ro.run_counters.get("C") == Some(&1));
}

#[test]
fn test_ephemeral_retriggered_changing_output() {
    /*
     * TA -> B
      becomes
      TA -> B
      ↓
      C
    */
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "TA");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));

    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        create_graph(g);
        g.add_node("C", JobKind::Output);
        g.depends_on("C", "TA"); // so this retrigers
    }

    ro.setup_graph = Box::new(create_graph2);
    ro.outputs.insert("TA".to_string(), "changed".to_string());

    start_logging();
    error!("part2");
    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&2));
    let failed = g.query_failed();
    dbg!(&failed);
    assert!(failed.contains("TA"));
    assert!(failed.len() == 1);
    let upstream_failed = g.query_upstream_failed();
    assert!(upstream_failed.contains("B"));
    assert!(upstream_failed.contains("C"));
    assert!(upstream_failed.len() == 2);
    // an ephemeral must not change it's output
    // when it get's retriggered (vs invalidated).
    // This should lead to an error,
    // and a failing of all downstreams.
    //
    //
}

#[test]
fn test_no_storing_removed_links_in_history() {
    // ie when a link between two existing jobs is missing
    // we no longer store that link
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "A");
    }

    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    ro.outputs.insert("A".to_string(), "AAA".to_string());
    let g = ro.run(&Vec::new()).unwrap();
    let history = g.new_history().unwrap();
    assert!(history.contains_key("A!!!B"));
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));

    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
        g.add_node("B", JobKind::Output);
    }
    ro.setup_graph = Box::new(create_graph2);
    ro.outputs.insert("A".to_string(), "AAAA".to_string());
    ro.already_done.remove("A");
    let g = ro.run(&Vec::new()).unwrap();
    let history = g.new_history().unwrap();
    assert!(!history.contains_key("A!!!B"));
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&2)); // after all, we lost an input

    ro.setup_graph = Box::new(create_graph);
    let g = ro.run(&Vec::new()).unwrap();
    let history = g.new_history().unwrap();
    assert!(history.contains_key("A!!!B"));
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&3)); // we regained the input.
}
#[test]
fn test_job_removed_input_changed_job_restored() {
    // ie when a link between two existing jobs is missing
    // we no longer store that link
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "A");
    }

    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    ro.outputs.insert("A".to_string(), "AAA".to_string());
    let g = ro.run(&Vec::new()).unwrap();
    let history = g.new_history().unwrap();
    assert!(history.contains_key("A!!!B"));
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("B") == Some(&1));

    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
    }
    ro.setup_graph = Box::new(create_graph2);
    ro.outputs.insert("A".to_string(), "AAAA".to_string());
    ro.already_done.remove("A");
    let g = ro.run(&Vec::new()).unwrap();
    let history = g.new_history().unwrap();
    assert!(history.contains_key("A!!!B"));
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&1)); // after all, we lost an input
                                                   //
                                                   //
    ro.setup_graph = Box::new(create_graph);
    let g = ro.run(&Vec::new()).unwrap();
    let history = g.new_history().unwrap();
    assert!(history.contains_key("A!!!B"));
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.run_counters.get("B") == Some(&2)); // we regained
}

#[test]
fn test_ephemeral_two_chained_with_always_inputs_dont_run() {
    // ie when a link between two existing jobs is missing
    // we no longer store that link
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.depends_on("TB", "TA");

        g.add_node("FIA", JobKind::Always);
        g.add_node("FIB", JobKind::Always);
        g.depends_on("TA", "FIA");
        g.depends_on("TB", "FIB");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();

    assert!(ro.run_counters.get("FIA") == Some(&1));
    assert!(ro.run_counters.get("FIB") == Some(&1));
    assert!(ro.run_counters.get("TA") == None);
    assert!(ro.run_counters.get("TB") == None);
}

#[test]
fn test_two_temp_jobs() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("TB", JobKind::Ephemeral);
        g.add_node("C", JobKind::Output);
        g.add_node("D", JobKind::Output);
        g.depends_on("C", "TA");
        g.depends_on("C", "TB");
        g.depends_on("D", "TB");

        g.add_node("FiTA", JobKind::Always);
        //g.add_node("FiTB", JobKind::Always);
        //g.add_node("FiC", JobKind::Always);
        //g.add_node("FiD", JobKind::Always);
        g.depends_on("TA", "FiTA");
        //g.depends_on("TB", "FiTB");
        //g.depends_on("C", "FiC");
        //g.depends_on("D", "FiD");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&Vec::new()).unwrap();

    let g = ro.run(&Vec::new()).unwrap();
    assert!(ro.run_counters.get("TA") == Some(&1));
    assert!(ro.run_counters.get("TB") == Some(&1));
    assert!(ro.run_counters.get("C") == Some(&1));
    assert!(ro.run_counters.get("D") == Some(&1));
}

#[test]
fn test_file_exists() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("b", JobKind::Output);
        g.add_node("load_b", JobKind::Ephemeral);
        g.add_node("A", JobKind::Output);
        g.add_node("FIA", JobKind::Always);
        g.depends_on("load_b", "b");
        g.depends_on("A", "load_b");
        g.depends_on("A", "FIA");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    ro.already_done.insert("b".to_string());
    let g = ro.run(&Vec::new()).unwrap();

    assert!(ro.run_counters.get("FIA") == Some(&1));
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(ro.run_counters.get("load_b") == Some(&1));
    assert!(ro.run_counters.get("b") == Some(&1)); // we had no history, so we need to rerun
}

#[test]
fn test_failure_does_not_store_history_link() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Always);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "A");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));

    let g = ro.run(&["B"]).unwrap();
    assert!(!ro.already_done.contains("B"));
    let history = g.new_history().unwrap();
    assert!(!history.contains_key("A!!!B"));
    assert!(ro.run_counters.get("B") == Some(&1));

    let g = ro.run(&[]).unwrap();
    let history = g.new_history().unwrap();
    assert!(ro.run_counters.get("B") == Some(&2));
    assert!(ro.already_done.contains("B"));
    assert!(history.contains_key("A!!!B"));
    let old_ab = history.get("A!!!B").unwrap().to_string();

    // retrigger
    ro.outputs
        .insert("A".to_string(), "trigger_inval".to_string());
    let g = ro.run(&["B"]).unwrap();
    let history = g.new_history().unwrap();
    assert!(ro.run_counters.get("B") == Some(&3));
    assert!(*history.get("A!!!B").unwrap() == old_ab);
}

#[test]
fn test_failure_does_not_store_history_for_job() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Always);
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&["A"]).unwrap();
    let history = g.new_history().unwrap();
    dbg!(&history);
    assert!(history.is_empty());
    assert!(ro.run_counters.get("A") == Some(&1));
    assert!(!ro.already_done.contains("A"));

    let g = ro.run(&[]).unwrap();
    let history = g.new_history().unwrap();
    assert!(!history.is_empty());
    assert!(ro.run_counters.get("A") == Some(&2));
    assert!(ro.already_done.contains("A"));
}

#[test]
fn test_no_cleanup_if_downstream_failes() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("TA", JobKind::Ephemeral);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "TA");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&["B"]).unwrap();
    assert!(!ro.already_done.contains("B"));
    assert!(ro.already_done.contains("TA"));
    assert!(!ro.cleaned_up.contains("TA"));
    let history = g.new_history().unwrap();
}
#[test]
fn test_correct_storing_of_skipped_ephemerals() {
    crate::test_big_graph_in_layers(2, 3, 2);
}

#[test]
fn test_if_present_but_history_removed() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Always);
        g.add_node("B", JobKind::Output);
        g.depends_on("B", "A");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    assert!(ro.run_counters.get("B") == Some(&1));
    let g = ro.run(&[]).unwrap();
    assert!(ro.run_counters.get("B") == Some(&1));
    ro.history.remove("B");
    let g = ro.run(&[]).unwrap();
    assert!(ro.run_counters.get("B") == Some(&2));
}

#[test]
fn test_upstream_failure_but_history_still_captured() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
        g.add_node("B", JobKind::Output);
        g.add_node("C", JobKind::Output);
        g.add_node("D:E:F", JobKind::Output);
        g.depends_on("B", "A");
        g.depends_on("C", "B");
        g.depends_on("D:E:F", "B");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    let history = g.new_history().unwrap();
    assert!(history.contains_key("A"));
    assert!(history.contains_key("B"));
    assert!(history.contains_key("C"));
    assert!(history.contains_key("D:E:F"));

    ro.already_done.remove("A");
    let g = ro.run(&["A"]).unwrap();
    assert!(g.query_failed().len() == 1);
    let history = g.new_history().unwrap();
    assert!(history.contains_key("B"));
    assert!(history.contains_key("C"));

    assert!(history.contains_key("B!!!"));
    assert!(history.contains_key("C!!!"));
    assert!(history.contains_key("D:E:F"));
    assert!(history.contains_key("D:E:F!!!"));

    assert!(!history.contains_key("A"));
    assert!(!history.contains_key("A!!!"));

    assert!(history.contains_key("A!!!B"));
    assert!(history.contains_key("B!!!C"));
    assert!(history.contains_key("B!!!D:E:F"));
}
#[test]

fn test_fuzz_0() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Ephemeral);
        g.add_node("N1", JobKind::Output);
        g.add_node("N2", JobKind::Output);
        g.depends_on("N1", "N0");
        g.depends_on("N2", "N0");
        g.depends_on("N2", "N1");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    let g = ro.run(&[]).unwrap();
}

#[test]
fn test_field_230202a() {
    //this is definatly a bug, but 291 is ephemeral,
    //so it *shouldn't matter*
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("239", JobKind::Ephemeral);
        g.add_node("289", JobKind::Output);
        g.add_node("291", JobKind::Ephemeral);

        let edges = vec![("239", "291"), ("239", "289"), ("289", "291")];

        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(b, a);
                println!("(\"{}\", \"{}\"),", a, b);
            }
        }
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();

    let mut g = ro.run(&[]).unwrap();
    assert!(g.is_finished());
}

#[test]
fn test_field_230202b() {
    //this is definatly a bug, but C is ephemeral,
    //so it *shouldn't matter*
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Ephemeral);
        g.add_node("B", JobKind::Output);
        g.add_node("C", JobKind::Ephemeral);
        g.add_node("D", JobKind::Output);

        let edges = vec![("A", "C"), ("A", "B"), ("B", "C"), ("C", "D")];

        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(b, a);
                println!("(\"{}\", \"{}\"),", a, b);
            }
        }
    }

    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let mut g = ro.run(&[]).unwrap();
    assert!(g.is_finished());

    let mut g = ro.run(&[]).unwrap();
    assert!(g.is_finished());

    println!("done");
}

#[test]
fn test_fuzz_1() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Ephemeral);
        g.add_node("N1", JobKind::Output);
        g.add_node("N2", JobKind::Ephemeral);
        g.depends_on("N1", "N0");
        g.depends_on("N2", "N0");
        g.depends_on("N2", "N1");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    let g = ro.run(&[]).unwrap();
}
#[test]
fn test_fuzz_2() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Always);
        g.add_node("N1", JobKind::Ephemeral);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Ephemeral);
        g.depends_on("N3", "N2");
        g.depends_on("N3", "N1");
        g.depends_on("N2", "N0");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    let g = ro.run(&[]).unwrap();
}

#[test]
fn test_fuzz_3() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Always);
        g.add_node("N1", JobKind::Ephemeral);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Ephemeral);
        g.add_node("N4", JobKind::Output);
        g.depends_on("N4", "N0");
        g.depends_on("N2", "N1");
        g.depends_on("N3", "N1");
        g.depends_on("N4", "N1");
        g.depends_on("N3", "N2");
        g.depends_on("N4", "N3");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    let g = ro.run(&[]).unwrap();
}

#[test]
fn test_fuzz_4() {
    //always can go from undetermined to ready to run when it's getting validated
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("0", JobKind::Ephemeral);
        g.add_node("1", JobKind::Output);
        g.add_node("2", JobKind::Output);
        g.add_node("3", JobKind::Ephemeral);
        g.add_node("4", JobKind::Output);
        g.add_node("5", JobKind::Always);
        let edges = vec![
            ("1", "0"),
            ("4", "0"),
            ("2", "1"),
            ("4", "1"),
            ("5", "1"),
            ("5", "2"),
            ("4", "3"),
            ("5", "4"),
        ];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }

    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    let g = ro.run(&[]).unwrap();
}

#[test]
fn test_fuzz_5() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N1", JobKind::Always);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Output);
        let edges = vec![("N3", "N1"), ("N3", "N2")];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }
    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Output);
        g.add_node("N1", JobKind::Always);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Output);
        let edges = vec![("N1", "N0"), ("N3", "N1"), ("N3", "N2")];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    ro.setup_graph = Box::new(create_graph2);

    let fails = ["N0"];
    let g = ro
        .run(&fails)
        .map_err(|x| {
            dbg!(&x);
            x
        })
        .unwrap();
}

#[test]
fn test_fuzz_6() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        //g.add_node("N0", JobKind::Always);
        g.add_node("N1", JobKind::Ephemeral);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Ephemeral);
        g.add_node("N4", JobKind::Output);
        let edges = vec![("N3", "N0"), ("N2", "N1"), ("N4", "N2"), ("N4", "N3")];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Always);
        g.add_node("N1", JobKind::Ephemeral);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Ephemeral);
        g.add_node("N4", JobKind::Output);
        let edges = vec![("N3", "N0"), ("N2", "N1"), ("N4", "N2"), ("N4", "N3")];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }
    ro.setup_graph = Box::new(create_graph2);
    let g = ro.run(&["N0"]).unwrap();
}

#[test]
fn test_fuzz_7() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Always);
        g.add_node("N1", JobKind::Ephemeral);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Ephemeral);
        g.add_node("N4", JobKind::Output);
        g.add_node("N5", JobKind::Always);
        g.add_node("N6", JobKind::Output);
        let edges = vec![
            ("N1", "N0"),
            ("N2", "N0"),
            ("N3", "N0"),
            ("N2", "N1"),
            ("N3", "N1"),
            ("N5", "N1"),
            ("N6", "N1"),
            ("N3", "N2"),
            ("N4", "N2"),
            ("N6", "N2"),
            ("N4", "N3"),
            ("N5", "N3"),
            ("N6", "N4"),
            ("N6", "N5"),
        ];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }

    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    let g = ro.run(&[]).unwrap();
}

#[test]
fn test_fuzz_8() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Ephemeral);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Ephemeral);
        g.add_node("N4", JobKind::Ephemeral);
        g.add_node("N5", JobKind::Output);
        let edges = vec![
            ("N2", "N0"),
            ("N5", "N0"),
            ("N3", "N1"),
            ("N4", "N2"),
            ("N5", "N2"),
            ("N4", "N3"),
            ("N5", "N3"),
            ("N5", "N4"),
        ];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Ephemeral);
        g.add_node("N1", JobKind::Ephemeral);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Ephemeral);
        g.add_node("N4", JobKind::Ephemeral);
        g.add_node("N5", JobKind::Output);
        let edges = vec![
            ("N2", "N0"),
            ("N5", "N0"),
            ("N3", "N1"),
            ("N4", "N2"),
            ("N5", "N2"),
            ("N4", "N3"),
            ("N5", "N3"),
            ("N5", "N4"),
        ];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }
    ro.setup_graph = Box::new(create_graph2);
    let g = ro.run(&["N1"]).unwrap();
}

#[test]
fn test_fuzz_9() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Ephemeral);
        g.add_node("N1", JobKind::Output);
        g.add_node("N2", JobKind::Output);
        g.add_node("N3", JobKind::Ephemeral);
        g.add_node("N4", JobKind::Output);
        //g.add_node("N5", JobKind::Output);
        let edges = vec![
            ("N1", "N0"),
            ("N4", "N0"),
            ("N2", "N1"),
            ("N4", "N1"),
            ("N5", "N1"),
            ("N5", "N2"),
            ("N4", "N3"),
            ("N5", "N4"),
        ];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }

    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();

    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Ephemeral);
        g.add_node("N1", JobKind::Output);
        g.add_node("N2", JobKind::Output);
        g.add_node("N3", JobKind::Ephemeral);
        g.add_node("N4", JobKind::Output);
        g.add_node("N5", JobKind::Output);
        let edges = vec![
            ("N1", "N0"),
            ("N4", "N0"),
            ("N2", "N1"),
            ("N4", "N1"),
            ("N5", "N1"),
            ("N5", "N2"),
            ("N4", "N3"),
            ("N5", "N4"),
        ];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }

    ro.setup_graph = Box::new(create_graph2);
    let g = ro.run(&["N5"]).unwrap();
}

#[test]
fn test_fuzz_10() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Ephemeral);
        //g.add_node("N1", JobKind::Output);
        g.add_node("N2", JobKind::Always);
        g.add_node("N3", JobKind::Output);
        g.add_node("N4", JobKind::Output);
        g.add_node("N5", JobKind::Ephemeral);
        g.add_node("N6", JobKind::Output);
        let edges = vec![
            ("N3", "N0"),
            ("N6", "N0"),
            ("N2", "N1"),
            ("N3", "N2"),
            ("N4", "N3"),
            ("N5", "N4"),
            ("N6", "N5"),
        ];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }

    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Ephemeral);
        g.add_node("N1", JobKind::Output);
        g.add_node("N2", JobKind::Always);
        g.add_node("N3", JobKind::Output);
        g.add_node("N4", JobKind::Output);
        g.add_node("N5", JobKind::Ephemeral);
        g.add_node("N6", JobKind::Output);
        let edges = vec![
            ("N3", "N0"),
            ("N6", "N0"),
            ("N2", "N1"),
            ("N3", "N2"),
            ("N4", "N3"),
            ("N5", "N4"),
            ("N6", "N5"),
        ];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }

    ro.setup_graph = Box::new(create_graph2);
    let g = ro.run(&["N1"]).unwrap();
}

#[test]
fn test_fuzz_11() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        //g.add_node("N0", JobKind::Always);
        g.add_node("N1", JobKind::Always);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Output);
        g.add_node("N4", JobKind::Always);
        g.add_node("N5", JobKind::Always);
        g.add_node("N6", JobKind::Output);
        let edges = vec![
            ("N1", "N0"),
            ("N3", "N1"),
            ("N3", "N2"),
            ("N6", "N2"),
            ("N4", "N3"),
            ("N5", "N4"),
            ("N6", "N5"),
        ];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }

    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    fn create_graph2(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Always);
        g.add_node("N1", JobKind::Always);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Output);
        g.add_node("N4", JobKind::Always);
        g.add_node("N5", JobKind::Always);
        g.add_node("N6", JobKind::Output);
        let edges = vec![
            ("N1", "N0"),
            ("N3", "N1"),
            ("N3", "N2"),
            ("N6", "N2"),
            ("N4", "N3"),
            ("N5", "N4"),
            ("N6", "N5"),
        ];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }

    ro.setup_graph = Box::new(create_graph2);
    let g = ro.run(&["N0"]).unwrap();
}
#[test]
fn test_aborting_inbetween_jobs() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N1", JobKind::Output);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Output);
        g.depends_on("N2", "N1");
        g.depends_on("N3", "N2");
    }
    let strat = StrategyForTesting::new();
    let mut g = PPGEvaluator::new(strat);
    let strat = StrategyForTesting::new();
    create_graph(&mut g);
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N1"]);
    g.event_now_running("N1").unwrap();
    assert!(g.query_ready_to_run().is_empty());
    g.event_job_finished_success("N1", "out1output".to_string())
        .unwrap();
    strat.already_done.borrow_mut().insert("N1".to_string());
    assert_eq!(g.query_ready_to_run(), set!["N2"]);
    assert!(!g.is_finished());
    g.event_now_running("N2").unwrap();
    assert!(!g.is_finished());
    g.event_job_finished_success("N2", "out2output".to_string())
        .unwrap();
    strat.already_done.borrow_mut().insert("N2".to_string());
    assert_eq!(g.query_ready_to_run(), set!["N3"]);
    g.event_now_running("N3").unwrap();
    assert!(!g.is_finished());
    g.event_job_finished_success("N3", "out3output".to_string())
        .unwrap();
    assert!(g.is_finished());
    strat.already_done.borrow_mut().insert("N3".to_string());
    let history = g.new_history().unwrap();
    assert!(history.get(&("N1!!!N2".to_string())).unwrap() == "out1output");
    assert!(history.get(&("N2!!!N3".to_string())).unwrap() == "out2output");
    assert!(history.len() == 2 + 3 + 3);

    let mut g = PPGEvaluator::new_with_history(history.clone(), strat);
    create_graph(&mut g);
    g.add_node("A", JobKind::Always);
    g.add_node("B", JobKind::Output);
    g.depends_on("N1", "A");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["A", "B"]);
    g.event_now_running("A").unwrap();
    assert_eq!(g.query_ready_to_run(), set!["B"]);
    g.event_job_finished_success("A", "outA".to_string())
        .unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N1", "B"]);
    g.event_now_running("N1").unwrap();
    g.event_job_finished_success("N1", "out1output_changed".to_string())
        .unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N2", "B"]);
    g.event_now_running("N2").unwrap();
    g.event_job_finished_failure("N2").unwrap();
    assert_eq!(g.query_ready_to_run(), set!["B"]);
    g.abort_remaining().unwrap();

    let history2 = g.new_history().unwrap();
}
#[test]
fn test_aborting_while_ephemeral_is_running() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N1", JobKind::Output);
        g.add_node("N2", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Output);
        g.depends_on("N2", "N1");
        g.depends_on("N3", "N2");
    }
    let strat = StrategyForTesting::new();
    let mut g = PPGEvaluator::new(strat.clone());
    create_graph(&mut g);
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N1"]);
    g.event_now_running("N1").unwrap();
    assert!(g.query_ready_to_run().is_empty());
    g.event_job_finished_success("N1", "out1output".to_string())
        .unwrap();
    strat.already_done.borrow_mut().insert("N1".to_string());
    assert_eq!(g.query_ready_to_run(), set!["N2"]);
    assert!(!g.is_finished());
    g.event_now_running("N2").unwrap();
    assert!(!g.is_finished());
    g.event_job_finished_success("N2", "out2output".to_string())
        .unwrap();
    strat.already_done.borrow_mut().insert("N2".to_string());
    assert_eq!(g.query_ready_to_run(), set!["N3"]);
    g.event_now_running("N3").unwrap();
    assert!(!g.is_finished());
    g.event_job_finished_success("N3", "out3output".to_string())
        .unwrap();
    strat.already_done.borrow_mut().insert("N3".to_string());
    assert!(g.is_finished());
    let history = g.new_history().unwrap();
    assert!(history.get(&("N1!!!N2".to_string())).unwrap() == "out1output");
    assert!(history.get(&("N2!!!N3".to_string())).unwrap() == "out2output");
    assert!(history.len() == 2 + 3 + 3);
    assert!(strat.already_done.borrow_mut().contains("N1"));

    let mut g = PPGEvaluator::new_with_history(history.clone(), strat.clone());
    create_graph(&mut g);
    g.add_node("A", JobKind::Always);
    g.add_node("B", JobKind::Output);
    g.depends_on("N1", "A");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["A", "B"]);
    g.event_now_running("A").unwrap();
    assert_eq!(g.query_ready_to_run(), set!["B"]);
    g.event_job_finished_success("A", "outA".to_string())
        .unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N1", "B"]);
    g.event_now_running("N1").unwrap();
    g.event_job_finished_success("N1", "out1output_changed".to_string())
        .unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N2", "B"]);
    g.event_now_running("N2").unwrap();
    g.event_job_finished_failure("N2").unwrap();
    assert_eq!(g.query_ready_to_run(), set!["B"]);

    g.abort_remaining().unwrap();

    let history2 = g.new_history().unwrap();

    assert!(history2.get(&("N1".to_string())).unwrap() == "out1output_changed");
    assert!(history2.get(&("N1!!!".to_string())).unwrap() == "A");
    assert!(history2.get(&("N1!!!N2".to_string())).unwrap() == "out1output"); //we did not filter
                                                                              //this.
    assert!(history2.get(&("N2!!!".to_string())).is_none());
    assert!(history2.get(&("N2".to_string())).is_none());

    let strat = StrategyForTesting::new();
    strat.already_done.borrow_mut().insert("N1".to_string());
    strat.already_done.borrow_mut().insert("N3".to_string());
    let mut g = PPGEvaluator::new_with_history(history2.clone(), strat);
    create_graph(&mut g);
    g.add_node("A", JobKind::Always);
    g.add_node("B", JobKind::Output);
    g.depends_on("N1", "A");
    g.depends_on("B", "N2");
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["A"]);
    g.event_now_running("A").unwrap();
    assert!(g.query_ready_to_run().is_empty());
    g.event_job_finished_success("A", "outA".to_string())
        .unwrap();

    assert_eq!(g.query_ready_to_run(), set!["N2"]);
    g.event_now_running("N2").unwrap();
    g.event_job_finished_success("N2", "out2output_changed".to_string())
        .unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N3", "B"]);
}

#[test]
fn test_aborting_between_ephemerals_1() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N1", JobKind::Ephemeral);
        g.add_node("N2a", JobKind::Ephemeral);
        g.add_node("N2b", JobKind::Ephemeral);
        g.add_node("N2c", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Output);
        g.depends_on("N2a", "N1");
        g.depends_on("N2b", "N1");
        g.depends_on("N2c", "N1");
        g.depends_on("N3", "N2a");
        g.depends_on("N3", "N2b");
        g.depends_on("N3", "N2c");
    }
    //so the idea is that everything had run. Then it was rerun, and N1 was rebuild
    // (because it was missing, or perhaps because it was invalidated),
    // and then N2a was run, but before N2b could be run (and update it's inputs), the abort
    // happend
    // so we have diverging history.
    // and we need a third job go the ephemeral to rebuild.
    // but we should not need a third job, the fact taht N2b is invalidated by the (stored) N1
    // output should already retrigger N1.
    // So We might Have Two Bugs: Not triggering N1 thouh N2b is invalidated
    // and then 'comparing with stored on "N1!!N2b" instead of "N2b"
    let strat = StrategyForTesting::new();
    strat.already_done.borrow_mut().insert("N1".to_string());
    strat.already_done.borrow_mut().insert("N3".to_string());
    let mut history = HashMap::new();
    history.insert("N1".to_string(), "1_changed".to_string());
    history.insert("N1!!!".to_string(), "".to_string());

    history.insert("N2a".to_string(), "2a".to_string());
    history.insert("N2a!!!".to_string(), "N1".to_string());
    history.insert("N1!!!N2a".to_string(), "1_changed".to_string());

    history.insert("N2b".to_string(), "2b".to_string());
    history.insert("N2b!!!".to_string(), "N1".to_string());
    history.insert("N1!!!N2b".to_string(), "1".to_string()); // that's the one that did not update.

    history.insert("N3".to_string(), "3".to_string());
    history.insert("N3!!!".to_string(), "N2a\nN2b".to_string());
    history.insert("N2a!!!N3".to_string(), "2a".to_string()); // that's the one that did not update.
    history.insert("N2b!!!N3".to_string(), "2b".to_string()); // that's the one that did not update.

    let mut g = PPGEvaluator::new_with_history(history, strat);
    start_logging();
    create_graph(&mut g);
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N1"]);
    g.event_now_running("N1").unwrap();
    g.event_job_finished_success("N1", "1_changed".to_string())
        .unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N2a", "N2b", "N2c"]); // N3 needs all three.
    g.event_now_running("N2a").unwrap();
    g.event_now_running("N2b").unwrap();
    g.event_now_running("N2c").unwrap();
    g.event_job_finished_success("N2a", "2a".to_string())
        .unwrap();
    g.event_job_finished_success("N2b", "2b".to_string())
        .unwrap();
    g.event_job_finished_success("N2c", "2c".to_string())
        .unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N3"]);
    g.event_now_running("N3").unwrap();
    g.event_job_finished_success("N3", "3".to_string()).unwrap();
    //
    assert!(g.query_ready_to_run().is_empty());
    assert!(g.is_finished());
}

#[test]
fn test_aborting_between_ephemerals_invalidation_triggers() {
    //same as above, but forcing us to actually have the N2b invalidation because N1 changed.
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N1", JobKind::Ephemeral);
        g.add_node("N2a", JobKind::Ephemeral);
        g.add_node("N2b", JobKind::Ephemeral);
        g.add_node("N3", JobKind::Output);
        g.depends_on("N2a", "N1");
        g.depends_on("N2b", "N1");
        g.depends_on("N3", "N2a");
        g.depends_on("N3", "N2b");
    }
    let strat = StrategyForTesting::new();
    strat.already_done.borrow_mut().insert("N1".to_string());
    strat.already_done.borrow_mut().insert("N3".to_string());
    let mut history = HashMap::new();
    history.insert("N1".to_string(), "1_changed".to_string());
    history.insert("N1!!!".to_string(), "".to_string());

    history.insert("N2a".to_string(), "2a".to_string());
    history.insert("N2a!!!".to_string(), "N1".to_string());
    history.insert("N1!!!N2a".to_string(), "1_changed".to_string());

    history.insert("N2b".to_string(), "2b".to_string());
    history.insert("N2b!!!".to_string(), "N1".to_string());
    history.insert("N1!!!N2b".to_string(), "1".to_string()); // that's the one that did not update.

    history.insert("N3".to_string(), "3".to_string());
    history.insert("N3!!!".to_string(), "N2a\nN2b".to_string());
    history.insert("N2a!!!N3".to_string(), "2a".to_string()); // that's the one that did not update.
    history.insert("N2b!!!N3".to_string(), "2b".to_string()); // that's the one that did not update.

    let mut g = PPGEvaluator::new_with_history(history, strat);
    create_graph(&mut g);
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N1"]);
    g.event_now_running("N1").unwrap();
    g.event_job_finished_success("N1", "1_changed".to_string())
        .unwrap();
    assert_eq!(g.query_ready_to_run(), set!["N2b"]);
    g.event_now_running("N2b").unwrap();
    g.event_job_finished_success("N2b", "2b".to_string())
        .unwrap();

    assert!(g.is_finished());
}

#[test]
fn test_invalidation_case_20231120() {
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A", JobKind::Output);
        g.add_node("B", JobKind::Ephemeral);
        g.add_node("C1", JobKind::Ephemeral);
        g.add_node("C2", JobKind::Always);
        g.depends_on("A", "B");
        g.depends_on("B", "C1");
        g.depends_on("B", "C2");
    }
    let strat = StrategyForTesting::new();
    strat.already_done.borrow_mut().insert("A".to_string());

    let mut history = HashMap::new();
    history.insert("C1".to_string(), "C1".to_string());
    history.insert("C2".to_string(), "C2".to_string());
    history.insert("B".to_string(), "B".to_string());
    history.insert("A".to_string(), "A".to_string());
    history.insert("C1!!!".to_string(), "".to_string());
    history.insert("C2!!!".to_string(), "".to_string());
    history.insert("B!!!".to_string(), "C1\nC2".to_string());
    history.insert("A!!!".to_string(), "B".to_string());
    history.insert("B!!!A".to_string(), "B".to_string());
    history.insert("C1!!!B".to_string(), "C1".to_string());
    history.insert("C2!!!B".to_string(), "C2".to_string());

    //start_logging();
    let mut g = PPGEvaluator::new_with_history(history, strat);
    create_graph(&mut g);
    g.event_startup().unwrap();
    assert_eq!(g.query_ready_to_run(), set!["C2"]);
    g.event_now_running("C2").unwrap();
    g.event_job_finished_success("C2", "C2_changed".to_string())
        .unwrap();
    assert_eq!(g.query_ready_to_run(), set!["C1"]);
    error!("start reading here");
    warn!("start reading here");
    g.event_now_running("C1").unwrap();
    assert!(!g.is_finished());
    g.event_job_finished_success("C1", "C1".to_string())
        .unwrap();

    assert_eq!(g.query_ready_to_run(), set!["B"]);
    g.event_now_running("B").unwrap();
    g.event_job_finished_success("B", "B".to_string()).unwrap();

    assert!(g.is_finished());
}

#[test]
fn test_fail_panic_after_20231120_fix() {
    //turned out to be downstream_requirement_status returning 'unknown' too eagerly.
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Ephemeral);
        g.add_node("N1", JobKind::Ephemeral);
        g.add_node("N2", JobKind::Always);
        g.add_node("N3", JobKind::Output);

        let edges = vec![("N1", "N0"), ("N2", "N0"), ("N3", "N1"), ("N3", "N2")];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    start_logging();
    let g = ro.run(&[]).unwrap();
}

#[test]
fn test_fail_panic_after_20231120_fix2() { //and another one.
    //bet it's another early return in downstream_requirement_status.
    //and indeed it was.
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("N0", JobKind::Ephemeral);
        g.add_node("N1", JobKind::Ephemeral);
        g.add_node("N2", JobKind::Always);
        g.add_node("N3", JobKind::Output);

        let edges = vec![("N1", "N0"), ("N2", "N0"), ("N3", "N0"), ("N3", "N2")];
        for (a, b) in edges {
            if g.contains_node(a) && g.contains_node(b) {
                g.depends_on(a, b);
            }
        }
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    start_logging();
    let g = ro.run(&[]).unwrap();
}
/*
#[test]
fn test_multi_file_job_gaining_output() {
    // the rust engine does not know anything about filenames or jobs with 'multiple'
    // outputs
    // we fake that by delegating to is_history_altered on the python side.
    // but adding/removing an output will retrigger the downstreams
    // because the job name changes and we track input job names.
    // now I suppose we could delegate this to python.
    // which has the 'filename-dependncy-graph' to actually make the decision, right?,
    // I already have that test    todo!()
    fn create_graph(g: &mut PPGEvaluator<StrategyForTesting>) {
        g.add_node("A::B", JobKind::Output);
        g.add_node("C", JobKind::Output);
        g.depends_on("C", "A::B");
    }
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    let g = ro.run(&[]).unwrap();
    assert!(ro.run_counters.get("A::B") == Some(&1));
    let g = ro.run(&[]).unwrap();
    assert!(ro.run_counters.get("B") == Some(&1));
    ro.history.remove("B");
    let g = ro.run(&[]).unwrap();
    assert!(ro.run_counters.get("B") == Some(&2));

}
*/
