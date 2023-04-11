use pypipegraph2::{
    start_logging_to_file, JobKind, PPGEvaluator, StrategyForTesting, TestGraphRunner,
};
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
    println!("first run");
    let _g = ro.run(&[]).unwrap();
    start_logging_to_file("debug.log");
    println!("second run");
    let _g = ro.run(&[]).unwrap();
    println!("done");
}

fn main() {
    println!("running big graph");
    test_fuzz_3()
    //    pypipegraph2::test_big_graph_in_layers(300, 30, 2);
}
