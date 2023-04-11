use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pypipegraph2::{
    test_big_graph_in_layers, test_big_linear_graph, test_big_linear_graph_half_ephemeral,
};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("linear_graph", |b| {
        b.iter(|| test_big_linear_graph(black_box(1000)))
    });
    c.bench_function("linear_graph_half_ephmerela", |b| {
        b.iter(|| test_big_linear_graph_half_ephemeral(black_box(1000)))
    });
    c.bench_function("test_big_graph_in_layers", |b| {
        b.iter(|| test_big_graph_in_layers(black_box(10), black_box(10), 1))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
