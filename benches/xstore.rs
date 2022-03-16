use criterion::{criterion_group, criterion_main, Criterion};

use cid::multihash::{Code, MultihashDigest};
use cid::Cid;

pub fn xstore_benchmark(c: &mut Criterion) {
    c.bench_function("has", |b| {
        let bs = xstore::store::Blockstore::new_memory();
        let block = b"thing";
        let key = Cid::new_v1(0x55, Code::Sha2_256.digest(block));

        b.iter(|| bs.has(&key).unwrap())
    });
}

criterion_group!(benches, xstore_benchmark);
criterion_main!(benches);
