# clem

**A high-throughput storage engine for multidimensional analytical data, written in Rust.**

[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)

`clem` optimises read and write performance independently by separating the data lifecycle into two phases:

1. **In-memory** accumulator for high-throughput ingestion.
2. **On-disk** columnar buffers for analytical queries across a arbitrary number of dimensions.

The result is a single, self-describing, portable file that ingests data quickly and answers analytical queries with
minimal IO overhead. The format is intended as an extensible backend that can be adapted to suit a variety of scientific
applications. Implementers can enhance the minimal high-performance core library with domain-specific optimisations.

### Citation

Please cite `clem` in your academic work using the provided [citation](CITATION.cff) metadata.

### Motivation and Design Goals

// TODO → add a list of design goals here

To achieve these design goals, clem decouples **logical structure** (types and schemas) from **physical storage**
(segments). The [on-disk-format.md](./doc/on-disk-format.md) document shows how each goal is met.

### When to use clem

`clem` is a strong fit for any workload that writes once and queries many times. The rapid ingestion append-only design
is ideally suited to high-throughput sensor streams, experimental runs, telemetry, and time-series data. Clem is **not**
a transactional database and deliberately omits support for in-situ mutation, deletions, or ad-hoc SQL queries.

The table below compares clem against several widely used alternatives:

| Capability                            | Clem | Parquet | Arrow IPC | HDF5 | SQLite |
|---------------------------------------|:----:|:-------:|:---------:|:----:|:------:|
| Columnar storage                      |  ✓   |    ✓    |     ✓     |  ~   |   ✗    |
| Zero-copy memory-mapped reads         |  ✓   |    ✗    |     ✓     |  ~   |   ✗    |
| Predicate pushdown (min/max/count)    |  ✓   |    ✓    |     ✗     |  ✗   |   ✓    |
| Storage niche optimisation            |  ✓   |    ✗    |     ✗     |  ✗   |   ✗    |
| Multi-schema single-file (multimodal) |  ✓   |    ✗    |     ✗     |  ✓   |   ✓    |
| Block compression                     |  ✗   |    ✓    |     ~     |  ✓   |   ~    |
| Crash-safe atomic commits             |  ✓   |    ~    |     ~     |  ✗   |   ✓    |
| Lock-free concurrent readers          |  ✓   |    ✓    |     ✓     |  ~   |   ~    |
| Deletion and in-situ mutation         |  ✗   |    ✗    |     ✗     |  ✓   |   ✓    |

<sub>✓ native · ~ partial or via tooling · ✗ not supported</sub>

`clem` understands **platform-agnostic** primitive types such as `u32` or `f64`. Platform-dependent types such as
`usize` are deliberately omitted to ensure file portability. External user-defined types are mapped directly to a
generated schema using the provided `#[derive(Data)]` macro.

### How to use clem

Add `clem` to your `Cargo.toml`:

```toml
[dependencies]
clem = "0.1"
```

### Crate Features

### License

Licensed under the [BSD 3-Clause Licence](LICENSE). Copyright © 2026 Amelia Fraser-Dale.
