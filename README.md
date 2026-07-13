# Multimodal Segmented Compact Archive (MSCA)

**A high-throughput storage engine for multidimensional analytical data, written in Rust.**

[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)

`MSCA` optimises read and write performance independently by separating the data lifecycle into two phases:

1. **In-memory** accumulator for high-throughput ingestion.
2. **On-disk** columnar buffers for analytical queries across an arbitrary number of dimensions.

The result is a single, self-describing, portable file that ingests data quickly and answers analytical queries with
minimal IO overhead. The format is intended as an extensible backend that can be adapted to suit a variety of scientific
applications. Implementers can enhance the minimal performance-focused core library with their own domain-specific
optimisations.

### Citation

Please cite `msca` in your academic work using the provided [citation](CITATION.cff) metadata.

### Motivation and Design Goals

`MSCA` is designed to address the shortcomings of existing storage engines for edge deployment in scientific and
research applications where data integrity and high throughput are critical. Development is guided by a focused set of
design goals:

- **Compact** file size with interleaved segments.
- **Performant** ingestion and queries with minimal IO.
- **Adaptable** to a wide variety of applications.
- **Durable** against crashes and data loss.
- **Minimal** memory footprint and runtime overhead.

The self-describing deterministic on-disk layout ensures portability across platforms and architectures. Multiple
schemas can coexist in a single multimodal file, enabling simple collaboration and sharing of heterogeneous data.
No background server is required.

To achieve these goals, `msca` decouples **logical structure** (types and schemas) from **physical storage** (segments).
The [on-disk-format.md](./doc/on-disk-format.md) document shows how each goal is met.

### When to use MSCA

`MSCA` is a strong fit for any workload that writes once and queries many times. The rapid ingestion append-only design
is ideally suited to high-throughput sensor streams, experimental runs, telemetry, and time-series data. Clem is **not**
a transactional database and deliberately omits support for in-situ mutation, deletions, or ad-hoc SQL queries.

The table below compares `msca` against several widely used alternatives:

| Capability                            | MSCA | Parquet | Arrow IPC | HDF5 | SQLite |
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

`MSCA` understands **platform-agnostic** primitive types such as `u32` or `f64`. Platform-dependent types such as
`usize` are deliberately omitted to ensure file portability. External user-defined types are mapped directly to a
generated schema using the provided `#[derive(Data)]` macro.

### How to use MSCA

Add `msca` to your `Cargo.toml`:

```toml
[dependencies]
msca = "1.0"
```

A minimal end-to-end example — derive the traits, accumulate rows, commit a segment, then query it:

```rust
use msca::{Accumulate, Data, Dataset, Read};

#[derive(Data, Read)]
struct Reading {
    sensor: u32,
    temperature: f64,
}

let mut dataset = Dataset::new("readings.msca").await?;

// Register the schema once and accumulate rows in memory.
let mut readings = dataset.schema::<Reading>("readings").await?;
readings.push(Reading { sensor: 1, temperature: 21.5 });
readings.push(Reading { sensor: 2, temperature: 18.0 });

// Commit one immutable data segment.
dataset.write(readings).await?;

// Query with segment pruning and lazy zero-copy reads.
for row in dataset.query("readings") ?.range("temperature", 20.0..) ?.read::<Reading>() ? {
let row = row ?;
// ... process each row
}
```

See the [user guide](./doc/user-guide.md) for a full walkthrough covering schema registration, multithreaded
accumulation, and the complete query and filter vocabulary.

### Crate Features

`MSCA` ships a minimal default surface; additional capabilities are opt-in via Cargo features.

| Feature    | Default | Description                                                            |
|------------|:-------:|------------------------------------------------------------------------|
| `derive`   |   on    | `#[derive(Data)]` and `#[derive(Read)]` macros for external types.     |
| `serde`    |   off   | `serde` support for exported types                                     |
| `metadata` |   off   | Read and write surface for the optional free-form file-level metadata. |
| `log`      |   off   | Structured logging across the read and write paths; requires a logger. |

> [!WARNING] 🚧 Under Development
> Several crate features are still under active development. Feautre APIs may change before the 1.0 release.

### License

Licensed under the [BSD 3-Clause Licence](LICENSE). Copyright © 2026 Amelia Fraser-Dale.
