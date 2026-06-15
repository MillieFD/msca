# clem

**A high-throughput storage engine for multidimensional analytical data, written in Rust.**

[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)

`clem` optimises read and write performance independently by separating the data lifecycle into two phases:

1. **In-memory** accumulator for high-throughput ingestion.
2. **On-disk** columnar buffers for range-based querying across arbitrary dimensions.

The result is a single, self-describing, portable file that ingests data quickly and answers analytical queries with
minimal IO overhead. The format is intended as an extensible backend that can be adapted to suit a variety of scientific
applications. Implementers can enhance the minimal high-performance core library with domain-specific optimisations.

### Citation

Please cite `clem` in your academic work using the provided [`citation`](CITATION.cff) metadata.

### License

Licensed under the [BSD 3-Clause Licence](LICENSE). Copyright © 2026 Amelia Fraser-Dale.
