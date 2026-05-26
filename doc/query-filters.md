### Query Filters

Filters are used to exclude rows from the selected columns. All filters are **conjunctive** by default – each additional
filter appends an `AND` condition. Multiple filters on the same column are composed together. Segment pruning is applied
during [Phase 2][1] of the read-cycle if the filter type maps to a segment-level statistic stored in the manifest. Row
filtering is applied during [Phase 3][2] for predicates that cannot be fully resolved from manifest statistics alone.

##### Select

A query returns all columns defined by the schema unless otherwise specified. The `.select` method restricts the
returned columns to a named subset, reducing file IO to only the required buffers.

```rust
.select(["a", "b"]) // Return only columns "a" and "b"
```

Columns omitted from `select` are never read from disk. This is the primary mechanism to reduce file IO on wide schemas.
Omitting `select` is equivalent to selecting every column.

##### Range

Retain rows where the value in the specified column falls within a specified interval `[min, max]`. Directly exploits
the `min` and `max` buffer statistics for segment pruning.

```rust
.range("temperature", 10..20) // 10.0 ≤ temperature < 20.0 inclusive range
.range("altitude", 100..=500) // inclusive upper bound on additonal column
```

Open or half-open ranges are also supported:

```rust
.range("pressure", 101.3..) // pressure ≥ 101.3  (no upper bound)
.range("pressure", ..105.0) // pressure < 105.0  (no lower bound)
```

##### Equality

Retain rows where the value in the specified column exactly equals a given value. Useful for boolean flags, integer
codes, and enum discriminants.

```rust
.eq("active", true)
.eq("sensor_id", 42u32)
```

Equality on an orderable type is equivalent to `.range(col, v..=v)` and benefits from segment pruning.

##### Option

Retain or reject optional rows that contain `Some` or `None`. Exploits the null bitmap or *in situ* option encoding for
each buffer. Returns an error if the column type is not optional.

```rust
.is_some("calibration") // row must have a calibration value
.is_none("error_code")  // row must have no error code
```

##### Set Membership

Retain or reject rows where the column value is a member of a finite set. Useful for allowlists, category codes, or
string tags. Orderable types benefit from segment pruning; skipped if `buffer.max < set.min || buffer.min > set.max`.

```rust
.one_of("sensor_id", [1u32, 4, 7, 12])
.none_of("status_code", [404u16, 500])
```

##### Mask

Retain rows by position using a boolean or optional column. Applies the named column as a bitmask; only rows where the
mask column is `true` or `some` are returned.

```rust
.mask("is_valid") // equivalent to .eq("is_valid", true) with cross-column semantics
```

##### Limit and Offset

Restrict the number of rows returned without a value-based predicate. Segment pruning uses `buffer.count` to skip
segments that fall entirely outside the requested window. Limit and offset filters are applied to the result set after
all other conditions have been evaluated.

```rust
.limit(1000) // return at most 1000 rows
.offset(500) // skip the first 499 matching rows
.limit(1000).offset(500) // rows 500..1500
```

##### Stride

Sample every nth row from the result set. Useful for decimation and preview reads on dense time-series data.

```rust
.stride(10) // return every 10th row
```

##### Execution and Output

`.read().await` executes the query and returns a lazy async zero-copy batched reader. Each call to `.next().await`
yields one deserialized row; no row is deserialized before being requested. The reader chains across segments
transparently, meaning callers observe a flat sequence regardless of the underlying segment structure.

```rust
let mut cursor = dataset
.query("schema_name")
.select(["latitude", "longitude"])
.range("altitude", 0.0..=1000.0)
.read()
.await?;
while let Some(row) = cursor.next().await { process(row?); }
```

A `.collect().await` convenience method collects the full result into an owned `Vec` for callers that require random
access over the result set.

```rust
let result: Vec<R> = dataset
.query("schema_name")
.eq("active", true)
.collect()
.await?;
```

##### Evaluation Order

Filters are evaluated in two stages to minimise IO:

| Stage | Scope   | Uses Manifest | Description                                             |
|-------|---------|---------------|---------------------------------------------------------|
| One   | Segment | Yes           | Discard entire segments using manifest statistics.      |
| Two   | Row     | No            | Evaluate remaining predicates row-by-row during decode. |

Filters that can be fully satisfied by manifest statistics never cause unnecessary file IO. Filters that require
individual row values are combined and lazily evaluated during iteration; each `.next().await` applies stage-two filters
until the next matching row is found or the result set is exhausted. This design minimises file IO by ensuring a single
pass across the candidate segments.

##### Filter Summary

| Method                | Segment pruning | Row-level filter | Notes                                   |
|-----------------------|-----------------|------------------|-----------------------------------------|
| `.select([cols])`     | ✓ column IO     | —                | Skips unselected buffer reads entirely. |
| `.range(col, lo..hi)` | ✓ `min` `max`   | ✓                | Core predicate; composable.             |
| `.eq(col, val)`       | ✓ `min` `max`   | ✓                | Equivalent to `range(col, val..=val)`.  |
| `.is_some(col)`       | —               | ✓                | Requires buffer read.                   |
| `.is_none(col)`       | —               | ✓                | Requires buffer read.                   |
| `.one_of(col, set)`   | ✓ `min` `max`   | ✓                | Prunes when set is disjoint from range. |
| `.none_of(col, set)`  | ✓ `min` `max`   | ✓                |                                         |
| `.mask(col)`          | —               | ✓                | Cross-column boolean filter.            |
| `.limit(n)`           | ✓ `count`       | ✓                | Stops iteration once `n` rows yielded.  |
| `.offset(n)`          | ✓ `count`       | ✓                | Skips segments wholly before offset.    |
| `.stride(n)`          | —               | ✓                | Decimation; no segment-level skip.      |

[1]: read-cycle.md#phase-2-segment-pruning
[2]: read-cycle.md#phase-3-lazy-async-zero-copy-batched-reads