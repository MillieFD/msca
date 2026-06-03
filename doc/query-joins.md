### Query Across Schemas

A single `clem` file may contain multiple schemas. Each query builder is spawned from a single schema. Users can `join`
two existing `Query` instances to operate across multiple schemas. The join strategy is selected automatically based on
`count` statistics. Both legs are independent and support the full filter and projection vocabulary. The specified
columns are matched by value and must share a compatible type.

##### Join

Retain only rows where the key is present in both legs.

```rust,ignore
let readings = dataset.query("readings").select(["time", "sensor", "value"]).range("value", 10.0..=20.0);
let sensors = dataset.query("sensors").select(["id", "location"]);
let result = readings.join(sensors, "sensor", "id").read().await?
```

##### Semi-Join

Retain rows from the left leg if the key is also present in the right leg, but do not include any right-leg columns in
the output. Useful for existence filtering without column inflation.

```rust,ignore
.semi_join(
dataset.query("active_sensors").eq("online", true),
"sensor_id",
"sensor_id",
)
```

##### Anti-Join

The complement of semi-join. Retain rows from the left leg whose key does *not* appear in the right leg, but do not
include any right-leg columns in the output.

```rust,ignore
.anti_join(
dataset.query("faulty_sensors"),
"sensor_id",
"sensor_id",
)
```

##### Join Composition

The query builder is deliberately composable. Each `join` returns a new `Query` instance that can itself be further
filtered or joined.

```rust,ignore
let result = dataset
.query("measurements")
.range("time", t0..t1)
.join(
dataset.query("sensors").eq("online", true),
"sensor_id",
"sensor_id",
)
.join(
dataset.query("locations_dictionary").select(["id", "region", "timezone"]),
"sensor_id",
"id",
)
.select(["time", "value", "region"])
.read()
.await?
```

Filters after a join apply to the combined output. Filters before a join apply only to the calling `Query` instance.
The join strategy is selected automatically based on `count` statistics:

| Size                     | Strategy                       |
|--------------------------|--------------------------------|
| Right leg fits in memory | Hash join                      |
| Both legs are large      | Disk-based external sort joins |