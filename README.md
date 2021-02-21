# bgutil-rs

## Build

Don't forget to download & install [cassandra-cpp](https://downloads.datastax.com/cpp-driver/centos/8/cassandra/v2.15.3/) & [libuv](https://downloads.datastax.com/cpp-driver/centos/8/dependencies/libuv/v1.35.0/).

## Run

```sh
$ cargo build
    Finished dev [unoptimized + debuginfo] target(s) in 0.04s

$ cargo run -- --help
bgutil-rs 

USAGE:
    bgutil-rs <SUBCOMMAND>

SUBCOMMANDS:
    delete    Delete metric(s)
    help      Prints this message or the help of the given subcommand(s)
    info      Information about a metric
    list      List metrics with given pattern
    read      Read a metric contents
    write     Write a metric and its value
```

### Info

```sh
$ cargo run -- info --help
bgutil-rs-info 
Information about a metric

USAGE:
    bgutil-rs info <metric>

ARGS:
    <metric>    the metric
```

Example:

```sh
$ cargo run -- info observability.testaroo.up
observability.testaroo.up {"aggregator": "average", "retention": "11520*60s:720*3600s:730*86400s", "carbon_xfilesfactor": "0.500000"}
```

### Read

```sh
bgutil-rs-read
Read a metric contents

USAGE:
    bgutil-rs read [OPTIONS] <metric>

OPTIONS:
        --stage <stage>
        --time-end <time-end>
        --time-start <time-start>

ARGS:
    <metric>    metric to get values
```

Example:

```sh
$ cargo run -- read observability.testaroo.up --stage "11520*60s" --time-start 1613257200 --time-end 1613343600
1613319120;0.0
1613319180;0.0
1613319240;1.0
1613319300;1.0
1613319360;1.0
1613319420;1.0
1613319480;1.0
1613319540;1.0
...
```

### List

```sh
$ cargo run -- list --help
bgutil-rs-list
List metrics with given pattern

USAGE:
    bgutil-rs list <glob>

ARGS:
    <glob>
```

Example:

```sh
$ cargo run -- list observability.*.up
d observability.testaroo.up
m observability.testaroo.up {"retention": "11520*60s:720*3600s:730*86400s", "aggregator": "average", "carbon_xfilesfactor": "0.500000"}

$ time cargo run -- list observability.test*.go*
d observability.testaroo.go_memstats_next_gc_bytes
d observability.testaroo.go_memstats_mallocs_total
...
m observability.testaroo.go_memstats_next_gc_bytes {"aggregator": "average", "carbon_xfilesfactor": "0.500000", "retention": "11520*60s:720*3600s:730*86400s"}
m observability.testaroo.go_memstats_mallocs_total {"aggregator": "average", "retention": "11520*60s:720*3600s:730*86400s", "carbon_xfilesfactor": "0.500000"}
...
```


### Write

```sh
$ cargo run -- write --help
bgutil-rs-write
Write a metric and its value

USAGE:
    bgutil-rs write [OPTIONS] <metric> <value>

OPTIONS:
        --retention <retention>
    -t, --timestamp <timestamp>

ARGS:
    <metric>
    <value>
```

### Delete

```sh
$ cargo run -- delete --help
bgutil-rs-delete
Delete metric(s)

USAGE:
    bgutil-rs delete [FLAGS] <metric>

FLAGS:
        --recursive

ARGS:
    <metric>
```

## Todo

* command: read
  - human timestamps (but unix timestamps are ok)
* command: list
  - Missing pattern matching like {}, **
* command: write
  - Arguments handling
* command: delete
  - with recursive

```
usage: bgutil delete [--help] [-r] [--dry-run] path

positional arguments:
  path             One metric or subdirectory name

optional arguments:
  --help           show this help message and exit
  -r, --recursive  Delete points for all metrics as a subtree
  --dry-run        Only show commands to create/upgrade the schema.
```

* command: copy

```
usage: bgutil copy [--help] [-r] [--time-start TIME_START]
                   [--time-end TIME_END] [--dry-run]
                   [--src_retention SRC_RETENTION]
                   [--dst_retention DST_RETENTION]
                   src dst

positional arguments:
  src                   One source metric or subdirectory name
  dst                   One destination metric or subdirectory name

optional arguments:
  --help                show this help message and exit
  -r, --recursive       Copy points for all metrics as a subtree
  --time-start TIME_START
                        Copy points written later than this time.
  --time-end TIME_END   Copy points written earlier than this time.
  --dry-run             Only show commands to create/upgrade the schema.
  --src_retention SRC_RETENTION
                        Retention used to read points from the source metrics.
  --dst_retention DST_RETENTION
                        Retention used to write points to the destination
                        metrics. It only works if retentions are similar, i.e.
                        with same precisions.
```

* command: clean

```
usage: bgutil clean [--help] [--clean-cache] [--clean-backend]
                    [--clean-corrupted] [--quiet] [--max-age MAX_AGE]
                    [--start-key START_KEY] [--end-key END_KEY]
                    [--shard SHARD] [--nshards NSHARDS]
                    [--disable-clean-directories] [--disable-clean-metrics]

optional arguments:
  --help                show this help message and exit
  --clean-cache         clean cache
  --clean-backend       clean backend
  --clean-corrupted     clean corrupted metrics
  --quiet               Show no output unless there are problems.
  --max-age MAX_AGE     Specify the age of metrics in seconds to evict (ie:
                        3600 to delete older than one hour metrics)
  --start-key START_KEY
                        Start key.
  --end-key END_KEY     End key.
  --shard SHARD         Shard number.
  --nshards NSHARDS     Number of shards.
  --disable-clean-directories
                        Disable cleaning directories
  --disable-clean-metrics
                        Disable cleaning outdated metrics

```

* command: repair

```
usage: bgutil repair [--help] [--start-key START_KEY] [--end-key END_KEY]
                     [--shard SHARD] [--nshards NSHARDS] [--quiet]

optional arguments:
  --help                show this help message and exit
  --start-key START_KEY
                        Start key.
  --end-key END_KEY     End key.
  --shard SHARD         Shard number.
  --nshards NSHARDS     Number of shards.
  --quiet               Show no output unless there are problems.
```

* command: write

```
usage: bgutil write [--help] [-t TIMESTAMP] [-c COUNT]
                    [--aggregator AGGREGATOR] [--retention RETENTION]
                    [--x-files-factor X_FILES_FACTOR]
                    metric value

positional arguments:
  metric                Name of the metric to update.
  value                 Value to write at the select time.

optional arguments:
  --help                show this help message and exit
  -t TIMESTAMP, --timestamp TIMESTAMP
                        Timestamp at which to write the new point.
  -c COUNT, --count COUNT
                        Count associated with the value to be written.
  --aggregator AGGREGATOR
                        Aggregator function for the metric (average, last,
                        max, min, sum).
  --retention RETENTION
                        Retention configuration for the metric.
  --x-files-factor X_FILES_FACTOR
                        Science fiction coefficient.
```

* command: test