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
    help    Prints this message or the help of the given subcommand(s)
    info    Information about a metric
    list    List metrics with given pattern
    read    Read a metric contents
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
```

## Todo

* command: read
  - async
  - human timestamps
* command: list
  - Enhance pattern matching (with '{}', 'xxx*' or '*xxx'...)
* command: clean
