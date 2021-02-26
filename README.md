# bgutil-rs

bgutil-rs is a rewrite of biggraphite's bgutil tool.

You might find more information on [wiki](https://git.mkz.me/mycroft/bgutil-rs/wiki).

## Build

Don't forget to download & install [cassandra-cpp](https://downloads.datastax.com/cpp-driver/centos/8/cassandra/v2.15.3/) & [libuv](https://downloads.datastax.com/cpp-driver/centos/8/dependencies/libuv/v1.35.0/).

## Run

```sh
bgutil-rs 

USAGE:
    bgutil-rs [FLAGS] [OPTIONS] <SUBCOMMAND>

FLAGS:
        --dry-run    Do not write in database (local-clean only)
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --contact-metadata <contact-metadata>
             [env: CASSANDRA_CONTACT_METADATA=localhost]

        --contact-points <contact-points>
             [env: CASSANDRA_CONTACT_POINTS=localhost]


SUBCOMMANDS:
    clean          Clean outdated metrics & empty directories
    delete         Delete metric(s)
    help           Prints this message or the help of the given subcommand(s)
    info           Information about a metric
    list           List metrics with given pattern
    local-clean    Clean a directory of outdated metrics & empty sub-directories
    read           Read a metric contents
    stats          Stats
    write          Write a metric and its value

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

### Clean

```sh
$ cargo run -- clean --help
bgutil-rs-clean
Stats

USAGE:
    bgutil-rs clean [FLAGS] [OPTIONS]

FLAGS:
        --clean-directories
        --clean-metrics

OPTIONS:
        --end-key <end-key>
        --start-key <start-key>
```

### Local-clean

Clean outdated metrics in a given directory.

```sh
$ cargo run -- local-clean --help
bgutil-rs-local-clean 
Clean a directory of outdated metrics & empty sub-directories

USAGE:
    bgutil-rs local-clean <directory>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

ARGS:
    <directory>
```


## Todo

* command: read
  - human timestamps (but unix timestamps are ok)
* command: list
  - Missing pattern matching like {abc,def}, **, [0-99]
* command: write
  - Arguments handling
* command: delete
  - with recursive
* command: clean
  - progress bar
* ...


## Dedication

This piece of software was written during the mourning of Jean-Yves Moyart, aka Maître Mô, 21/10/1967-20/02/2021. My thoughts were with him, his family, his friends and all of us, who really appreciated him. Rest in Peace.
