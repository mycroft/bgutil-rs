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

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    help    Prints this message or the help of the given subcommand(s)
    info    Information about a metric
```

### Info

```sh
$ cargo run -- info --help
bgutil-rs-info 
Information about a metric

USAGE:
    bgutil-rs info <metric>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

ARGS:
    <metric>    the metric
```


## Todo

* command: read
  - async
  - human timestamps
* command: list
* command: clean
