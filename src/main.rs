/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use std::convert::TryFrom;
use std::error;

use cassandra_cpp::CassResult;
use chrono::Utc;
use clap::{App,AppSettings,Arg,SubCommand};

mod cassandra;
mod metric;
mod session;
mod stage;
mod timerange;
mod cmd;

use crate::cassandra::*;
use crate::session::Session;
use crate::stage::Stage;
use crate::metric::Metric;

use crate::cmd::clean::*;
use crate::cmd::delete::*;
use crate::cmd::info::*;
use crate::cmd::list::*;
use crate::cmd::local_clean::*;
use crate::cmd::stats::*;
use crate::cmd::write::*;

#[allow(dead_code)]
fn describe_result(result: &CassResult) {
    println!("Result has {} record(s).", result.row_count());
    println!("Schema is:");

    for column_id in 0..result.column_count() {
        println!("{:?}: {:?}",
            result.column_type(column_id as usize),
            result.column_name(column_id as usize)
        );
    }
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let matches = App::new("bgutil-rs")
                           .setting(AppSettings::SubcommandRequired)
                           .arg(Arg::with_name("contact-metadata")
                                .long("contact-metadata")
                                .env("CASSANDRA_CONTACT_METADATA")
                                .takes_value(true))
                           .arg(Arg::with_name("contact-points")
                                .long("contact-points")
                                .env("CASSANDRA_CONTACT_POINTS")
                                .takes_value(true))
                           .arg(Arg::with_name("dry-run")
                                .help("Do not write in database (local-clean only)")
                                .long("dry-run"))
                           .subcommand(SubCommand::with_name("info")
                                       .about("Information about a metric")
                                       .arg(Arg::with_name("metric")
                                            .help("metric to retrieve info about")
                                            .index(1)
                                            .required(true)))
                           .subcommand(SubCommand::with_name("read")
                                       .about("Read a metric contents")
                                       .arg(Arg::with_name("stage")
                                            .long("stage")
                                            .takes_value(true))
                                       .arg(Arg::with_name("time-start")
                                            .long("time-start")
                                            .takes_value(true))
                                       .arg(Arg::with_name("time-end")
                                            .long("time-end")
                                            .takes_value(true))
                                       .arg(Arg::with_name("metric")
                                            .help("metric to get values")
                                            .index(1)
                                            .required(true)))
                           .subcommand(SubCommand::with_name("list")
                                       .about("List metrics with given pattern")
                                       .arg(Arg::with_name("glob")
                                            .index(1)
                                            .required(true)))
                           .subcommand(SubCommand::with_name("write")
                                       .about("Write a metric and its value")
                                       .arg(Arg::with_name("metric")
                                            .index(1)
                                            .required(true))
                                       .arg(Arg::with_name("value")
                                            .index(2)
                                            .required(true))
                                       .arg(Arg::with_name("timestamp")
                                            .short("t")
                                            .long("timestamp")
                                            .takes_value(true))
                                       .arg(Arg::with_name("retention")
                                            .long("retention")
                                            .takes_value(true)))
                           .subcommand(SubCommand::with_name("delete")
                                       .about("Delete metric(s)")
                                       .arg(Arg::with_name("recursive")
                                            .long("recursive"))
                                       .arg(Arg::with_name("metric")
                                            .index(1)
                                            .required(true)))
                           .subcommand(SubCommand::with_name("stats")
                                        .about("Stats")
                                        .arg(Arg::with_name("start-key")
                                             .long("start-key")
                                             .takes_value(true))
                                        .arg(Arg::with_name("end-key")
                                             .long("end-key")
                                             .takes_value(true)))
                           .subcommand(SubCommand::with_name("clean")
                                        .about("Clean outdated metrics & empty directories")
                                        .arg(Arg::with_name("start-key")
                                             .long("start-key")
                                             .takes_value(true))
                                        .arg(Arg::with_name("end-key")
                                             .long("end-key")
                                             .takes_value(true))
                                        .arg(Arg::with_name("clean-metrics")
                                             .long("clean-metrics"))
                                        .arg(Arg::with_name("clean-directories")
                                             .long("clean-directories")))
                           .subcommand(SubCommand::with_name("local-clean")
                                        .about("Clean a directory of outdated metrics & empty sub-directories")
                                        .arg(Arg::with_name("directory")
                                             .index(1)
                                             .required(true)))
                           .get_matches();

    let mut contact_points_metadata = "localhost";
    if matches.is_present("contact-metadata") {
        contact_points_metadata = matches.value_of("contact-metadata").unwrap();
    }

    let mut contact_points_data = "localhost";
    if matches.is_present("contact-points") {
        contact_points_data = matches.value_of("contact-points").unwrap();
    }

    let dry_run = matches.is_present("dry-run");

    let mut session = Session::new(&contact_points_metadata, &contact_points_data)?;
    session.set_dry_run(dry_run);

    match matches.subcommand_name() {
        Some("info") => {
            let matches = matches.subcommand_matches("info").unwrap();
            metric_info(&session, matches.value_of("metric").unwrap())?;
        },
        Some("read") => {
            let matches = matches.subcommand_matches("read").unwrap();
            let stage = matches.value_of("stage").unwrap_or("11520*60s");
            // XXX: Change default value relative to stage's precision to have more or less data
            let time_start = matches.value_of("time-start"); // default now - 1h
            let time_end = matches.value_of("time-end"); // default: now

            let time_start = match time_start {
                None => Utc::now().timestamp() - 3600,
                Some(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("Could not parse {}", s);
                        return Ok(())
                    }
                }
            };

            let time_end = match time_end {
                None => time_start + 3600,
                Some(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("Could not parse {}", s);
                        return Ok(())
                    }
                }
            };

            let metric_name = matches.value_of("metric").unwrap();
            let metric = fetch_metric(&session, metric_name)?;

            let available_stages = metric.stages()?;
            let stage = Stage::try_from(stage)?;

            if !available_stages.iter().any(|x| *x == stage) {
                eprintln!("Could not find any stage matching {}", stage);
                return Ok(());
            }

            fetch_points(&session, &metric, &stage, time_start, time_end)?;
        },
        Some("list") => {
            let matches = matches.subcommand_matches("list").unwrap();
            metric_list(&session, matches.value_of("glob").unwrap())?;
        },
        Some("write") => {
            let matches = matches.subcommand_matches("write").unwrap();

            let metric = matches.value_of("metric").unwrap();
            let value = matches.value_of("value").unwrap().parse::<f64>()?;

            let retention = matches.value_of("retention").unwrap_or("11520*60s");
            let timestamp = match matches.value_of("timestamp") {
                None => Utc::now().timestamp(),
                Some(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("Could not parse {}", s);
                        return Ok(())
                    }
                }
            };

            metric_write(&session, metric, value, retention, timestamp)?;
        },
        Some("delete") => {
            let matches = matches.subcommand_matches("delete").unwrap();
            let metric = matches.value_of("metric").unwrap();

            if matches.is_present("recursive") {
                unimplemented!();
            }

            metric_delete(&session, &metric)?;
        },
        Some("stats") => {
            let matches = matches.subcommand_matches("stats").unwrap();
            let start_key = matches.value_of("start-key");
            let end_key = matches.value_of("end-key");

            let start_key = match start_key {
                None => 0,
                Some(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("Could not parse {}", s);
                        return Ok(())
                    }
                }
            };

            let end_key = match end_key {
                None => 100000000000000,
                Some(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("Could not parse {}", s);
                        return Ok(())
                    }
                }
            };

            metric_stats(&session, start_key, end_key)?;
        },
        Some("clean") => {
            let matches = matches.subcommand_matches("clean").unwrap();

            let start_key = matches.value_of("start-key");
            let end_key = matches.value_of("end-key");

            let start_key = match start_key {
                None => std::i64::MIN,
                Some(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("Could not parse {}", s);
                        return Ok(())
                    }
                }
            };

            let end_key = match end_key {
                None => std::i64::MAX,
                Some(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("Could not parse {}", s);
                        return Ok(())
                    }
                }
            };

            let clean_metrics = matches.is_present("clean-metrics");
            let clean_directories = matches.is_present("clean-directories");

            metrics_clean(&session, start_key, end_key, clean_metrics, clean_directories)?;
        },
        Some("local-clean") => {
            let matches = matches.subcommand_matches("local-clean").unwrap();
            let directory = matches.value_of("directory").unwrap();

            metrics_local_clean(&session, directory)?;
        }
        None => {
            eprintln!("No command was used.");
            return Ok(());
        },
        _ => {}
    }

    Ok(())
}


