/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use std::str::FromStr;
use std::convert::TryFrom;

use cassandra_cpp::*;
use chrono::Utc;
use clap::{App,AppSettings,Arg,SubCommand};

mod metric;
mod stage;
use crate::metric::*;
use crate::stage::*;


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

fn connect(contact_points: &str) -> Result<Session> {
    set_level(LogLevel::DISABLED);

    let mut cluster = Cluster::default();
    cluster.set_contact_points(contact_points).unwrap();
    cluster.set_load_balance_round_robin();

    cluster.set_protocol_version(4)?;

    cluster.connect()
}

fn fetch_metric(session: &Session, metric_name: &str) -> Result<Metric> {
    let mut query = stmt!("SELECT * FROM biggraphite_metadata.metrics_metadata WHERE name = ?");
    query.bind(0, metric_name)?;

    let result =  session.execute(&query).wait()?;
    Ok(result.first_row().unwrap().into())
}

fn metric_info(session: &Session, metric_name: &str) -> Result<()> {
    let metric = fetch_metric(session, metric_name)?;

    println!("{}", metric);

    Ok(())
}

fn fetch_points(session_points: &Session, m: &Metric, s: &Stage, time_start: i64, time_end: i64) -> Result<()> {
    let table_name = s.table_name();

    let q = format!(
        "SELECT time_start_ms, offset, value FROM biggraphite.{} WHERE metric = ? AND time_start_ms = ? AND offset >= ? AND offset < ? ORDER BY offset",
        table_name
    );

    let ranges = TimeRange::new(&s, time_start, time_end).ranges();
    // XXX concurrent
    for range in ranges.iter() {
        let mut query = stmt!(q.as_str());
        query.bind(0, Uuid::from_str(m.id().as_str())?)?;
        query.bind(1, range.0)?;
        query.bind(2, range.1 as i16)?;
        query.bind(3, range.2 as i16)?;

        let result =  session_points.execute(&query).wait()?;

        for row in result.iter() {
            let ts : i64 = row.get_column_by_name("time_start_ms".to_string())?.get_i64()?;
            let offset : i16 = row.get_column_by_name("offset".to_string())?.get_i16()?;
            let value : f64 = row.get_column_by_name("value".to_string())?.get_f64()?;

            let ts = ts / 1000;
            let offset : i64 = offset as i64 * s.precision_as_seconds();

            println!("{:?};{:?}", ts + offset, value);
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let matches = App::new("bgutil-rs")
                           .setting(AppSettings::SubcommandRequired)
                           .subcommand(SubCommand::with_name("info")
                                       .about("Information about a metric")
                                       .arg(Arg::with_name("metric")
                                            .help("metric to retrieve info about")
                                            .index(1)
                                            .required(true)
                                        ))
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
                                            .required(true)
                                        ))
                          .get_matches();

    let contact_points_metadata = "tag--cstars07--cassandra-cstars07.query.consul.preprod.crto.in";
    let contact_points_data = "tag--cstars04--cassandra-cstars04.query.consul.preprod.crto.in";

    let session_metadata = match connect(contact_points_metadata) {
        Ok(session) => session,
        Err(err) => {
            eprintln!("{:?}", err);
            return Ok(());
        }
    };

    let session_points = match connect(contact_points_data) {
        Ok(session) => session,
        Err(err) => {
            eprintln!("{:?}", err);
            return Ok(());
        }
    };

    match matches.subcommand_name() {
        Some("info") => {
            let matches = matches.subcommand_matches("info").unwrap();
            metric_info(&session_metadata, matches.value_of("metric").unwrap())?;
        },
        Some("read") => {
            let matches = matches.subcommand_matches("read").unwrap();
            let stage = matches.value_of("stage").unwrap_or("11520*60s");
            // XXX: Change default value relative to stage's precision to have more or less data
            let time_start = matches.value_of("time-start"); // default now - 1h
            let time_end= matches.value_of("time-end"); // default: now

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
            let metric = fetch_metric(&session_metadata, metric_name)?;

            let available_stages = metric.stages()?;
            let stage = Stage::try_from(stage)?;

            if !available_stages.iter().any(|x| *x == stage) {
                eprintln!("Could not find any stage matching {}", stage);
                return Ok(());
            }

            fetch_points(&session_points, &metric, &stage, time_start, time_end)?;
        }
        None => {
            eprintln!("No command was used.");
            return Ok(());
        },
        _ => {}
    }

    Ok(())
}
