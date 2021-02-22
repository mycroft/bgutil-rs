/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use std::str::FromStr;
use std::convert::TryFrom;
use std::error;

use cassandra_cpp::{Batch,BatchType,BindRustType,CassCollection,CassResult,Consistency,Error,Map,Statement};
use cassandra_cpp::Session as CassSession;
use cassandra_cpp::Uuid as CassUuid;
use cassandra_cpp::{stmt};
use chrono::Utc;
use clap::{App,AppSettings,Arg,SubCommand};

use uuid::Uuid;

mod cassandra;
mod metric;
mod session;
mod stage;
mod timerange;

use crate::cassandra::*;
use crate::metric::*;
use crate::session::Session;
use crate::stage::*;
use crate::timerange::*;

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

pub fn metric_info(session: &CassSession, metric_name: &str) -> Result<(), Error> {
    let metric = fetch_metric(session, metric_name)?;

    println!("{}", metric);

    Ok(())
}

fn prepare_component_query(table_name: &str, arguments: &Vec<&str>) -> Result<Statement, Error> {
    let mut q = format!("SELECT parent, name FROM biggraphite_metadata.{} WHERE ", table_name);
    let mut component_number = 0;
    let mut components = vec![];

    for (id, component) in arguments.iter().enumerate() {
        let mut operator = "=";

        if *component == "*" {
            component_number += 1;
            continue;
        }

        if component_number != 0 {
            q.push_str("AND ");
        }

        if component.ends_with("*") {
            operator = "LIKE";
        }

        q.push_str(format!("component_{} {} ? ", id, operator).as_str());
        component_number += 1;
        components.push(component.replace("*", "%"));
    }

    if component_number != 0 {
        q.push_str("AND ");
    }

    // Adding last component for __END__.
    q.push_str(format!("component_{} = ? ALLOW FILTERING;", component_number).as_str());
    components.push("__END__".to_string());

    let mut query = stmt!(q.as_str());

    for (id, arg) in components.iter().enumerate() {
        query.bind(id, arg.as_str())?;
    }

    Ok(query)
}

fn metric_list(session_metadata: &CassSession, glob: &str) -> Result<(), Error> {
    let components = glob.split(".").collect::<Vec<&str>>();

    let mut query_directories = prepare_component_query("directories", &components)?;
    query_directories.set_consistency(Consistency::QUORUM)?;
    let result = session_metadata.execute(&query_directories).wait()?;
    for row in result.iter() {
        let name = row.get_column_by_name("name".to_string()).unwrap().to_string();
        println!("d {}", name);
    }

    let mut query = prepare_component_query("metrics", &components)?;
    query.set_consistency(Consistency::QUORUM)?;
    let result = session_metadata.execute(&query).wait()?;

    let names = result
        .iter()
        .map(|x| {
            x.get_column_by_name("name".to_string()).unwrap().to_string()
        })
        .collect::<Vec<String>>();

    let metrics = fetch_metrics(session_metadata, &names)?;
    for metric in metrics {
        println!("m {}", metric);
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let matches = App::new("bgutil-rs")
                           .setting(AppSettings::SubcommandRequired)
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
                           .get_matches();

    let contact_points_metadata = "tag--cstars07--cassandra-cstars07.query.consul.preprod.crto.in";
    let contact_points_data = "tag--cstars04--cassandra-cstars04.query.consul.preprod.crto.in";

    let session = Session::new(&contact_points_metadata, &contact_points_data)?;

    match matches.subcommand_name() {
        Some("info") => {
            let matches = matches.subcommand_matches("info").unwrap();
            metric_info(session.metadata_session(), matches.value_of("metric").unwrap())?;
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
            let metric = fetch_metric(session.metadata_session(), metric_name)?;

            let available_stages = metric.stages()?;
            let stage = Stage::try_from(stage)?;

            if !available_stages.iter().any(|x| *x == stage) {
                eprintln!("Could not find any stage matching {}", stage);
                return Ok(());
            }

            fetch_points(session.points_session(), &metric, &stage, time_start, time_end)?;
        },
        Some("list") => {
            let matches = matches.subcommand_matches("list").unwrap();
            metric_list(session.metadata_session(), matches.value_of("glob").unwrap())?;
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

            metric_write(session.metadata_session(), session.points_session(), metric, value, retention, timestamp)?;
        },
        Some("delete") => {
            let matches = matches.subcommand_matches("delete").unwrap();
            let metric = matches.value_of("metric").unwrap();

            if matches.is_present("recursive") {
                unimplemented!();
            }

            metric_delete(session.metadata_session(), &metric)?;
        }
        None => {
            eprintln!("No command was used.");
            return Ok(());
        },
        _ => {}
    }

    Ok(())
}
