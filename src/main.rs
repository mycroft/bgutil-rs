/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */

use std::collections::HashMap;
use std::convert::TryFrom;
use std::error;

use cassandra_cpp::{BindRustType,CassResult,Error,Statement};
use cassandra_cpp::{stmt};
use chrono::Utc;
use clap::{App,AppSettings,Arg,SubCommand};

mod cassandra;
mod metric;
mod session;
mod stage;
mod timerange;

use crate::cassandra::*;
use crate::session::Session;
use crate::stage::*;
use crate::metric::Metric;


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

pub fn metric_info(session: &Session, metric_name: &str) -> Result<(), Error> {
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

fn prepare_component_query_globstar(table_name: &str, arguments: &Vec<&str>) -> Result<Vec<Statement>, Error> {
    let _q = format!("SELECT parent, name FROM biggraphite_metadata.{} WHERE ", table_name);
    let _component_number = 0;

    let mut out = vec![];

    let pos_globstar = arguments.iter().enumerate().filter(|(_, &x)| x == "**").map(|(id, _)| id).collect::<Vec<usize>>();
    if pos_globstar.len() != 1 {
        // XXX return error
        return Ok(vec![prepare_component_query(table_name, arguments)?]);
    }

    let pos_globstar = pos_globstar[0];
    let mut queries = vec![];

    let mut init_args = vec![];
    let mut end_args = arguments[pos_globstar+1..].to_vec();
    end_args.push("__END__");

    for (id, el) in arguments[0..pos_globstar].iter().enumerate() {
        if *el == "*" {
            continue;
        }

        if el.ends_with("*") {
            init_args.push((id, "LIKE", el.replace("*", "%")));
        } else {
            init_args.push((id, "=", el.to_string()));
        }
    }

    let components = 16;

    for id in init_args.len()..(components-end_args.len()+1) {
        let mut current_query = init_args.to_vec();

        for (sub_id, el) in end_args.iter().enumerate() {
            if *el == "*" {
                continue;
            }

            if el.ends_with("*") {
                current_query.push((sub_id + id, "LIKE", el.replace("*", "%")));
            } else {
                current_query.push((sub_id + id, "=", el.to_string()));
            }
        }

        queries.push(current_query);
    }

    for query in &queries {
        let mut current_query = _q.to_string();

        for el in query {
            if el.0 != 0 {
                current_query.push_str("AND ");
            }

            current_query.push_str(&format!("component_{} {} ? ", el.0, el.1));
        }

        current_query.push_str(&String::from("ALLOW FILTERING;"));

        let mut statement = stmt!(&current_query);
        for (id, el) in query.iter().enumerate() {
            statement.bind(id, el.2.as_str())?;
        }

        out.push(statement);
    }

    Ok(out)
}

fn metric_list(session: &Session, glob: &str) -> Result<(), Error> {
    let components = glob.split(".").collect::<Vec<&str>>();

    let query_directories = prepare_component_query_globstar("directories", &components)?;
    let mut results = vec![];

    for mut q in query_directories {
        q.set_consistency(session.read_consistency())?;
        results.push(session.metadata_session().execute(&q));
    }

    for result in results {
        let rows = result.wait()?;
        for row in rows.iter() {
            let name = row.get_column_by_name("name".to_string()).unwrap().to_string();
            println!("d {}", name);

        }
    }

    let query_metrics = prepare_component_query_globstar("metrics", &components)?;
    let mut results = vec![];

    for mut q in query_metrics {
        q.set_consistency(session.read_consistency())?;
        results.push(session.metadata_session().execute(&q));
    }

    for result in results {
        let rows = result.wait()?;
        let names = rows
            .iter()
            .map(|x| {
                x.get_column_by_name("name".to_string()).unwrap().to_string()
            })
            .collect::<Vec<String>>();

        let metrics = fetch_metrics(session, &names)?;
        for metric in metrics {
            println!("m {}", metric);
        }
    }

    Ok(())
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
                                        .about("Stats")
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
                           .get_matches();

    let mut contact_points_metadata = "localhost";
    if matches.is_present("contact-metadata") {
        contact_points_metadata = matches.value_of("contact-metadata").unwrap();
    }

    let mut contact_points_data = "localhost";
    if matches.is_present("contact-points") {
        contact_points_data = matches.value_of("contact-points").unwrap();
    }

    let session = Session::new(&contact_points_metadata, &contact_points_data)?;

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
        None => {
            eprintln!("No command was used.");
            return Ok(());
        },
        _ => {}
    }

    Ok(())
}

fn metric_stats(session: &Session, start_key: i64, end_key: i64) -> Result<(), Error> {
    let q =
        "SELECT id, name, token(name), config, created_on, updated_on, read_on \
         FROM biggraphite_metadata.metrics_metadata WHERE token(name) > ? LIMIT 1000";

    let mut current_token = start_key;
    let mut n = 0;
    let mut points : u64 = 0;

    let mut stats : HashMap<String, usize> = HashMap::new();

    while current_token < end_key {
        let mut query = stmt!(q);
        query.bind(0, current_token)?;

        let results = session.metadata_session().execute(&query).wait()?;

        for row in results.iter() {
            current_token = row.get_column(2)?.get_i64()?;

            let metric : Metric = row.into();
            let stages = match metric.stages() {
                Ok(stages) => stages,
                Err(_) => continue,
            };

            for stage in stages {
                points += stage.points() as u64;
            }

            let parts = metric.name().split(".").collect::<Vec<&str>>();
            *stats.entry(String::from(parts[0])).or_insert(0) += 1;

            n += 1;
        }
    }

    let p : f64 = ((current_token - start_key) / std::i64::MAX) as f64;

    println!("Range: {} -> {} ({:.4}%)", start_key, current_token, 100. * p);
    println!("{} metrics", n);
    println!("{} points", points);
    println!("-----");

    let mut vec : Vec<(&String, &usize)> = stats.iter().collect();
    vec.sort_by(|a, b| b.1.cmp(a.1));

    for (id, v) in vec.iter().enumerate() {
        println!("{} {}", v.0, v.1);

        if id == 10 {
            break;
        }
    }

    Ok(())
}

fn metrics_clean(session: &Session, start_key: i64, end_key: i64, clean_metrics: bool, clean_directories: bool) -> Result<(), Error> {
    let mut current_token = start_key;
    let cutoff : u64 = (Utc::now().timestamp() as u64 - 86400 * 14) * 1000;

    let namespace = "biggraphite_metadata";
    let batch_limit = 1000;

    let query = format!("SELECT name, token(name) FROM {}.metrics_metadata \
                         WHERE updated_on <= maxTimeuuid({}) and token(name) > ? and token(name) < ? LIMIT {};",
                        namespace, cutoff, batch_limit);
    let delete_metric_query = format!("DELETE FROM {}.metrics WHERE name = ?;", namespace);
    let delete_metadata_query = format!("DELETE FROM {}.metrics_metadata WHERE name = ?;", namespace);

    let mut deleted_metrics_count = 0;
    let mut scanned_metrics_count = 0;
    let mut scanned_directories_count = 0;
    let mut deleted_directories_count = 0;

    // clean metrics
    loop {
        if !clean_metrics || current_token >= end_key {
            // println!("Stopping: {} >= {}", current_token, end_key);
            break;
        }

        let mut outdated_metrics_query = stmt!(query.as_str());
        outdated_metrics_query.set_consistency(session.read_consistency())?;
        outdated_metrics_query.bind(0, current_token)?;
        outdated_metrics_query.bind(1, end_key)?;

        let result = session.metadata_session().execute(&outdated_metrics_query).wait()?;
        if result.row_count() == 0 {
            break;
        }
        let mut queries = vec![];

        for row in result.iter() {
            let name = row.get_column_by_name("name".to_string())?.to_string();
            scanned_metrics_count += 1;
            let mut delete_metric_query = stmt!(delete_metric_query.as_str());
            delete_metric_query.set_consistency(session.write_consistency())?;
            delete_metric_query.bind(0, name.as_str())?;
            queries.push(session.metadata_session().execute(&delete_metric_query));

            let mut delete_metadata_query = stmt!(delete_metadata_query.as_str());
            delete_metric_query.set_consistency(session.write_consistency())?;
            delete_metadata_query.bind(0, name.as_str())?;
            queries.push(session.metadata_session().execute(&delete_metadata_query));

            deleted_metrics_count += 1;
            current_token = row.get_column(1)?.get_i64()?;
        }

        if result.row_count() != batch_limit {
            // println!("Stopping because count == 0");
            break;
        }

        for query in queries {
            if let Err(err) = query.wait() {
                eprintln!("Failed: {:?}", err);
            }
        }
    }

    let list_directories_query = format!("SELECT name, token(name) FROM {}.directories WHERE token(name) > ? AND token(name) < ? LIMIT {};",
        namespace, batch_limit);
    let metric_query = format!("SELECT name FROM {}.metrics WHERE parent LIKE ? LIMIT 1", namespace);
    let delete_directory_query = format!("DELETE FROM {}.directories WHERE name = ?;", namespace);

    current_token = start_key;

    // clean directories
    loop {
        if !clean_directories || current_token >= end_key {
            break;
        }

        let mut list_directories_query = stmt!(list_directories_query.as_str());
        list_directories_query.set_consistency(session.read_consistency())?;
        list_directories_query.bind(0, current_token)?;
        list_directories_query.bind(1, end_key)?;

        let list_result = session.metadata_session().execute(&list_directories_query).wait()?;
        if list_result.row_count() == 0 {
            break;
        }

        let mut queries = vec![];

        for row in list_result.iter() {
            let mut name = row.get_column_by_name("name".to_string())?.to_string();
            let orig_name = name.clone();
            name.push_str(".%");
            current_token = row.get_column(1)?.get_i64()?;

            let mut metric_query = stmt!(metric_query.as_str());
            metric_query.set_consistency(session.read_consistency())?;
            metric_query.bind(0, name.as_str())?;
            let query = session.metadata_session().execute(&metric_query);

            queries.push((orig_name, query));
        }

        let mut to_delete_queries = vec![];

        for el in queries {
            let result = el.1.wait()?;
            scanned_directories_count += 1;
            if result.row_count() != 0 {
                continue;
            }

            let mut delete_directory_query = stmt!(delete_directory_query.as_str());
            delete_directory_query.set_consistency(session.write_consistency())?;
            delete_directory_query.bind(0, el.0.as_str())?;

            to_delete_queries.push(session.metadata_session().execute(&delete_directory_query));

            deleted_directories_count += 1;
        }

        for to_delete in to_delete_queries {
            to_delete.wait()?;
        }

        if list_result.row_count() != batch_limit {
            break;
        }
    }

    println!("Deleted {} metrics, {} directories.", deleted_metrics_count, deleted_directories_count);
    println!("Scanned {} metrics, {} directories", scanned_metrics_count, scanned_directories_count);

    Ok(())
}
