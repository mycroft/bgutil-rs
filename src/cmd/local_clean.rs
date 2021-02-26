/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use std::error;

use crate::Session;

use crate::delete_directory;
use crate::delete_metric;
use crate::fetch_metric;
use crate::prepare_component_query_globstar;

use cassandra_cpp::stmt;
use cassandra_cpp::BindRustType;

use chrono::Utc;

fn clean_metrics_in_directory(session: &Session, directory: &str) -> Result<(), Box<dyn error::Error>> {
    // println!("Cleaning metrics in directory: '{}'", directory);

    let mut directory = String::from(directory);
    directory.push_str(".**");

    let components = directory.split(".").collect::<Vec<&str>>();
    let mut results = vec![];

    let query_metrics = prepare_component_query_globstar("metrics", &components)?;
    let outdated_ts : u64 = (Utc::now().timestamp() as u64 - 14 * 86400) * 1000;

    for mut q in query_metrics {
        q.set_consistency(session.read_consistency())?;
        results.push(session.metadata_session().execute(&q));
    }

    for result in results {
        let rows = result.wait()?;
        for row in rows.iter() {
            let name = row.get_column_by_name("name".to_string()).unwrap().to_string();
            let metric = fetch_metric(session, &name);

            if let Err(e) = metric {
                eprintln!("Error while retrieving metric: {}", e);
                continue;
            }

            let metric = metric.unwrap();

            if metric.updated_on() > outdated_ts {
                continue;
            }

            println!("Deleting metric {}", metric.name());
            if session.is_dry_run() {
                continue;
            }
            delete_metric(session, metric.name())?;
        }
    }

    Ok(())
}

fn directory_has_metrics(session: &Session, directory: &str) -> Result<bool, Box<dyn error::Error>> {
    let query = format!("SELECT name FROM biggraphite_metadata.metrics WHERE parent LIKE ? LIMIT 1;");
    let mut query = stmt!(query.as_str());
    let mut directory = String::from(directory);
    directory.push_str(".%");
    query.bind(0, directory.as_str())?;

    let result = session.metadata_session().execute(&query).wait()?;

    Ok(result.row_count() != 0)
}

fn clean_empty_directories_in_directory(session: &Session, directory: &str) -> Result<(), Box<dyn error::Error>> {
    // println!("Cleaning empty directories in directory '{}'", directory);

    let mut directory = String::from(directory);
    directory.push_str(".**");

    let components = directory.split(".").collect::<Vec<&str>>();
    let mut results = vec![];

    let query_directories = prepare_component_query_globstar("directories", &components)?;

    for mut q in query_directories {
        q.set_consistency(session.read_consistency())?;
        results.push(session.metadata_session().execute(&q));
    }

    for result in results {
        let rows = result.wait()?;
        for row in rows.iter() {
            let name = row.get_column_by_name("name".to_string()).unwrap().to_string();

            if directory_has_metrics(session, &name)? {
                continue;
            }

            println!("Deleting directory {}", name);
            if session.is_dry_run() {
                continue;
            }
            delete_directory(session, &name)?;
        }
    }

    Ok(())
}

pub fn metrics_local_clean(session: &Session, directory: &str) -> Result<(), Box<dyn error::Error>> {
    let components = directory.split(".").collect::<Vec<&str>>();

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

            clean_metrics_in_directory(session, &name)?;
            clean_empty_directories_in_directory(session, &name)?;
        }
    }

    Ok(())
}
