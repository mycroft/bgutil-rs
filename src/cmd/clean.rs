/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use crate::Session;

use cassandra_cpp::{BindRustType,Error};
use cassandra_cpp::stmt;

use chrono::Utc;

pub fn metrics_clean(session: &Session, start_key: i64, end_key: i64, clean_metrics: bool, clean_directories: bool) -> Result<(), Error> {
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
