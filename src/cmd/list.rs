/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */

use crate::prepare_component_query_globstar;
use crate::fetch_metrics;
use crate::Session;
use cassandra_cpp::{Error};

pub fn metric_list(session: &Session, glob: &str) -> Result<(), Error> {
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
        let res = result.wait();
        if let Err(err) = res {
            eprintln!("Query failed: {}", err);
            continue;
        }

        let rows = res.unwrap();
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
