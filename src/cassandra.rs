/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use std::str::FromStr;

use crate::metric::Metric;
use crate::session::Session;
use crate::stage::Stage;
use crate::timerange::TimeRange;

use cassandra_cpp::Session as CassSession;
use cassandra_cpp::Uuid as CassUuid;
use cassandra_cpp::{Batch,BatchType,BindRustType,CassCollection,Cluster,Error,LogLevel,Map,Statement};
use cassandra_cpp::{set_level,stmt};

use uuid::Uuid;

pub fn connect(contact_points: &str) -> Result<CassSession, Error> {
    set_level(LogLevel::DISABLED);

    let mut cluster = Cluster::default();
    cluster.set_contact_points(contact_points).unwrap();
    cluster.set_load_balance_round_robin();

    cluster.set_protocol_version(4)?;

    cluster.connect()
}

pub fn prepare_component_query(table_name: &str, arguments: &Vec<&str>) -> Result<Statement, Error> {
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

pub fn prepare_component_query_globstar(table_name: &str, arguments: &Vec<&str>) -> Result<Vec<Statement>, Error> {
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

pub fn fetch_metric(session: &Session, metric_name: &str) -> Result<Metric, Error> {
    let mut query = stmt!("SELECT * FROM biggraphite_metadata.metrics_metadata WHERE name = ?");
    query.bind(0, metric_name)?;

    // XXX set consistency
    // query.set_consistency(session.read_consistency());

    let result =  session.metadata_session().execute(&query).wait()?;
    Ok(result.first_row().unwrap().into())
}

pub fn fetch_points(session: &Session, m: &Metric, s: &Stage, time_start: i64, time_end: i64) -> Result<(), Error> {
    let table_name = s.table_name();

    let q = format!(
        "SELECT time_start_ms, offset, value FROM biggraphite.{} WHERE metric = ? AND time_start_ms = ? AND offset >= ? AND offset < ? ORDER BY offset",
        table_name
    );

    let ranges = TimeRange::new(&s, time_start, time_end).ranges();
    // XXX concurrent
    for range in ranges.iter() {
        let mut query = stmt!(q.as_str());
        query.bind(0, CassUuid::from_str(m.id().as_str())?)?;
        query.bind(1, range.0)?;
        query.bind(2, range.1 as i16)?;
        query.bind(3, range.2 as i16)?;

        let result =  session.points_session().execute(&query).wait()?;

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

/// async fetch multiple metrics
pub fn fetch_metrics(session: &Session, metric_names: &Vec<String>) -> Result<Vec<Metric>, Error> {
    let mut results = vec![];
    let mut out = vec![];

    for metric_name in metric_names.iter() {
        let mut query = stmt!("SELECT * FROM biggraphite_metadata.metrics_metadata WHERE name = ?");
        query.bind(0, metric_name.as_str())?;
        query.set_consistency(session.read_consistency())?;

        let result = session.metadata_session().execute(&query);
        results.push(result);
    }

    for result in results {
        let result = result.wait()?;

        if result.row_count() < 1 {
            continue;
        }

        out.push(result.first_row().unwrap().into())
    }

    Ok(out)
}

pub fn create_metric(session: &Session, metric: &str) -> Result<(), Error> {
    let mut batch = Batch::new(BatchType::LOGGED);

    let metrics_parts = metric.split(".").collect::<Vec<&str>>();

    for d in 0..metrics_parts.len() {
        let mut fields = vec![String::from("name"), String::from("parent")];
        let mut values = vec![];
        let n = metrics_parts.len() - d;

        let path = &metrics_parts[0..n].join(".");
        let parent_path = &mut metrics_parts[0..n-1].join(".");
        parent_path.push('.');

        values.push(String::from(path));
        values.push(parent_path.to_string());

        for id in 0..=n {
            let field = format!("component_{}", id);
            fields.push(field);
            if id != n {
                values.push(String::from(metrics_parts[id]));
            } else {
                values.push(String::from("__END__"));
            }
        }

        let query = format!("INSERT INTO biggraphite_metadata.{}({}) VALUES ({});",
            String::from("directories"),
            fields.join(", "),
            fields.iter().map(|_| String::from("?")).collect::<Vec<String>>().join(", ")
        );

        // before anything, create the "metrics" record.
        if d == 0 {
            let query_metrics = format!("INSERT INTO biggraphite_metadata.{}({}) VALUES ({});",
                String::from("metrics"),
                fields.join(", "),
                fields.iter().map(|_| String::from("?")).collect::<Vec<String>>().join(", ")
            );

            let mut query_metrics = stmt!(query_metrics.as_str());

            for (id, arg) in values.iter().enumerate() {
                query_metrics.bind(id, arg.as_str())?;
            }

            batch.add_statement(&query_metrics)?;
        }

        let mut query = stmt!(query.as_str());

        for (id, arg) in values.iter().enumerate() {
            query.bind(id, arg.as_str())?;
        }

        batch.add_statement(&query)?;
    }

    let query = format!(
        "INSERT INTO biggraphite_metadata.metrics_metadata(name, config, id, created_on, updated_on) VALUES (?, ?, ?, now(), now())"
    );

    let uuid = Uuid::new_v4();

    let mut config = Map::new(0);
    config.append_string("aggregator")?;
    config.append_string("average")?;

    config.append_string("carbon_xfilesfactor")?;
    config.append_string("0.500000")?;

    config.append_string("retention")?;
    config.append_string("11520*60s:720*3600s:730*86400s")?;

    let mut query = stmt!(&query);
    query.bind(0, metric)?; // name
    query.bind(1, config)?; // config
    query.bind(2, CassUuid::from_str(&uuid.to_hyphenated().to_string())?)?; 

    query.set_consistency(session.write_consistency())?;

    session.metadata_session().execute(&query).wait()?;

    // Write directories
    session.metadata_session().execute_batch(batch).wait()?;

    println!("Metric was written.");

    Ok(())
}

