/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use cassandra_cpp::{BindRustType,Error};
use cassandra_cpp::stmt;

use std::collections::HashMap;

use crate::Metric;
use crate::Session;

pub fn metric_stats(session: &Session, start_key: i64, end_key: i64) -> Result<(), Error> {
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
