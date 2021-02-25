/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use std::error;

use cassandra_cpp::stmt;
use cassandra_cpp::BindRustType;
use crate::fetch_metric;

use crate::Session;

pub fn metric_delete(session: &Session, metric_name: &str) -> Result<(), Box<dyn error::Error>> {
    let mut query = stmt!("SELECT * FROM biggraphite_metadata.metrics_metadata WHERE name = ?");
    query.bind(0, metric_name)?;

    let result = session.metadata_session().execute(&query).wait()?;
    if result.row_count() == 0 {
        println!("Metric is not existing");
        return Ok(());
    }

    let _metric = fetch_metric(session, metric_name)?;

    let mut query = stmt!("DELETE FROM biggraphite_metadata.metrics_metadata WHERE name = ?;");
    query.bind(0, metric_name)?;
    query.set_consistency(session.write_consistency())?;
    session.metadata_session().execute(&query).wait()?;

    let mut query = stmt!("DELETE FROM biggraphite_metadata.metrics_metadata WHERE name = ?;");
    query.bind(0, metric_name)?;
    query.set_consistency(session.write_consistency())?;
    session.metadata_session().execute(&query).wait()?;

    let mut query = stmt!("DELETE FROM biggraphite_metadata.directories WHERE name = ?;");
    query.bind(0, metric_name)?;
    query.set_consistency(session.write_consistency())?;
    session.metadata_session().execute(&query).wait()?;

    Ok(())
}
