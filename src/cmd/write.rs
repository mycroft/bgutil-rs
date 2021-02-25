/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use std::error;
use std::str::FromStr;

use crate::Session;
use crate::Stage;

use crate::create_metric;
use crate::fetch_metric;

use cassandra_cpp::stmt;
use cassandra_cpp::BindRustType;
use cassandra_cpp::Uuid as CassUuid;

use std::convert::TryFrom;

pub fn metric_write(session: &Session, metric_name: &str, value: f64, retention: &str, timestamp: i64) -> Result<(), Box<dyn error::Error>> {
    let mut query = stmt!("SELECT * FROM biggraphite_metadata.metrics_metadata WHERE name = ?");
    query.bind(0, metric_name)?;

    let result = session.metadata_session().execute(&query).wait()?;
    if result.row_count() == 0 {
        create_metric(session, metric_name)?;
    }

    let stage = Stage::try_from(retention)?;

    let metric = fetch_metric(session, metric_name)?;
    let (time_start_ms, offset) = stage.time_offset_ms(timestamp);

    let query = format!(
        "INSERT INTO biggraphite.{} (metric, time_start_ms, offset, value) VALUES (?, ?, ?, ?);",
        stage.table_name()
    );

    let mut query = stmt!(&query);
    query.bind(0, CassUuid::from_str(metric.id().as_str())?)?;
    query.bind(1, time_start_ms)?;
    query.bind(2, offset as i16)?;
    query.bind(3, value)?;

    session.points_session().execute(&query).wait()?;

    Ok(())
}
