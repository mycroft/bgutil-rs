/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use crate::Session;

use crate::fetch_metric;
use cassandra_cpp::Error;

pub fn metric_info(session: &Session, metric_name: &str) -> Result<(), Error> {
    let metric = fetch_metric(session, metric_name)?;

    println!("{}", metric);

    Ok(())
}
