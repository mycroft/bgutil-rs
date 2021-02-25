/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use std::error;

use crate::Session;
use crate::fetch_metric;

pub fn metric_info(session: &Session, metric_name: &str) -> Result<(), Box<dyn error::Error>> {
    let metric = fetch_metric(session, metric_name)?;

    println!("{}", metric);

    Ok(())
}
