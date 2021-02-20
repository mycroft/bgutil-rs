/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */
use crate::stage::Stage;

use std::collections::HashMap;
use std::fmt;
use std::convert::TryFrom;

use cassandra_cpp::Row;

#[derive(Debug)]
pub struct Metric {
    id: String,
    name: String,
    config: HashMap<String, String>,
    created_on: u64,
    updated_on: u64
}

impl Metric {
    pub fn id(self: &Self) -> &String {
        &self.id
    }

    pub fn config(self: &Self, name: String) -> Result<String, String> {
        let res = self.config.get(&name);
        if let Some(v) = res {
            Ok(v.to_string())
        } else {
            Err("Invalid key".to_string())
        }
    }

    pub fn stages(self: &Self) -> Result<Vec<Stage>, String> {
        let mut out = vec![];
        let stages = self.config("retention".to_string());

        if let Err(err) = stages {
            return Err(err);
        }

        for stage in stages.unwrap().split(":") {
            match Stage::try_from(stage) {
                Ok(stage) => out.push(stage),
                Err(err) => {
                    return Err(err.to_string())
                }
            };
        }

        Ok(out)
    }
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} created_on:{} updated_on:{}\n{:?}",
            self.name,
            self.created_on,
            self.updated_on,
            self.config,
        )
    }
}

impl From<String> for Metric {
    fn from(name: String) -> Self {
        Metric {
            id: String::from(""),
            name: name,
            config: HashMap::new(),
            created_on: 0,
            updated_on: 0
        }
    }
}

impl From<Row> for Metric {
    fn from(row: Row) -> Self {
        let config_collection = row.get_column_by_name("config".to_string()).unwrap().get_map().unwrap();
        let mut config : HashMap<String, String> = HashMap::new();
        config_collection
            .map(|(k, v)| config.insert(k.to_string(), v.to_string()))
            .count();

        let created_on = row.get_column_by_name("created_on".to_string()).unwrap();
        let created_on_timestamp = created_on.get_uuid().unwrap().timestamp();

        let updated_on = row.get_column_by_name("updated_on".to_string()).unwrap();
        let updated_on_timestamp = updated_on.get_uuid().unwrap().timestamp();

        Self {
            id: row.get_column_by_name("id".to_string()).unwrap().get_uuid().unwrap().to_string(),
            name: row.get_column_by_name("name".to_string()).unwrap().to_string(),
            config: config,
            created_on: created_on_timestamp,
            updated_on: updated_on_timestamp
        }
    }
}
