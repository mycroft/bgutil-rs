/*
 * bgutil-rs
 *
 * Author: Patrick MARIE <pm@mkz.me>
 */

use cassandra_cpp::Session as CassSession;
use cassandra_cpp::Error as CassError;
use cassandra_cpp::Consistency;

use crate::cassandra::*;

pub struct Session {
    metadata: CassSession,
    points: CassSession,
}

impl Session {
    pub fn new(metadata_contact: &str, points_contact: &str) -> Result<Self, CassError> {
        let metadata = connect(metadata_contact)?;
        let points = connect(points_contact)?;

        let session = Self {
            metadata: metadata,
            points: points,
        };

        Ok(session)
    }

    pub fn metadata_session(&self) -> &CassSession {
        &self.metadata
    }

    pub fn points_session(&self) -> &CassSession {
        &self.points
    }

    // XXX to make configurable
    pub fn read_consistency(&self) -> Consistency {
        Consistency::LOCAL_QUORUM
    }

    // XXX to make configurable
    pub fn write_consistency(&self) -> Consistency {
        Consistency::LOCAL_QUORUM
    }
}