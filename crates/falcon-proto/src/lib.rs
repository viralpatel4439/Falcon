#![deny(unsafe_code)]

pub mod replication {
    tonic::include_proto!("kv.replication.v1");
}

use falcon_events::{ChangeEvent, ChangeValue, Hlc};
use replication::change_event_proto::Value as ProtoValue;
use replication::ChangeEventProto;

impl From<&ChangeEvent> for ChangeEventProto {
    fn from(event: &ChangeEvent) -> Self {
        let value = match &event.value {
            ChangeValue::Put(v) => ProtoValue::PutValue(v.clone()),
            ChangeValue::Delete => ProtoValue::Tombstone(true),
        };
        ChangeEventProto {
            keyspace: event.keyspace.clone(),
            key: event.key.clone(),
            sequence: event.sequence,
            timestamp_ms: event.timestamp as u64,
            origin_region: event.origin_region.clone(),
            value: Some(value),
            hlc_wall: event.hlc.wall,
            hlc_logical: event.hlc.logical,
            hlc_region: event.hlc.region.clone(),
        }
    }
}

impl From<ChangeEventProto> for ChangeEvent {
    fn from(proto: ChangeEventProto) -> Self {
        let value = match proto.value {
            Some(ProtoValue::PutValue(v)) => ChangeValue::Put(v),
            _ => ChangeValue::Delete,
        };
        ChangeEvent {
            keyspace: proto.keyspace,
            key: proto.key,
            value,
            sequence: proto.sequence,
            timestamp: proto.timestamp_ms as u128,
            origin_region: proto.origin_region,
            hlc: Hlc {
                wall: proto.hlc_wall,
                logical: proto.hlc_logical,
                region: proto.hlc_region,
            },
        }
    }
}