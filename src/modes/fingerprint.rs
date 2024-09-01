use std::{
    net::{Ipv4Addr, SocketAddrV4},
    time::{Duration, SystemTime},
};

use bson::{doc, Document};
use futures_util::StreamExt;

use crate::database::{self, Database};

// pub async fn get_addrs_and_protocol_versions(
//     database: &Database,
// ) -> anyhow::Result<Vec<(SocketAddrV4, i32)>> {
//     let mut results = Vec::new();

//     let filter = doc! {
//         "timestamp": {
//             // must be online
//             "$gt": bson::DateTime::from(SystemTime::now() -
// Duration::from_secs(60 * 60 * 3600)),         },
//         "$or": [
//             {
//                 "fingerprint.activeMinecraft.timestamp": {
//                     // the last active fingerprint must've been over a week
// ago                     "$lt": bson::DateTime::from(SystemTime::now() -
// Duration::from_secs(60 * 60 * 24 * 7)),                 }
//             },
//             { "fingerprint.activeMinecraft": { "$exists": false } },
//         ]
//     };

//     println!("filter: {:?}", filter);

//     let mut pipeline: Vec<Document> = vec![doc! { "$match": filter }];
//     pipeline.push(doc! { "$project": { "ip": 1, "port": 1, "protocol": 1,
// "_id": 0 } });     pipeline.push(doc! { "$sort": { "timestamp": 1 } });

//     let mut cursor = database
//         .servers_coll()
//         .aggregate(pipeline)
//         .batch_size(2000)
//         .await
//         .unwrap();

//     while let Some(Ok(doc)) = cursor.next().await {
//         if let Ok(ip_str) = doc.get_str("ip") {
//             if let Ok(addr) = ip_str.parse::<Ipv4Addr>() {
//                 if let Some(port) = database::get_u32(&doc, "port") {
//                     let protocol_version =
// doc.get_i32("protocol").unwrap_or(47);

//                     results.push((SocketAddrV4::new(addr, port as u16),
// protocol_version));                     if results.len() % 1000 == 0 {
//                         println!("{} ips", results.len());
//                     }
//                 }
//             }
//         }
//     }

//     Ok(results)
// }

pub async fn get_addrs_and_protocol_versions(
    database: &Database,
) -> anyhow::Result<Vec<(SocketAddrV4, i32)>> {
    let mut results = Vec::new();

    let two_hours_ago = SystemTime::now() - Duration::from_secs(60 * 60 * 2);
    let over_a_week_ago = SystemTime::now() - Duration::from_secs(60 * 60 * 24 * 7);
    let filter = doc! {
        "lastSeen": {
            "$gt": bson::DateTime::from(two_hours_ago),
        },
        "$or": [
            {
                "fingerprintTimestamp": {
                    "$lt": bson::DateTime::from(over_a_week_ago),
                }
            },
            {
                "fingerprintTimestamp": {
                    "$exists": false
                }
            }
        ]
    };

    // Debug prints (keep these)
    println!("Two hours ago: {:?}", two_hours_ago);
    println!("Over a week ago: {:?}", over_a_week_ago);
    println!("Filter: {:?}", filter);

    let mut pipeline: Vec<Document> = vec![doc! { "$match": filter.clone() }];
    pipeline.push(doc! { "$project": { "ip": 1, "port": 1, "protocol": 1, "_id": 0 } });
    pipeline.push(doc! { "$sort": { "timestamp": 1 } });
    println!("Full pipeline: {:?}", pipeline);

    let mut cursor = database.servers_coll().find(filter).await?;

    let mut count = 0;
    let mut ip_missing = 0;
    let mut port_missing = 0;
    while let Some(result) = cursor.next().await {
        count += 1;
        if count % 1000 == 0 {
            println!("Processed {} documents", count);
        }

        let doc = result?;
        let ip_str = doc.get_str("ip").ok();
        let port = database::get_u32(&doc, "port");

        if ip_str.is_none() {
            ip_missing += 1;
        }
        if port.is_none() {
            port_missing += 1;
        }

        if let (Some(ip_str), Some(port)) = (ip_str, port) {
            let protocol_version = doc.get_i32("protocol").unwrap_or(47);
            let addr = ip_str.parse::<Ipv4Addr>().unwrap();
            results.push((SocketAddrV4::new(addr, port as u16), protocol_version));
        }

        // Debug print for every 10000th document
        if count % 10000 == 0 {
            println!("Sample document: {:?}", doc);
        }
    }

    println!("Total documents processed: {}", count);
    println!("Documents missing IP: {}", ip_missing);
    println!("Documents missing port: {}", port_missing);
    println!("Total results: {}", results.len());

    Ok(results)
}
