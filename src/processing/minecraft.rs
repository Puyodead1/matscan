use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    hash::{Hash, Hasher},
    net::SocketAddrV4,
    sync::{Arc, LazyLock},
    time::SystemTime,
};

use anyhow::bail;
use async_trait::async_trait;
use azalea_chat::FormattedText;
use bson::{doc, Bson, Document};
use mongodb::options::UpdateOptions;
use parking_lot::Mutex;
use regex::Regex;
use serde::Deserialize;
use tracing::error;

use crate::{
    config::Config,
    database::{self, bulk_write::BulkUpdate, CachedIpHash, Database},
    scanner::protocols,
};

use super::{ProcessableProtocol, SharedData};

const ANONYMOUS_PLAYER_NAME: &str = "Anonymous Player";

#[async_trait]
impl ProcessableProtocol for protocols::Minecraft {
    fn process(
        shared: &Arc<Mutex<SharedData>>,
        config: &Config,
        target: SocketAddrV4,
        data: &[u8],
        database: &Database,
    ) -> Option<BulkUpdate> {
        let data = String::from_utf8_lossy(data);

        // let passive_fingerprint = generate_passive_fingerprint(&data).ok();

        let data: serde_json::Value = match serde_json::from_str(&data) {
            Ok(json) => json,
            Err(_) => {
                // not a minecraft server ig
                return None;
            }
        };

        if let Some(cleaned_data) = clean_response_data(&data) {
            let mongo_update = doc! { "$set": cleaned_data };
            match create_bulk_update(database, &target, mongo_update) {
                Ok(r) => Some(r),
                Err(err) => {
                    error!("Error updating server {target}: {err}");
                    None
                }
            }
        } else {
            None
        }
    }
}

/// Clean up the response data from the server into something we can insert into
/// our database.
fn clean_response_data(data: &serde_json::Value) -> Option<Document> {
    let data_serde_json = data.as_object()?.to_owned();
    let mut data = Bson::deserialize(data).ok()?;
    let mut data = data.as_document_mut()?.to_owned();
    // default to empty string if description is missing
    let Some(description) = data_serde_json
        .get("description")
        .map(|d| FormattedText::deserialize(d).unwrap_or_default())
    else {
        // no description, so probably not even a minecraft server
        return None;
    };

    let description = description.to_string();

    // update description to be a string
    data.insert(
        "description".to_string(),
        Bson::String(description.to_string()),
    );

    // maybe in the future we can store favicons in a separate collection
    // if data.contains_key("favicon") {
    //     data.insert("favicon", Bson::Boolean(true));
    // }

    if data.contains_key("modinfo") {
        // forge server
        data.insert("isModded", Bson::Boolean(true));
    }

    if data.contains_key("forgeData")
        || data.contains_key("modinfo")
        || data.contains_key("modpackData")
    {
        data.insert("isModded", Bson::Boolean(true));
    }

    let version_name = data
        .get("version")
        .and_then(|v| v.as_document())
        .and_then(|v| v.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or_default();

    let version_protocol = data
        .get("version")
        .and_then(|v| v.as_document())
        .and_then(|v| v.get("protocol"))
        .and_then(|p| p.as_i32())
        .unwrap_or_default();

    let max_players = data
        .get("players")
        .and_then(|p| p.as_document())
        .and_then(|p| p.get("max"))
        .and_then(|m| m.as_i32())
        .unwrap_or_default();
    let online_players = data
        .get("players")
        .and_then(|p| p.as_document())
        .and_then(|p| p.get("online"))
        .and_then(|m| m.as_i32())
        .unwrap_or_default();

    if description.contains("Craftserve.pl - wydajny hosting Minecraft!")
        || description.contains("Ochrona DDoS: Przekroczono limit polaczen.")
        || description.contains("¨ |  ")
        || description.contains("Start the server at FalixNodes.net/start")
        || description.contains("This server is offline Powcered by FalixNodes.net")
        || description.contains("Serwer jest aktualnie wy")
        || description.contains("Blad pobierania statusu. Polacz sie bezposrednio!")
        || matches!(
            version_name,
            "COSMIC GUARD" | "TCPShield.com" | "â  Error" | "⚠ Error"
        )
    {
        return None;
    }

    let mut is_online_mode: Option<bool> = None;
    let mut mixed_online_mode = false;
    let mut fake_sample = false;
    let mut has_players = false;

    let mut players_data = bson::Document::default();
    let mut extra_data = bson::Document::default();

    // servers with this motd randomize the online players
    let should_ignore_players = description == "To protect the privacy of this server and its\nusers, you must log in once to see ping data.";

    if !should_ignore_players {
        for player in data
            .get("players")
            .and_then(|p| p.as_document())
            .and_then(|p| p.get("sample"))
            .and_then(|p| p.as_array())
            // only take the first 100 players in case the sample is massive
            .map(|s| s.iter().take(100).collect::<Vec<_>>())
            .unwrap_or_default()
        {
            let player = player.as_document()?;

            let uuid = player
                .get("id")
                .and_then(|id| id.as_str())
                .unwrap_or_default();
            let name = player
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default();

            let uuid = uuid.replace('-', "");

            static UUID_REGEX: LazyLock<Regex> =
                LazyLock::new(|| Regex::new("[0-9a-f]{12}[34][0-9a-f]{19}").unwrap());

            // anonymous player is a nil uuid so it wouldn't match the regex
            if !UUID_REGEX.is_match(&uuid) && name != ANONYMOUS_PLAYER_NAME {
                fake_sample = true;
            }

            if !mixed_online_mode {
                // ignore nil uuids (anonymous players)
                let is_nil = uuid.chars().all(|c| c == '0');
                if !is_nil {
                    // uuidv4 means online mode
                    // uuidv3 means offline mode
                    let is_uuidv4 = uuid.len() >= 12 && uuid[12..].starts_with('4');
                    if (is_uuidv4 && is_online_mode == Some(false))
                        || (!is_uuidv4 && is_online_mode == Some(true))
                    {
                        mixed_online_mode = true;
                    } else if is_online_mode.is_none() {
                        is_online_mode = Some(is_uuidv4);
                    }
                }
            }

            let mut player_doc = Document::new();
            player_doc.insert(
                "last_seen",
                Bson::DateTime(bson::DateTime::from_system_time(SystemTime::now())),
            );
            player_doc.insert("name", Bson::String(name.to_string()));

            players_data.insert(format!("players.{}", uuid), player_doc);

            has_players = true;
        }
    }

    if !fake_sample {
        if mixed_online_mode {
            extra_data.insert("isCracked", Bson::Null);
        } else if let Some(is_online_mode) = is_online_mode {
            extra_data.insert("isCracked", Bson::Boolean(is_online_mode));
        }
        extra_data.insert(
            "last_seen",
            Bson::DateTime(bson::DateTime::from_system_time(SystemTime::now())),
        );
        if has_players {
            extra_data.insert(
                "lastActive",
                Bson::DateTime(bson::DateTime::from_system_time(SystemTime::now())),
            );
        } else {
            extra_data.insert(
                "lastEmpty",
                Bson::DateTime(bson::DateTime::from_system_time(SystemTime::now())),
            );
        }
    }

    let mut final_cleaned = doc! {
        "updated_at": bson::DateTime::from_system_time(SystemTime::now()),
        "online_players": online_players,
        "max_players": max_players,
        "version": version_name,
        "protocol": version_protocol,
        "description": description,
    };

    if !fake_sample {
        final_cleaned.extend(players_data);
    }

    // if let Some(passive_minecraft_fingerprint) = passive_minecraft_fingerprint {
    //     final_cleaned.insert(
    //         "fingerprint.minecraft.incorrectOrder",
    //         Bson::Boolean(passive_minecraft_fingerprint.incorrect_order),
    //     );
    //     if let Some(field_order) = passive_minecraft_fingerprint.field_order {
    //         final_cleaned.insert(
    //             "fingerprint.minecraft.fieldOrder",
    //             Bson::String(field_order),
    //         );
    //     }
    //     final_cleaned.insert(
    //         "fingerprint.minecraft.emptySample",
    //         Bson::Boolean(passive_minecraft_fingerprint.empty_sample),
    //     );
    //     final_cleaned.insert(
    //         "fingerprint.minecraft.emptyFavicon",
    //         Bson::Boolean(passive_minecraft_fingerprint.empty_favicon),
    //     );
    // }

    // final_cleaned.extend(data);
    final_cleaned.extend(extra_data);

    Some(final_cleaned)
}

pub fn create_bulk_update(
    database: &Database,
    target: &SocketAddrV4,
    mongo_update: Document,
) -> anyhow::Result<BulkUpdate> {
    if database.shared.lock().bad_ips.contains(target.ip()) && target.port() != 25565 {
        // no
        bail!("bad ip");
    }

    fn determine_hash(mongo_update: &Document) -> anyhow::Result<u64> {
        let set_data = mongo_update.get_document("$set")?;
        // let minecraft = set_data.get_document("minecraft")?;

        // let version = set_data.get_document("version")?;

        let description = set_data.get_str("description").unwrap_or_default();
        let version_name = set_data.get_str("version").unwrap_or_default();
        let version_protocol = database::get_i32(set_data, "protocol").unwrap_or_default();
        let max_players = database::get_i32(set_data, "max_players").unwrap_or_default();

        let mut hasher = DefaultHasher::new();
        (description, version_name, version_protocol, max_players).hash(&mut hasher);
        Ok(hasher.finish())
    }

    let mut is_bad_ip = false;
    {
        let mut shared = database.shared.lock();
        let ips_with_same_hash = shared.ips_with_same_hash.get_mut(target.ip());
        if let Some((data, previously_checked_ports)) = ips_with_same_hash {
            if !previously_checked_ports.contains(&target.port()) {
                if let Some(count) = &mut data.count {
                    let this_server_hash = determine_hash(&mongo_update)?;

                    if this_server_hash == data.hash {
                        *count += 1;
                        previously_checked_ports.insert(target.port());

                        if *count >= 100 {
                            // too many servers with the same hash... add to bad ips!
                            println!("found a new bad ip: {} :(", target.ip());
                            // calls add_to_bad_ips slightly lower down
                            // we have to do it like that to avoid keeping the lock during the await
                            is_bad_ip = true;
                        }
                    } else {
                        // this server has a different hash than the other servers with the same IP
                        data.count = None;
                    }
                }
            }
        } else {
            let this_server_hash = determine_hash(&mongo_update)?;
            shared.ips_with_same_hash.insert(
                *target.ip(),
                (
                    CachedIpHash {
                        count: Some(1),
                        hash: this_server_hash,
                    },
                    HashSet::from_iter(vec![target.port()]),
                ),
            );
        }
    }

    if is_bad_ip {
        tokio::spawn(database.to_owned().add_to_bad_ips(*target.ip()));
        bail!("bad ip {target:?}");
    }

    // println!("{addr}:{port} -> {mongo_update:?}");
    // println!("{}:{}", target.ip(), target.port());

    Ok(BulkUpdate {
        query: doc! {
            "ip": { "$eq": target.ip().to_string() },
            "port": { "$eq": target.port() as u32 }
        },
        update: mongo_update,
        options: Some(UpdateOptions::builder().upsert(true).build()),
    })
}

async fn send_to_webhook(webhook_url: String, message: String) {
    let client = reqwest::Client::new();
    if let Err(e) = client
        .post(webhook_url)
        .json(
            &vec![("content".to_string(), message.to_string())]
                .into_iter()
                .collect::<HashMap<String, String>>(),
        )
        .send()
        .await
    {
        println!("{}", e);
    }
}

// pub struct PassiveMinecraftFingerprint {
//     pub incorrect_order: bool,
//     pub field_order: Option<String>,
//     /// Servers shouldn't have the sample field if there are no players
// online.     pub empty_sample: bool,
//     /// A favicon that has the string ""
//     pub empty_favicon: bool,
// }
// pub fn generate_passive_fingerprint(data: &str) ->
// anyhow::Result<PassiveMinecraftFingerprint> {     let data: serde_json::Value
// = serde_json::from_str(data)?;

//     let protocol_version = data
//         .get("version")
//         .and_then(|s| s.as_object())
//         .and_then(|s| s.get("protocol"))
//         .and_then(|s| s.as_u64())
//         .unwrap_or_default();

//     let empty_favicon = data.get("favicon").map(|s| s.as_str()) ==
// Some(Some(""));

//     let mut incorrect_order = false;
//     let mut field_order = None;
//     let mut empty_sample = false;

//     // the correct field order is description, players, version (ignore
// everything     // else)

//     if let Some(data) = data.as_object() {
//         // mojang changed the order in 23w07a/1.19.4
//         let correct_order = if matches!(protocol_version, 1073741943.. |
// 762..=0x40000000 ) {             ["version", "description", "players"]
//         } else {
//             ["description", "players", "version"]
//         };

//         let keys = data
//             .keys()
//             .filter(|&k| correct_order.contains(&k.as_str()))
//             .cloned()
//             .collect::<Vec<_>>();

//         let players = data.get("players").and_then(|s| s.as_object());
//         let version = data.get("version").and_then(|s| s.as_object());

//         let correct_players_order = ["max", "online"];
//         let correct_version_order = ["name", "protocol"];

//         let players_keys = players
//             .map(|s| {
//                 s.keys()
//                     .filter(|&k| correct_players_order.contains(&k.as_str()))
//                     .cloned()
//                     .collect::<Vec<_>>()
//             })
//             .unwrap_or_default();
//         let version_keys = version
//             .map(|s| {
//                 s.keys()
//                     .filter(|&k| correct_version_order.contains(&k.as_str()))
//                     .cloned()
//                     .collect::<Vec<_>>()
//             })
//             .unwrap_or_default();

//         // if keys != correct_order
//         //     || players_keys != correct_players_order
//         //     || version_keys != correct_version_order
//         // {
//         //     incorrect_order = true;
//         // }

//         // if incorrect_order {
//         //     let mut field_order_string = String::new();
//         //     for (i, key) in keys.iter().enumerate() {
//         //         field_order_string.push_str(key);
//         //         if key == "players" && players_keys !=
// correct_players_order {         //
// field_order_string.push_str(format!("({})",
// players_keys.join(",")).as_str());         //         } else if key ==
// "version" && version_keys != correct_version_order {         //
// field_order_string.push_str(format!("({})",
// version_keys.join(",")).as_str());         //         }
//         //         if i != keys.len() - 1 {
//         //             field_order_string.push(',');
//         //         }
//         //     }
//         //     field_order = Some(field_order_string);
//         // }

//         if let Some(players) = data.get("players").and_then(|s|
// s.as_object()) {             if let Some(sample) =
// players.get("sample").and_then(|s| s.as_array()) {                 if
// sample.is_empty() {                     empty_sample = true;
//                 }
//             }
//         }
//     }

//     Ok(PassiveMinecraftFingerprint {
//         incorrect_order,
//         field_order,
//         empty_sample,
//         empty_favicon,
//     })
// }
