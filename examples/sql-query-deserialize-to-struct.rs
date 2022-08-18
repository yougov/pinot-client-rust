use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use pinot_client_rust::response::{ResponseStats, SqlResponse};
use pinot_client_rust::response::deserialise::{
    deserialize_bytes,
    deserialize_double,
    deserialize_doubles,
    deserialize_float,
    deserialize_floats,
    deserialize_json,
    deserialize_timestamp,
};

#[derive(PartialEq, Debug, Deserialize)]
struct Row {
    names: Vec<String>,
    #[serde(rename(deserialize = "gameIds"))]
    game_ids: Vec<i32>,
    scores: Vec<i64>,
    #[serde(rename(deserialize = "handicapAdjustedScores"))]
    #[serde(deserialize_with = "deserialize_floats")]
    handicap_adjusted_scores: Vec<f32>,
    #[serde(rename(deserialize = "handicapAdjustedScores_highPrecision"))]
    #[serde(deserialize_with = "deserialize_doubles")]
    handicap_adjusted_scores_high_precision: Vec<f64>,
    handle: String,
    age: i32,
    #[serde(rename(deserialize = "totalScore"))]
    total_score: i64,
    #[serde(rename(deserialize = "avgScore"))]
    #[serde(deserialize_with = "deserialize_float")]
    avg_score: f32,
    #[serde(rename(deserialize = "avgScore_highPrecision"))]
    #[serde(deserialize_with = "deserialize_double")]
    avg_score_high_precision: f64,
    #[serde(rename(deserialize = "hasPlayed"))]
    has_played: bool,
    #[serde(rename(deserialize = "dateOfBirth"))]
    #[serde(deserialize_with = "deserialize_timestamp")]
    date_of_birth: DateTime<Utc>,
    #[serde(rename(deserialize = "dateOfFirstGame"))]
    date_of_first_game: i64,
    #[serde(deserialize_with = "deserialize_json")]
    extra: Value,
    #[serde(deserialize_with = "deserialize_bytes")]
    raw: Vec<u8>,
}

fn main() {
    let client = pinot_client_rust::connection::client_from_broker_list(
        vec!["localhost:8099".to_string()], None).unwrap();
    let table = "scoreSheet";
    let query = "select * from scoreSheet limit 2";

    println!("===Querying SQL===");
    println!("\n---Trying to query Pinot: {}---", query);
    let response = client.execute_sql(table, query, true).unwrap();
    print_sql_response(&response);
}

fn print_sql_response(broker_resp: &SqlResponse<Row>) {
    if let Some(stats) = &broker_resp.stats {
        print_stats(stats);
    }
    if let Some(table) = &broker_resp.table {
        println!("Broker response table results: {:?}", table);
        return;
    }
}

fn print_stats(stats: &ResponseStats) {
    println!(
        "Query Stats: response time - {} ms, scanned docs - {}, total docs - {}",
        stats.time_used_ms,
        stats.num_docs_scanned,
        stats.total_docs,
    );
}
