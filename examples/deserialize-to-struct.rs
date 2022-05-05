use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use pinot_client_rust::response::{Exception, PqlBrokerResponse, ResponseStats, SqlBrokerResponse};
use pinot_client_rust::response::deserialise::{
    deserialize_json,
};

#[derive(PartialEq, Debug, Deserialize)]
struct Row {
    names: Vec<String>,
    #[serde(rename(deserialize = "gameIds"))]
    game_ids: Vec<i32>,
    scores: Vec<i64>,
    #[serde(rename(deserialize = "handicapAdjustedScores"))]
    handicap_adjusted_scores: Vec<f32>,
    #[serde(rename(deserialize = "handicapAdjustedScores_highPrecision"))]
    handicap_adjusted_scores_high_precision: Vec<f64>,
    handle: String,
    age: i32,
    #[serde(rename(deserialize = "totalScore"))]
    total_score: i64,
    #[serde(rename(deserialize = "avgScore"))]
    avg_score: f32,
    #[serde(rename(deserialize = "avgScore_highPrecision"))]
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
    let pinot_queries: Vec<String> = vec![
        "select * from scoreSheet limit 2",
        "select count(*) as cnt from scoreSheet limit 1",
        "select count(*) as cnt, sum(totalScore) as sum_totalScore from scoreSheet limit 1",
        "select name, count(*) as cnt, sum(totalScore) as sum_totalScore from scoreSheet group by name limit 10",
        "select max(totalScore) from scoreSheet limit 10",
    ].into_iter().map(|s| s.to_string()).collect();
    let table = "baseballStats";

    println!("===Querying SQL===");
    for query in &pinot_queries {
        println!("\n---Trying to query Pinot: {}---", query);
        let broker_response = client.execute_sql(table, query).unwrap();
        print_sql_broker_resp(&broker_response);
    }

    println!("\n===Querying PQL===");
    for query in &pinot_queries {
        println!("\n---Trying to query Pinot: {}---", query);
        let broker_response = client.execute_pql(table, query).unwrap();
        print_pql_broker_resp(&broker_response);
    }
}

fn print_sql_broker_resp(broker_resp: &SqlBrokerResponse<Row>) {
    print_resp_stats(&broker_resp.stats);
    print_exceptions(&broker_resp.exceptions);
    if let Some(result_table) = &broker_resp.result_table {
        println!("Broker response table results: {:?}", result_table);
        return;
    }
}

fn print_pql_broker_resp(broker_resp: &PqlBrokerResponse) {
    print_resp_stats(&broker_resp.stats);
    print_exceptions(&broker_resp.exceptions);
    if !broker_resp.aggregation_results.is_empty() {
        println!("Broker response aggregation results: {:?}", broker_resp.aggregation_results);
        return;
    }
    if let Some(selection_results) = &broker_resp.selection_results {
        println!("Broker response selection results: {:?}", selection_results);
        return;
    }
}

fn print_resp_stats(stats: &ResponseStats) {
    println!(
        "Query Stats: response time - {} ms, scanned docs - {}, total docs - {}",
        stats.time_used_ms,
        stats.num_docs_scanned,
        stats.total_docs,
    );
}

fn print_exceptions(exceptions: &Vec<Exception>) {
    if !exceptions.is_empty() {
        println!("Broker response exceptions: {:?}", exceptions);
        return;
    }
}
