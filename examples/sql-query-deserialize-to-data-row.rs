use pinot_client_rust::response::{ResponseStats, SqlBrokerResponse};
use pinot_client_rust::response::data::DataRow;

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
    let table = "scoreSheet";

    println!("===Querying SQL===");
    for query in &pinot_queries {
        println!("\n---Trying to query Pinot: {}---", query);
        let broker_response = client.execute_sql(table, query, true).unwrap();
        print_sql_broker_resp(&broker_response);
    }
}

fn print_sql_broker_resp(broker_resp: &SqlBrokerResponse<DataRow>) {
    if let Some(stats) = &broker_resp.stats {
        print_resp_stats(stats);
    }
    if let Some(result_table) = &broker_resp.result_table {
        println!("Broker response table results: {:?}", result_table);
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
