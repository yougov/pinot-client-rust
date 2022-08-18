use pinot_client_rust::response::{PqlResponse, ResponseStats};

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

    println!("\n===Querying PQL===");
    for query in &pinot_queries {
        println!("\n---Trying to query Pinot: {}---", query);
        let response = client.execute_pql(table, query, true).unwrap();
        print_pql_response(&response);
    }
}

fn print_pql_response(broker_resp: &PqlResponse) {
    if let Some(stats) = &broker_resp.stats {
        print_stats(stats);
    }
    if !broker_resp.aggregation_results.is_empty() {
        println!("Broker response aggregation results: {:?}", broker_resp.aggregation_results);
        return;
    }
    if let Some(selection_results) = &broker_resp.selection_results {
        println!("Broker response selection results: {:?}", selection_results);
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