use pinot_client_rust::response::BrokerResponse;

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
        print_broker_resp(broker_response);
    }

    println!("\n===Querying PQL===");
    for query in &pinot_queries {
        println!("\n---Trying to query Pinot: {}---", query);
        let broker_response = client.execute_pql(table, query).unwrap();
        print_broker_resp(broker_response);
    }
}

fn print_broker_resp(broker_resp: BrokerResponse) {
    println!(
        "Query Stats: response time - {} ms, scanned docs - {}, total docs - {}",
        broker_resp.time_used_ms,
        broker_resp.num_docs_scanned,
        broker_resp.total_docs,
    );
    if !broker_resp.exceptions.is_empty() {
        println!("Broker response exceptions: {:?}", broker_resp.exceptions);
        return;
    }
    if let Some(result_table) = broker_resp.result_table {
        println!("Broker response table results: {:?}", result_table);
        return;
    }
    if !broker_resp.aggregation_results.is_empty() {
        println!("Broker response aggregation results: {:?}", broker_resp.aggregation_results);
        return;
    }
    if let Some(selection_results) = broker_resp.selection_results {
        println!("Broker response selection results: {:?}", selection_results);
        return;
    }
}
