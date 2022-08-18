use bimap::BiMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::{Error, Result};
use crate::response::{DataType, ResponseStats};
use crate::response::raw::{RawBrokerResponse, RawBrokerResponseWithoutStats, RawSchema, RawTable};

/// Data structure for a broker response to an SQL query.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct SqlResponse<T: FromRow> {
    pub table: Option<Table<T>>,
    pub stats: Option<ResponseStats>,
}

impl<T: FromRow> From<RawBrokerResponse> for Result<SqlResponse<T>> {
    fn from(raw: RawBrokerResponse) -> Self {
        if !raw.exceptions.is_empty() {
            return Err(Error::PinotExceptions(raw.exceptions));
        };

        let table: Option<Table<T>> = match raw.result_table {
            None => None,
            Some(raw) => Some(Result::from(raw)?),
        };
        Ok(SqlResponse {
            table,
            stats: Some(ResponseStats {
                trace_info: raw.trace_info,
                num_servers_queried: raw.num_servers_queried,
                num_servers_responded: raw.num_servers_responded,
                num_segments_queried: raw.num_segments_queried,
                num_segments_processed: raw.num_segments_processed,
                num_segments_matched: raw.num_segments_matched,
                num_consuming_segments_queried: raw.num_consuming_segments_queried,
                num_docs_scanned: raw.num_docs_scanned,
                num_entries_scanned_in_filter: raw.num_entries_scanned_in_filter,
                num_entries_scanned_post_filter: raw.num_entries_scanned_post_filter,
                num_groups_limit_reached: raw.num_groups_limit_reached,
                total_docs: raw.total_docs,
                time_used_ms: raw.time_used_ms,
                min_consuming_freshness_time_ms: raw.min_consuming_freshness_time_ms,
            }),
        })
    }
}

impl<T: FromRow> From<RawBrokerResponseWithoutStats> for Result<SqlResponse<T>> {
    fn from(raw: RawBrokerResponseWithoutStats) -> Self {
        if !raw.exceptions.is_empty() {
            return Err(Error::PinotExceptions(raw.exceptions));
        };

        let table: Option<Table<T>> = match raw.result_table {
            None => None,
            Some(raw) => Some(Result::from(raw)?),
        };
        Ok(SqlResponse {
            table,
            stats: None,
        })
    }
}

/// Represents any structure which can deserialize
/// a table row of json fields provided a `Schema`
pub trait FromRow: Sized {
    fn from_row(
        data_schema: &Schema,
        row: Vec<Value>,
    ) -> std::result::Result<Self, serde_json::Error>;
}

/// Holder for SQL queries.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Table<T: FromRow> {
    schema: Schema,
    rows: Vec<T>,
}

impl<T: FromRow> Table<T> {
    pub fn new(
        schema: Schema,
        rows: Vec<T>,
    ) -> Self {
        Table { schema, rows }
    }

    /// Returns the schema
    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns how many rows in the Table
    pub fn get_row_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns a row given a row index
    pub fn get_row(&self, row_index: usize) -> Result<&T> {
        self.rows.get(row_index).ok_or(Error::InvalidResultRowIndex(row_index))
    }

    /// Converts result table into rows vector
    pub fn into_rows(self) -> Vec<T> {
        self.rows
    }

    /// Converts result table into a `Schema` and rows vector
    pub fn into_schema_and_rows(self) -> (Schema, Vec<T>) {
        (self.schema, self.rows)
    }
}

impl<T: FromRow> From<RawTable> for Result<Table<T>> {
    fn from(raw: RawTable) -> Self {
        let schema: Schema = raw.schema.into();
        let rows = raw.rows
            .into_iter()
            .map(|row| T::from_row(&schema, row))
            .collect::<std::result::Result<Vec<T>, serde_json::Error>>()?;
        Ok(Table::new(schema, rows))
    }
}

/// Schema with a bimap to allow easy name <-> index retrieval
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Schema {
    column_data_types: Vec<DataType>,
    column_name_to_index: bimap::BiMap::<String, usize>,
}

impl Schema {
    pub fn new(
        column_data_types: Vec<DataType>,
        column_name_to_index: bimap::BiMap::<String, usize>,
    ) -> Self {
        Self { column_data_types, column_name_to_index }
    }

    /// Returns how many columns in the Table
    pub fn get_column_count(&self) -> usize {
        self.column_data_types.len()
    }

    /// Returns column name given a column index
    pub fn get_column_name(&self, column_index: usize) -> Result<&str> {
        self.column_name_to_index.get_by_right(&column_index)
            .map(|column| column.as_str())
            .ok_or(Error::InvalidResultColumnIndex(column_index))
    }

    /// Returns column index given a column name
    pub fn get_column_index(&self, column_name: &str) -> Result<usize> {
        self.column_name_to_index.get_by_left(column_name)
            .copied()
            .ok_or_else(|| Error::InvalidResultColumnName(column_name.to_string()))
    }

    /// Returns column data type given a column index
    pub fn get_column_data_type(&self, column_index: usize) -> Result<DataType> {
        self.column_data_types.get(column_index)
            .copied()
            .ok_or(Error::InvalidResultColumnIndex(column_index))
    }

    /// Returns column data type given a column index
    pub fn get_column_data_type_by_name(&self, column_name: &str) -> Result<DataType> {
        let column_index = self.get_column_index(column_name)?;
        self.get_column_data_type(column_index)
    }

    /// Returns column data types
    pub fn get_colum_data_types(&self) -> &[DataType] {
        &self.column_data_types
    }

    /// Returns column data types
    pub fn get_column_name_to_index_map(&self) -> &BiMap<String, usize> {
        &self.column_name_to_index
    }

    pub fn into_data_types_and_name_to_index_map(self) -> (Vec<DataType>, BiMap<String, usize>) {
        (self.column_data_types, self.column_name_to_index)
    }
}

impl From<RawSchema> for Schema {
    fn from(raw_schema: RawSchema) -> Self {
        let column_data_types = raw_schema.column_data_types;
        let mut column_name_to_index: BiMap::<String, usize> = BiMap::with_capacity(
            raw_schema.column_names.len());
        for (index, column_name) in raw_schema.column_names.into_iter().enumerate() {
            column_name_to_index.insert(column_name, index);
        }
        Schema { column_data_types, column_name_to_index }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::iter::FromIterator;

    use serde::Deserialize;
    use serde_json::json;

    use crate::response::{DataType::Double as DubT, DataType::Int as IntT, DataType::Long as LngT, PinotException};
    use crate::response::data::{
        Data::Double as DubD,
        Data::Long as LngD,
    };
    use crate::response::data::DataRow;
    use crate::response::data::tests::test_data_row;
    use crate::tests::to_string_vec;

    use super::*;

    #[test]
    fn sql_response_with_pinot_data_types_converts_from_raw_broker_response() {
        let raw_broker_response = RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(RawTable {
                schema: RawSchema {
                    column_data_types: vec![LngT, DubT],
                    column_names: to_string_vec(vec!["cnt", "score"]),
                },
                rows: vec![vec![json!(97889), json!(232.1)]],
            }),
            exceptions: vec![],
            trace_info: Default::default(),
            num_servers_queried: 1,
            num_servers_responded: 2,
            num_segments_queried: 3,
            num_segments_processed: 4,
            num_segments_matched: 5,
            num_consuming_segments_queried: 6,
            num_docs_scanned: 7,
            num_entries_scanned_in_filter: 8,
            num_entries_scanned_post_filter: 9,
            num_groups_limit_reached: false,
            total_docs: 10,
            time_used_ms: 11,
            min_consuming_freshness_time_ms: 12,
        };
        let broker_response: SqlResponse<DataRow> = Result::from(raw_broker_response).unwrap();
        assert_eq!(broker_response, SqlResponse {
            table: Some(Table {
                schema: Schema {
                    column_data_types: vec![LngT, DubT],
                    column_name_to_index: BiMap::from_iter(vec![
                        ("cnt".to_string(), 0), ("score".to_string(), 1),
                    ]),
                },
                rows: vec![DataRow::new(vec![LngD(97889), DubD(232.1)])],
            }),
            stats: Some(ResponseStats {
                trace_info: Default::default(),
                num_servers_queried: 1,
                num_servers_responded: 2,
                num_segments_queried: 3,
                num_segments_processed: 4,
                num_segments_matched: 5,
                num_consuming_segments_queried: 6,
                num_docs_scanned: 7,
                num_entries_scanned_in_filter: 8,
                num_entries_scanned_post_filter: 9,
                num_groups_limit_reached: false,
                total_docs: 10,
                time_used_ms: 11,
                min_consuming_freshness_time_ms: 12,
            }),
        });
    }

    #[test]
    fn sql_response_with_pinot_data_types_converts_from_raw_broker_response_without_stats() {
        let raw_broker_response = RawBrokerResponseWithoutStats {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(RawTable {
                schema: RawSchema {
                    column_data_types: vec![LngT, DubT],
                    column_names: to_string_vec(vec!["cnt", "score"]),
                },
                rows: vec![vec![json!(97889), json!(232.1)]],
            }),
            exceptions: vec![],
        };
        let broker_response: SqlResponse<DataRow> = Result::from(raw_broker_response).unwrap();

        assert_eq!(broker_response, SqlResponse {
            table: Some(Table {
                schema: Schema {
                    column_data_types: vec![LngT, DubT],
                    column_name_to_index: BiMap::from_iter(vec![
                        ("cnt".to_string(), 0), ("score".to_string(), 1),
                    ]),
                },
                rows: vec![DataRow::new(vec![LngD(97889), DubD(232.1)])],
            }),
            stats: None,
        });
    }

    #[test]
    fn pql_response_deserializes_exceptions_correctly() {
        let raw_broker_response = RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: None,
            exceptions: vec![PinotException { error_code: 0, message: "msg".to_string() }],
            trace_info: Default::default(),
            num_servers_queried: 1,
            num_servers_responded: 2,
            num_segments_queried: 3,
            num_segments_processed: 4,
            num_segments_matched: 5,
            num_consuming_segments_queried: 6,
            num_docs_scanned: 7,
            num_entries_scanned_in_filter: 8,
            num_entries_scanned_post_filter: 9,
            num_groups_limit_reached: false,
            total_docs: 10,
            time_used_ms: 11,
            min_consuming_freshness_time_ms: 12,
        };
        let broker_response: Result<SqlResponse<DataRow>> = Result::from(raw_broker_response);
        match broker_response.unwrap_err() {
            Error::PinotExceptions(exceptions) => assert_eq!(
                exceptions, vec![PinotException { error_code: 0, message: "msg".to_string() }]),
            _ => panic!("Wrong variant")
        };
    }

    #[test]
    fn pql_response_deserializes_exceptions_without_stats_correctly() {
        let raw_broker_response = RawBrokerResponseWithoutStats {
            aggregation_results: vec![],
            selection_results: None,
            result_table: None,
            exceptions: vec![PinotException { error_code: 0, message: "msg".to_string() }],
        };
        let broker_response: Result<SqlResponse<DataRow>> = Result::from(raw_broker_response);
        match broker_response.unwrap_err() {
            Error::PinotExceptions(exceptions) => assert_eq!(
                exceptions, vec![PinotException { error_code: 0, message: "msg".to_string() }]),
            _ => panic!("Wrong variant")
        };
    }

    #[test]
    fn sql_response_with_deserializable_struct_converts_from_raw_broker_response() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            cnt: i64,
            score: f64,
        }

        let raw_broker_response = RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(RawTable {
                schema: RawSchema {
                    column_data_types: vec![LngT, DubT],
                    column_names: to_string_vec(vec!["cnt", "score"]),
                },
                rows: vec![vec![json!(97889), json!(232.1)]],
            }),
            exceptions: vec![],
            trace_info: Default::default(),
            num_servers_queried: 1,
            num_servers_responded: 2,
            num_segments_queried: 3,
            num_segments_processed: 4,
            num_segments_matched: 5,
            num_consuming_segments_queried: 6,
            num_docs_scanned: 7,
            num_entries_scanned_in_filter: 8,
            num_entries_scanned_post_filter: 9,
            num_groups_limit_reached: false,
            total_docs: 10,
            time_used_ms: 11,
            min_consuming_freshness_time_ms: 12,
        };
        let broker_response: SqlResponse<TestRow> = Result::from(raw_broker_response).unwrap();

        assert_eq!(broker_response, SqlResponse {
            table: Some(Table {
                schema: Schema {
                    column_data_types: vec![LngT, DubT],
                    column_name_to_index: BiMap::from_iter(vec![
                        ("cnt".to_string(), 0), ("score".to_string(), 1),
                    ]),
                },
                rows: vec![TestRow { cnt: 97889, score: 232.1 }],
            }),
            stats: Some(ResponseStats {
                trace_info: Default::default(),
                num_servers_queried: 1,
                num_servers_responded: 2,
                num_segments_queried: 3,
                num_segments_processed: 4,
                num_segments_matched: 5,
                num_consuming_segments_queried: 6,
                num_docs_scanned: 7,
                num_entries_scanned_in_filter: 8,
                num_entries_scanned_post_filter: 9,
                num_groups_limit_reached: false,
                total_docs: 10,
                time_used_ms: 11,
                min_consuming_freshness_time_ms: 12,
            }),
        });
    }

    #[test]
    fn sql_response_with_deserializable_struct_converts_from_raw_broker_response_without_stats() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            cnt: i64,
            score: f64,
        }

        let raw_broker_response = RawBrokerResponseWithoutStats {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(RawTable {
                schema: RawSchema {
                    column_data_types: vec![LngT, DubT],
                    column_names: to_string_vec(vec!["cnt", "score"]),
                },
                rows: vec![vec![json!(97889), json!(232.1)]],
            }),
            exceptions: vec![],
        };
        let broker_response: SqlResponse<TestRow> = Result::from(raw_broker_response).unwrap();

        assert_eq!(broker_response, SqlResponse {
            table: Some(Table {
                schema: Schema {
                    column_data_types: vec![LngT, DubT],
                    column_name_to_index: BiMap::from_iter(vec![
                        ("cnt".to_string(), 0), ("score".to_string(), 1),
                    ]),
                },
                rows: vec![TestRow { cnt: 97889, score: 232.1 }],
            }),
            stats: None,
        });
    }

    #[test]
    fn table_get_row_count_provides_correct_number_of_rows() {
        assert_eq!(test_table().get_row_count(), 1);
    }

    #[test]
    fn table_get_row_provides_correct_row() {
        assert_eq!(test_table().get_row(0).unwrap(), &test_data_row());
    }

    #[test]
    fn table_get_row_returns_error_for_out_of_bounds() {
        match test_table().get_row(1).unwrap_err() {
            Error::InvalidResultRowIndex(index) => assert_eq!(index, 1),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn schema_get_column_count_provides_correct_number_of_columns() {
        assert_eq!(test_schema().get_column_count(), 2);
    }

    #[test]
    fn schema_get_column_name_provides_correct_name() {
        assert_eq!(test_schema().get_column_name(1).unwrap(), "cnt2");
    }

    #[test]
    fn schema_get_column_name_returns_error_for_out_of_bounds() {
        match test_schema().get_column_name(3).unwrap_err() {
            Error::InvalidResultColumnIndex(index) => assert_eq!(index, 3),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn schema_get_column_index_provides_correct_index() {
        assert_eq!(test_schema().get_column_index("cnt2").unwrap(), 1);
    }

    #[test]
    fn schema_get_column_index_returns_error_for_out_of_bounds() {
        match test_schema().get_column_index("unknown").unwrap_err() {
            Error::InvalidResultColumnName(name) => assert_eq!(name, "unknown".to_string()),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn schema_get_column_data_type_provides_correct_date_type() {
        assert_eq!(test_schema().get_column_data_type(1).unwrap(), IntT);
    }

    #[test]
    fn schema_get_column_date_type_returns_error_for_out_of_bounds() {
        match test_schema().get_column_data_type(3).unwrap_err() {
            Error::InvalidResultColumnIndex(index) => assert_eq!(index, 3),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn schema_get_column_data_type_by_name_provides_correct_date_type() {
        assert_eq!(test_schema().get_column_data_type_by_name("cnt2").unwrap(), IntT);
    }

    #[test]
    fn schema_get_column_date_type_by_name_returns_error_for_out_of_bounds() {
        match test_schema().get_column_data_type_by_name("unknown").unwrap_err() {
            Error::InvalidResultColumnName(name) => assert_eq!(name, "unknown".to_string()),
            _ => panic!("Incorrect error kind"),
        }
    }

    pub fn test_table() -> Table<DataRow> {
        Table {
            schema: test_schema(),
            rows: vec![test_data_row()],
        }
    }

    pub fn test_schema() -> Schema {
        Schema {
            column_data_types: vec![LngT, IntT],
            column_name_to_index: BiMap::from_iter(vec![
                ("cnt".to_string(), 0),
                ("cnt2".to_string(), 1),
            ]),
        }
    }
}