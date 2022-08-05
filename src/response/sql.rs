use bimap::BiMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::{Error, Result};
use crate::response::{DataType, ResponseStats};
use crate::response::raw::{RawBrokerResponse, RawRespSchema, RawResultTable};

use super::Exception;

/// SqlBrokerResponse is the data structure for a broker response to an SQL query.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct SqlBrokerResponse<T: FromRow> {
    pub result_table: Option<ResultTable<T>>,
    pub exceptions: Vec<Exception>,
    pub stats: ResponseStats,
}

impl<T: FromRow> From<RawBrokerResponse> for Result<SqlBrokerResponse<T>> {
    fn from(raw: RawBrokerResponse) -> Self {
        let result_table: Option<ResultTable<T>> = match raw.result_table {
            None => None,
            Some(raw) => Some(Result::from(raw)?),
        };
        Ok(SqlBrokerResponse {
            result_table,
            exceptions: raw.exceptions,
            stats: ResponseStats {
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
            },
        })
    }
}

/// FromRow represents any structure which can deserialize
/// the ResultTable.rows json field provided a `RespSchema`
pub trait FromRow: Sized {
    fn from_row(
        data_schema: &RespSchema,
        row: Vec<Value>,
    ) -> std::result::Result<Self, serde_json::Error>;
}

/// ResultTable is the holder for SQL queries.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ResultTable<T: FromRow> {
    data_schema: RespSchema,
    rows: Vec<T>,
}

impl<T: FromRow> ResultTable<T> {
    pub fn new(
        data_schema: RespSchema,
        rows: Vec<T>,
    ) -> Self {
        ResultTable { data_schema, rows }
    }

    /// Returns the schema
    pub fn get_schema(&self) -> &RespSchema {
        &self.data_schema
    }

    /// Returns how many rows in the ResultTable
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

    /// Converts result table into a `RespSchema` and rows vector
    pub fn into_schema_and_rows(self) -> (RespSchema, Vec<T>) {
        (self.data_schema, self.rows)
    }
}

impl<T: FromRow> From<RawResultTable> for Result<ResultTable<T>> {
    fn from(raw: RawResultTable) -> Self {
        let data_schema: RespSchema = raw.data_schema.into();
        let rows = raw.rows
            .into_iter()
            .map(|row| T::from_row(&data_schema, row))
            .collect::<std::result::Result<Vec<T>, serde_json::Error>>()?;
        Ok(ResultTable::new(data_schema, rows))
    }
}

/// RespSchema is response schema with a bimap to allow easy name <-> index retrieval
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RespSchema {
    column_data_types: Vec<DataType>,
    column_name_to_index: bimap::BiMap::<String, usize>,
}

impl RespSchema {
    pub fn new(
        column_data_types: Vec<DataType>,
        column_name_to_index: bimap::BiMap::<String, usize>,
    ) -> Self {
        Self { column_data_types, column_name_to_index }
    }

    /// Returns how many columns in the ResultTable
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
}

impl From<RawRespSchema> for RespSchema {
    fn from(raw_resp_schema: RawRespSchema) -> Self {
        let column_data_types = raw_resp_schema.column_data_types;
        let mut column_name_to_index: BiMap::<String, usize> = BiMap::with_capacity(
            raw_resp_schema.column_names.len());
        for (index, column_name) in raw_resp_schema.column_names.into_iter().enumerate() {
            column_name_to_index.insert(column_name, index);
        }
        RespSchema { column_data_types, column_name_to_index }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::iter::FromIterator;

    use serde::Deserialize;
    use serde_json::json;

    use crate::response::{
        DataType::Double as DubT,
        DataType::Int as IntT,
        DataType::Long as LngT,
    };
    use crate::response::data::{
        Data::Double as DubD,
        Data::Long as LngD,
    };
    use crate::response::data::DataRow;
    use crate::response::data::tests::test_data_row;
    use crate::tests::to_string_vec;

    use super::*;

    #[test]
    fn sql_broker_response_with_pinot_data_types_converts_from_raw_broker_response() {
        let raw_broker_response = RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(RawResultTable {
                data_schema: RawRespSchema {
                    column_data_types: vec![LngT, DubT],
                    column_names: to_string_vec(vec!["cnt", "score"]),
                },
                rows: vec![vec![json!(97889), json!(232.1)]],
            }),
            exceptions: vec![Exception { error_code: 0, message: "msg".to_string() }],
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
        let broker_response: SqlBrokerResponse<DataRow> = Result::from(raw_broker_response).unwrap();

        assert_eq!(broker_response, SqlBrokerResponse {
            result_table: Some(ResultTable {
                data_schema: RespSchema {
                    column_data_types: vec![LngT, DubT],
                    column_name_to_index: BiMap::from_iter(vec![
                        ("cnt".to_string(), 0), ("score".to_string(), 1),
                    ]),
                },
                rows: vec![DataRow::new(vec![LngD(97889), DubD(232.1)])],
            }),
            exceptions: vec![Exception { error_code: 0, message: "msg".to_string() }],
            stats: ResponseStats {
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
            },
        });
    }

    #[test]
    fn sql_broker_response_with_deserializable_struct_converts_from_raw_broker_response() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            cnt: i64,
            score: f64,
        }

        let raw_broker_response = RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(RawResultTable {
                data_schema: RawRespSchema {
                    column_data_types: vec![LngT, DubT],
                    column_names: to_string_vec(vec!["cnt", "score"]),
                },
                rows: vec![vec![json!(97889), json!(232.1)]],
            }),
            exceptions: vec![Exception { error_code: 0, message: "msg".to_string() }],
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
        let broker_response: SqlBrokerResponse<TestRow> = Result::from(raw_broker_response).unwrap();

        assert_eq!(broker_response, SqlBrokerResponse {
            result_table: Some(ResultTable {
                data_schema: RespSchema {
                    column_data_types: vec![LngT, DubT],
                    column_name_to_index: BiMap::from_iter(vec![
                        ("cnt".to_string(), 0), ("score".to_string(), 1),
                    ]),
                },
                rows: vec![TestRow { cnt: 97889, score: 232.1 }],
            }),
            exceptions: vec![Exception { error_code: 0, message: "msg".to_string() }],
            stats: ResponseStats {
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
            },
        });
    }

    #[test]
    fn result_table_get_row_count_provides_correct_number_of_rows() {
        assert_eq!(test_result_table().get_row_count(), 1);
    }

    #[test]
    fn test_result_table_get_row_provides_correct_row() {
        assert_eq!(test_result_table().get_row(0).unwrap(), &test_data_row());
    }

    #[test]
    fn result_table_get_row_returns_error_for_out_of_bounds() {
        match test_result_table().get_row(1).unwrap_err() {
            Error::InvalidResultRowIndex(index) => assert_eq!(index, 1),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn resp_schema_get_column_count_provides_correct_number_of_columns() {
        assert_eq!(test_resp_schema().get_column_count(), 2);
    }

    #[test]
    fn resp_schema_get_column_name_provides_correct_name() {
        assert_eq!(test_resp_schema().get_column_name(1).unwrap(), "cnt2");
    }

    #[test]
    fn resp_schema_get_column_name_returns_error_for_out_of_bounds() {
        match test_resp_schema().get_column_name(3).unwrap_err() {
            Error::InvalidResultColumnIndex(index) => assert_eq!(index, 3),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn resp_schema_get_column_index_provides_correct_index() {
        assert_eq!(test_resp_schema().get_column_index("cnt2").unwrap(), 1);
    }

    #[test]
    fn resp_schema_get_column_index_returns_error_for_out_of_bounds() {
        match test_resp_schema().get_column_index("unknown").unwrap_err() {
            Error::InvalidResultColumnName(name) => assert_eq!(name, "unknown".to_string()),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn resp_schema_get_column_data_type_provides_correct_date_type() {
        assert_eq!(test_resp_schema().get_column_data_type(1).unwrap(), IntT);
    }

    #[test]
    fn resp_schema_get_column_date_type_returns_error_for_out_of_bounds() {
        match test_resp_schema().get_column_data_type(3).unwrap_err() {
            Error::InvalidResultColumnIndex(index) => assert_eq!(index, 3),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn resp_schema_get_column_data_type_by_name_provides_correct_date_type() {
        assert_eq!(test_resp_schema().get_column_data_type_by_name("cnt2").unwrap(), IntT);
    }

    #[test]
    fn resp_schema_get_column_date_type_by_name_returns_error_for_out_of_bounds() {
        match test_resp_schema().get_column_data_type_by_name("unknown").unwrap_err() {
            Error::InvalidResultColumnName(name) => assert_eq!(name, "unknown".to_string()),
            _ => panic!("Incorrect error kind"),
        }
    }

    pub fn test_result_table() -> ResultTable<DataRow> {
        ResultTable {
            data_schema: test_resp_schema(),
            rows: vec![test_data_row()],
        }
    }

    pub fn test_resp_schema() -> RespSchema {
        RespSchema {
            column_data_types: vec![LngT, IntT],
            column_name_to_index: BiMap::from_iter(vec![
                ("cnt".to_string(), 0),
                ("cnt2".to_string(), 1),
            ]),
        }
    }
}