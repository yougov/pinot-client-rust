use std::collections::HashMap;

use bimap::BiMap;
use serde::de::Error as SerdeError;
use serde::Deserialize;
use serde_json::Value;

use crate::errors::{Error, Result};

use super::Exception;

/// BrokerResponse is the data structure for a broker response to an SQL query.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct SqlBrokerResponse<T: FromRow> {
    #[serde(default = "default_optional_result_table")]
    #[serde(rename(deserialize = "resultTable"))]
    #[serde(deserialize_with = "deserialize_optional_result_table")]
    pub result_table: Option<ResultTable<T>>,
    pub exceptions: Vec<Exception>,
    #[serde(default)]
    #[serde(rename(deserialize = "traceInfo"))]
    pub trace_info: HashMap<String, String>,
    #[serde(rename(deserialize = "numServersQueried"))]
    pub num_servers_queried: i32,
    #[serde(rename(deserialize = "numServersResponded"))]
    pub num_servers_responded: i32,
    #[serde(rename(deserialize = "numSegmentsQueried"))]
    pub num_segments_queried: i32,
    #[serde(rename(deserialize = "numSegmentsProcessed"))]
    pub num_segments_processed: i32,
    #[serde(rename(deserialize = "numSegmentsMatched"))]
    pub num_segments_matched: i32,
    #[serde(rename(deserialize = "numConsumingSegmentsQueried"))]
    pub num_consuming_segments_queried: i32,
    #[serde(rename(deserialize = "numDocsScanned"))]
    pub num_docs_scanned: i64,
    #[serde(rename(deserialize = "numEntriesScannedInFilter"))]
    pub num_entries_scanned_in_filter: i64,
    #[serde(rename(deserialize = "numEntriesScannedPostFilter"))]
    pub num_entries_scanned_post_filter: i64,
    #[serde(rename(deserialize = "numGroupsLimitReached"))]
    pub num_groups_limit_reached: bool,
    #[serde(rename(deserialize = "totalDocs"))]
    pub total_docs: i64,
    #[serde(rename(deserialize = "timeUsedMs"))]
    pub time_used_ms: i32,
    #[serde(rename(deserialize = "minConsumingFreshnessTimeMs"))]
    pub min_consuming_freshness_time_ms: i64,
}

/// Work around for https://github.com/rust-lang/rust/issues/41617
fn default_optional_result_table<T: FromRow>() -> Option<ResultTable<T>> {
    None
}

/// Work around for https://github.com/rust-lang/rust/issues/41617
fn deserialize_optional_result_table<'de, D, T: FromRow>(
    deserializer: D
) -> std::result::Result<Option<ResultTable<T>>, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    Deserialize::deserialize(deserializer)
}

/// FromRow represents any structure which can deserialize
/// the ResultTable.rows json field provided a `RespSchema`
pub trait FromRow {
    fn from_row(
        data_schema: &RespSchema,
        row: Vec<Value>,
    ) -> std::result::Result<Self, serde_json::Error> where Self: Sized;
}

/// ResultTable is the holder for SQL queries.
#[derive(Clone, Debug, PartialEq)]
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
        self.rows.get(row_index)
            .ok_or_else(|| Error::InvalidResultRowIndex(row_index))
    }
}

impl<'de, T: FromRow> Deserialize<'de> for ResultTable<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
    {
        let raw_result_table: HashMap<String, Value> = Deserialize::deserialize(deserializer)?;

        let raw_data_schema: RawRespSchema = Deserialize::deserialize(raw_result_table
            .get("dataSchema")
            .ok_or_else(|| D::Error::missing_field("dataSchema"))?)
            .map_err(D::Error::custom)?;
        let data_schema: RespSchema = raw_data_schema.into();

        let raw_rows: Vec<Vec<Value>> = Deserialize::deserialize(raw_result_table
            .get("rows")
            .ok_or_else(|| D::Error::missing_field("rows"))?)
            .map_err(D::Error::custom)?;
        let rows = raw_rows
            .into_iter()
            .map(|row| T::from_row(&data_schema, row))
            .collect::<std::result::Result<Vec<T>, serde_json::Error>>()
            .map_err(D::Error::custom)?;

        Ok(ResultTable { data_schema, rows })
    }
}

/// RespSchema is a response schema as returned by pinot
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct RawRespSchema {
    #[serde(rename(deserialize = "columnDataTypes"))]
    pub column_data_types: Vec<DataType>,
    #[serde(rename(deserialize = "columnNames"))]
    pub column_names: Vec<String>,
}

/// RespSchema is response schema with a bimap to allow easy name <-> index retrieval
#[derive(Clone, Debug, Eq, PartialEq)]
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
            .ok_or_else(|| Error::InvalidResultColumnIndex(column_index))
    }

    /// Returns column index given a column name
    pub fn get_column_index(&self, column_name: &str) -> Result<usize> {
        self.column_name_to_index.get_by_left(column_name)
            .map(|column_index| column_index.clone())
            .ok_or_else(|| Error::InvalidResultColumnName(column_name.to_string()))
    }

    /// Returns column data type given a column index
    pub fn get_column_data_type(&self, column_index: usize) -> Result<DataType> {
        self.column_data_types.get(column_index)
            .map(|column| column.clone())
            .ok_or_else(|| Error::InvalidResultColumnIndex(column_index))
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

/// Pinot native types
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataType {
    Int,
    Long,
    Float,
    Double,
    Boolean,
    Timestamp,
    String,
    Json,
    Bytes,
    IntArray,
    LongArray,
    FloatArray,
    DoubleArray,
    BooleanArray,
    TimestampArray,
    StringArray,
    BytesArray,
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
    {
        let data_type: String = Deserialize::deserialize(deserializer)?;
        let data_type = match data_type.as_str() {
            "INT" => DataType::Int,
            "LONG" => DataType::Long,
            "FLOAT" => DataType::Float,
            "DOUBLE" => DataType::Double,
            "BOOLEAN" => DataType::Boolean,
            "STRING" => DataType::String,
            "TIMESTAMP" => DataType::Timestamp,
            "JSON" => DataType::Json,
            "BYTES" => DataType::Bytes,
            "INT_ARRAY" => DataType::IntArray,
            "LONG_ARRAY" => DataType::LongArray,
            "FLOAT_ARRAY" => DataType::FloatArray,
            "DOUBLE_ARRAY" => DataType::DoubleArray,
            "BOOLEAN_ARRAY" => DataType::BooleanArray,
            "STRING_ARRAY" => DataType::StringArray,
            "TIMESTAMP_ARRAY" => DataType::TimestampArray,
            "BYTES_ARRAY" => DataType::BytesArray,
            variant => return Err(D::Error::unknown_variant(variant, &[
                "INT", "LONG", "FLOAT", "DOUBLE", "BOOLEAN", "STRING", "TIMESTAMP", "JSON", "BYTES",
            ])),
        };
        Ok(data_type)
    }
}

#[cfg(test)]
pub mod tests {
    use std::iter::FromIterator;

    use serde_json::json;

    use crate::response::data::{
        Data::Double as DubD,
        Data::Long as LngD,
        Data::String as StrD,
    };
    use crate::response::data::DataRow;
    use crate::response::data::tests::test_data_row;
    use crate::response::sql::{
        DataType::Double as DubT,
        DataType::Long as LngT,
        DataType::Int as IntT,
        DataType::String as StrT,
    };
    use crate::response::tests::{test_broker_response_error_msg, test_error_containing_broker_response};

    use super::*;

    #[test]
    fn sql_broker_response_deserializes_sql_aggregation_query_correctly() {
        let json: Value = json!({
            "resultTable": {
                "dataSchema": {
                    "columnDataTypes": ["LONG"],
                    "columnNames": ["cnt"]
                },
                "rows": [[97889]]
            },
            "exceptions": [],
            "numServersQueried": 1,
            "numServersResponded": 1,
            "numSegmentsQueried": 1,
            "numSegmentsProcessed": 1,
            "numSegmentsMatched": 1,
            "numConsumingSegmentsQueried": 0,
            "numDocsScanned": 97889,
            "numEntriesScannedInFilter": 0,
            "numEntriesScannedPostFilter": 0,
            "numGroupsLimitReached": false,
            "totalDocs": 97889,
            "timeUsedMs": 5,
            "segmentStatistics": [],
            "traceInfo": {},
            "minConsumingFreshnessTimeMs": 0
        });
        let broker_response: SqlBrokerResponse<DataRow> = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, SqlBrokerResponse {
            result_table: Some(ResultTable {
                data_schema: RespSchema {
                    column_data_types: vec![LngT],
                    column_name_to_index: BiMap::from_iter(vec![("cnt".to_string(), 0)]),
                },
                rows: vec![DataRow::new(vec![LngD(97889)])],
            }),
            exceptions: vec![],
            trace_info: Default::default(),
            num_servers_queried: 1,
            num_servers_responded: 1,
            num_segments_queried: 1,
            num_segments_processed: 1,
            num_segments_matched: 1,
            num_consuming_segments_queried: 0,
            num_docs_scanned: 97889,
            num_entries_scanned_in_filter: 0,
            num_entries_scanned_post_filter: 0,
            num_groups_limit_reached: false,
            total_docs: 97889,
            time_used_ms: 5,
            min_consuming_freshness_time_ms: 0,
        });
    }

    #[test]
    fn sql_broker_response_deserializes_aggregation_group_by_response_correctly() {
        let json: Value = json!({
            "resultTable": {
                "dataSchema": {
                    "columnDataTypes": ["STRING","LONG","DOUBLE"],
                    "columnNames":["teamID","cnt","sum_homeRuns"]
                },
                "rows": [
                    ["ANA",337,1324.0],
                    ["BL2",197,136.0],
                    ["ARI",727,2715.0],
                    ["BL1",48,24.0],
                    ["ALT",17,2.0],
                    ["ATL",1951,7312.0],
                    ["BFN",122,105.0],
                    ["BL3",36,32.0],
                    ["BFP",26,20.0],
                    ["BAL",2380,9164.0]
                ]
            },
            "exceptions": [],
            "numServersQueried": 1,
            "numServersResponded": 1,
            "numSegmentsQueried": 1,
            "numSegmentsProcessed": 1,
            "numSegmentsMatched": 1,
            "numConsumingSegmentsQueried": 0,
            "numDocsScanned": 97889,
            "numEntriesScannedInFilter": 0,
            "numEntriesScannedPostFilter": 195778,
            "numGroupsLimitReached": true,
            "totalDocs": 97889,
            "timeUsedMs": 24,
            "segmentStatistics": [],
            "traceInfo": {},
            "minConsumingFreshnessTimeMs": 0
        });
        let broker_response: SqlBrokerResponse<DataRow> = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, SqlBrokerResponse {
            result_table: Some(ResultTable {
                data_schema: RespSchema {
                    column_data_types: vec![StrT, LngT, DubT],
                    column_name_to_index: BiMap::from_iter(vec![
                        ("teamID".to_string(), 0),
                        ("cnt".to_string(), 1),
                        ("sum_homeRuns".to_string(), 2),
                    ]),
                },
                rows: vec![
                    DataRow::new(vec![StrD("ANA".to_string()), LngD(337), DubD(1324.0)]),
                    DataRow::new(vec![StrD("BL2".to_string()), LngD(197), DubD(136.0)]),
                    DataRow::new(vec![StrD("ARI".to_string()), LngD(727), DubD(2715.0)]),
                    DataRow::new(vec![StrD("BL1".to_string()), LngD(48), DubD(24.0)]),
                    DataRow::new(vec![StrD("ALT".to_string()), LngD(17), DubD(2.0)]),
                    DataRow::new(vec![StrD("ATL".to_string()), LngD(1951), DubD(7312.0)]),
                    DataRow::new(vec![StrD("BFN".to_string()), LngD(122), DubD(105.0)]),
                    DataRow::new(vec![StrD("BL3".to_string()), LngD(36), DubD(32.0)]),
                    DataRow::new(vec![StrD("BFP".to_string()), LngD(26), DubD(20.0)]),
                    DataRow::new(vec![StrD("BAL".to_string()), LngD(2380), DubD(9164.0)]),
                ],
            }),
            exceptions: vec![],
            trace_info: Default::default(),
            num_servers_queried: 1,
            num_servers_responded: 1,
            num_segments_queried: 1,
            num_segments_processed: 1,
            num_segments_matched: 1,
            num_consuming_segments_queried: 0,
            num_docs_scanned: 97889,
            num_entries_scanned_in_filter: 0,
            num_entries_scanned_post_filter: 195778,
            num_groups_limit_reached: true,
            total_docs: 97889,
            time_used_ms: 24,
            min_consuming_freshness_time_ms: 0,
        });
    }

    #[test]
    fn sql_broker_response_deserializes_exception_correctly() {
        let error_message = test_broker_response_error_msg();
        let json = test_error_containing_broker_response(&error_message);
        let broker_response: SqlBrokerResponse<DataRow> = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, SqlBrokerResponse {
            result_table: None,
            exceptions: vec![Exception {
                error_code: 200,
                message: error_message,
            }],
            trace_info: Default::default(),
            num_servers_queried: 1,
            num_servers_responded: 1,
            num_segments_queried: 12,
            num_segments_processed: 0,
            num_segments_matched: 0,
            num_consuming_segments_queried: 0,
            num_docs_scanned: 0,
            num_entries_scanned_in_filter: 0,
            num_entries_scanned_post_filter: 0,
            num_groups_limit_reached: false,
            total_docs: 97889,
            time_used_ms: 5,
            min_consuming_freshness_time_ms: 0,
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