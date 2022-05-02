use std::collections::HashMap;

use bimap::BiMap;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use num::cast::FromPrimitive;
use serde::de::Error as SerdeError;
use serde::Deserialize;
use serde_json::Value;

use crate::errors::{Error, Result};

/// BrokerResponse is the data structure for broker response.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct BrokerResponse {
    #[serde(default)]
    #[serde(rename(deserialize = "aggregationResults"))]
    pub aggregation_results: Vec<AggregationResult>,
    #[serde(default)]
    #[serde(rename(deserialize = "selectionResults"))]
    pub selection_results: Option<SelectionResults>,
    #[serde(default)]
    #[serde(rename(deserialize = "resultTable"))]
    pub result_table: Option<ResultTable>,
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

/// AggregationResult is the data structure for PQL aggregation result
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct AggregationResult {
    pub function: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    #[serde(rename(deserialize = "traceInfo"))]
    pub group_by_columns: Vec<String>,
    #[serde(default)]
    #[serde(rename(deserialize = "traceInfo"))]
    pub group_by_result: Vec<GroupValue>,
}

/// GroupValue is the data structure for PQL aggregation GroupBy result
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct GroupValue {
    pub value: String,
    pub group: Vec<String>,
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

/// Exception is Pinot exceptions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Exception {
    #[serde(rename(deserialize = "errorCode"))]
    pub error_code: i32,
    pub message: String,
}

/// SelectionResults is the data structure for PQL selection result
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct SelectionResults {
    columns: Vec<String>,
    results: Vec<Vec<Value>>,
}

impl SelectionResults {
    pub fn new(columns: Vec<String>, results: Vec<Vec<Value>>) -> Self {
        Self { columns, results }
    }

    /// Returns how many rows in the ResultTable
    pub fn get_results_count(&self) -> usize {
        self.results.len()
    }

    /// Returns how many columns in the ResultTable
    pub fn get_column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns column name given column index
    pub fn get_column_name(&self, column_index: usize) -> Result<&str> {
        self.columns.get(column_index)
            .map(|column| column.as_str())
            .ok_or_else(|| Error::InvalidResultColumnIndex(column_index))
    }

    /// Returns a row given a row index
    pub fn get_row(&self, row_index: usize) -> Result<&Vec<Value>> {
        self.results.get(row_index)
            .ok_or_else(|| Error::InvalidResultRowIndex(row_index))
    }

    /// Returns a json `Value` entry given row index and column index
    pub fn get_data(&self, row_index: usize, column_index: usize) -> Result<&Value> {
        self.get_row(row_index)?.get(column_index)
            .ok_or_else(|| Error::InvalidResultColumnIndex(column_index))
    }
}

/// ResultTable is the holder for SQL queries.
#[derive(Clone, Debug, PartialEq)]
pub struct ResultTable {
    data_schema: RespSchema,
    rows: DataRows,
}

impl ResultTable {
    pub fn new(
        data_schema: RespSchema,
        rows: DataRows,
    ) -> Self {
        ResultTable { data_schema, rows }
    }

    /// Returns the schema
    pub fn get_schema(&self) -> &RespSchema {
        &self.data_schema
    }

    /// Returns the rows in the result table
    pub fn get_rows(&self) -> &DataRows {
        &self.rows
    }
}

impl<'de> Deserialize<'de> for ResultTable {
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
        let rows = deserialize_rows(&data_schema, raw_result_table
            .get("rows")
            .ok_or_else(|| D::Error::missing_field("rows"))?)
            .map_err(D::Error::custom)?;
        Ok(ResultTable { data_schema, rows })
    }
}

fn deserialize_rows(
    data_schema: &RespSchema,
    raw_rows: &Value,
) -> std::result::Result<DataRows, serde_json::Error> {
    let raw_rows: Vec<Vec<Value>> = Deserialize::deserialize(raw_rows)?;
    let mut rows: Vec<Vec<Data>> = Vec::with_capacity(raw_rows.len());
    for raw_row in raw_rows {
        let mut row: Vec<Data> = Vec::with_capacity(raw_row.len());
        for (column_index, raw_data) in raw_row.into_iter().enumerate() {
            let data_type = data_schema
                .column_data_types
                .get(column_index)
                .ok_or_else(|| SerdeError::custom(format!(
                    "column index of {} not found in data schema when deserializing rows",
                    column_index
                )))?;
            row.push(deserialize_data(data_type, raw_data)?);
        }
        rows.push(row);
    }
    Ok(DataRows::new(rows))
}

fn deserialize_data(
    data_type: &DataType,
    raw_data: Value,
) -> std::result::Result<Data, serde_json::Error> {
    if raw_data.is_null() {
        return Ok(Data::Null(data_type.clone()));
    }
    let data = match data_type {
        DataType::Int => Data::Int(Deserialize::deserialize(raw_data)?),
        DataType::Long => Data::Long(Deserialize::deserialize(raw_data)?),
        DataType::Float => Data::Float(Deserialize::deserialize(raw_data)?),
        DataType::Double => Data::Double(Deserialize::deserialize(raw_data)?),
        DataType::Boolean => Data::Boolean(Deserialize::deserialize(raw_data)?),
        DataType::Timestamp => Data::Timestamp(deserialize_timestamp(raw_data)?),
        DataType::String => Data::String(Deserialize::deserialize(raw_data)?),
        DataType::Json => {
            let value = deserialize_json(raw_data)?;
            if value.is_null() {
                Data::Null(DataType::Json)
            } else {
                Data::Json(value)
            }
        }
        DataType::Bytes => Data::Bytes(deserialize_bytes(raw_data)?),
        DataType::IntArray => Data::IntArray(Deserialize::deserialize(raw_data)?),
        DataType::LongArray => Data::LongArray(Deserialize::deserialize(raw_data)?),
        DataType::FloatArray => Data::FloatArray(Deserialize::deserialize(raw_data)?),
        DataType::DoubleArray => Data::DoubleArray(Deserialize::deserialize(raw_data)?),
        DataType::BooleanArray => Data::BooleanArray(Deserialize::deserialize(raw_data)?),
        DataType::TimestampArray => Data::TimestampArray(deserialize_timestamps(raw_data)?),
        DataType::StringArray => Data::StringArray(Deserialize::deserialize(raw_data)?),
        DataType::BytesArray => Data::BytesArray(deserialize_bytes_array(raw_data)?),
    };
    Ok(data)
}

fn deserialize_timestamps(
    raw_data: Value
) -> std::result::Result<Vec<DateTime<Utc>>, serde_json::Error> {
    let raw_dates: Vec<Value> = Deserialize::deserialize(raw_data)?;
    raw_dates
        .into_iter()
        .map(|raw_date| deserialize_timestamp(raw_date))
        .collect()
}

fn deserialize_timestamp(raw_data: Value) -> std::result::Result<DateTime<Utc>, serde_json::Error> {
    match raw_data {
        Value::Number(number) => {
            let epoch = Duration::milliseconds(Deserialize::deserialize(number)?);
            let secs = epoch.num_seconds();
            let nsecs_i64 = epoch.num_nanoseconds().unwrap_or(0);
            let nsecs = u32::from_i64(nsecs_i64).unwrap_or(0);
            Ok(DateTime::from_utc(NaiveDateTime::from_timestamp(secs, nsecs), Utc))
        }
        Value::String(string) => {
            Ok(DateTime::from_utc(
                NaiveDateTime::parse_from_str(&string, "%Y-%m-%d %H:%M:%S.%f")
                    .map_err(serde_json::Error::custom)?,
                Utc,
            ))
        }
        variant => Deserialize::deserialize(variant),
    }
}

fn deserialize_json(raw_data: Value) -> std::result::Result<Value, serde_json::Error> {
    match raw_data {
        Value::String(string) => {
            serde_json::from_str(&string)
        }
        variant => Ok(variant),
    }
}

fn deserialize_bytes_array(
    raw_data: Value
) -> std::result::Result<Vec<Vec<u8>>, serde_json::Error> {
    let raw_values: Vec<Value> = Deserialize::deserialize(raw_data)?;
    raw_values
        .into_iter()
        .map(|raw_value| deserialize_bytes(raw_value))
        .collect()
}

fn deserialize_bytes(raw_data: Value) -> std::result::Result<Vec<u8>, serde_json::Error> {
    match raw_data {
        Value::String(data) => hex::decode(data).map_err(serde_json::Error::custom),
        variant => Deserialize::deserialize(variant),
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

/// A matrix of `Data`
#[derive(Clone, Debug, PartialEq)]
pub struct DataRows {
    rows: Vec<Vec<Data>>,
}

impl DataRows {
    pub fn new(rows: Vec<Vec<Data>>) -> Self {
        DataRows { rows }
    }

    /// Returns how many rows in the ResultTable
    pub fn get_row_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns a row given a row index
    pub fn get_row(&self, row_index: usize) -> Result<&Vec<Data>> {
        self.rows.get(row_index)
            .ok_or_else(|| Error::InvalidResultRowIndex(row_index))
    }

    /// Returns a `Data` entry given row index and column index
    pub fn get_data(&self, row_index: usize, column_index: usize) -> Result<&Data> {
        self.get_row(row_index)?.get(column_index)
            .ok_or_else(|| Error::InvalidResultColumnIndex(column_index))
    }

    /// Convenience method that returns a int entry by calling `Data::get_int()`
    /// given row index and column index
    pub fn get_int(&self, row_index: usize, column_index: usize) -> Result<i32> {
        self.get_data(row_index, column_index)?.get_int()
    }

    /// Convenience method that returns a long entry by calling `Data::get_long()`
    /// given row index and column index
    pub fn get_long(&self, row_index: usize, column_index: usize) -> Result<i64> {
        self.get_data(row_index, column_index)?.get_long()
    }

    /// Convenience method that returns a float entry by calling `Data::get_float()`
    /// given row index and column index
    pub fn get_float(&self, row_index: usize, column_index: usize) -> Result<f32> {
        self.get_data(row_index, column_index)?.get_float()
    }

    /// Convenience method that returns a double entry by calling `Data::get_double()`
    /// given row index and column index
    pub fn get_double(&self, row_index: usize, column_index: usize) -> Result<f64> {
        self.get_data(row_index, column_index)?.get_double()
    }

    /// Convenience method that returns a boolean entry by calling `Data::get_boolean()`
    /// given row index and column index
    pub fn get_boolean(&self, row_index: usize, column_index: usize) -> Result<bool> {
        self.get_data(row_index, column_index)?.get_boolean()
    }

    /// Convenience method that returns a timestamp entry by calling `Data::get_timestamp()`
    /// given row index and column index
    pub fn get_timestamp(&self, row_index: usize, column_index: usize) -> Result<DateTime<Utc>> {
        self.get_data(row_index, column_index)?.get_timestamp()
    }

    /// Convenience method that returns a string entry by calling `Data::get_string()`
    /// given row index and column index
    pub fn get_string(&self, row_index: usize, column_index: usize) -> Result<&str> {
        self.get_data(row_index, column_index)?.get_string()
    }

    /// Convenience method that returns a json entry by calling `Data::get_json()`
    /// given row index and column index
    pub fn get_json(&self, row_index: usize, column_index: usize) -> Result<&Value> {
        self.get_data(row_index, column_index)?.get_json()
    }

    /// Convenience method that returns a bytes entry by calling `Data::get_bytes()`
    /// given row index and column index
    pub fn get_bytes(&self, row_index: usize, column_index: usize) -> Result<&Vec<u8>> {
        self.get_data(row_index, column_index)?.get_bytes()
    }

    /// Convenience method that returns a bytes entry by calling `Data::is_null()`
    /// given row index and column index
    pub fn is_null(&self, row_index: usize, column_index: usize) -> Result<bool> {
        Ok(self.get_data(row_index, column_index)?.is_null())
    }
}

/// Typed Pinot data
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Timestamp(DateTime<Utc>),
    String(String),
    Json(Value),
    Bytes(Vec<u8>),
    IntArray(Vec<i32>),
    LongArray(Vec<i64>),
    FloatArray(Vec<f32>),
    DoubleArray(Vec<f64>),
    BooleanArray(Vec<bool>),
    TimestampArray(Vec<DateTime<Utc>>),
    StringArray(Vec<String>),
    BytesArray(Vec<Vec<u8>>),
    Null(DataType),
}

impl Data {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Int(_) => DataType::Int,
            Self::Long(_) => DataType::Long,
            Self::Float(_) => DataType::Float,
            Self::Double(_) => DataType::Double,
            Self::Boolean(_) => DataType::Boolean,
            Self::Timestamp(_) => DataType::Timestamp,
            Self::String(_) => DataType::String,
            Self::Json(_) => DataType::Json,
            Self::Bytes(_) => DataType::Bytes,
            Self::IntArray(_) => DataType::IntArray,
            Self::LongArray(_) => DataType::LongArray,
            Self::FloatArray(_) => DataType::FloatArray,
            Self::DoubleArray(_) => DataType::DoubleArray,
            Self::BooleanArray(_) => DataType::BooleanArray,
            Self::TimestampArray(_) => DataType::TimestampArray,
            Self::StringArray(_) => DataType::StringArray,
            Self::BytesArray(_) => DataType::BytesArray,
            Self::Null(data_type) => data_type.clone(),
        }
    }

    /// Returns as int
    pub fn get_int(&self) -> Result<i32> {
        match self {
            Self::Int(v) => Ok(v.clone()),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Int,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as long
    pub fn get_long(&self) -> Result<i64> {
        match self {
            Self::Long(v) => Ok(v.clone()),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Long,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as float
    pub fn get_float(&self) -> Result<f32> {
        match self {
            Self::Float(v) => Ok(v.clone()),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Float,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as double
    pub fn get_double(&self) -> Result<f64> {
        match self {
            Self::Double(v) => Ok(v.clone()),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Double,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as boolean
    pub fn get_boolean(&self) -> Result<bool> {
        match self {
            Self::Boolean(v) => Ok(v.clone()),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Boolean,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as time stamp (`i64` where 0 is 1970-01-01 00:00:00...)
    pub fn get_timestamp(&self) -> Result<DateTime<Utc>> {
        match self {
            Self::Timestamp(v) => Ok(v.clone()),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Timestamp,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as string
    pub fn get_string(&self) -> Result<&str> {
        match self {
            Self::String(v) => Ok(v.as_str()),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::String,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as Json `Value`
    pub fn get_json(&self) -> Result<&Value> {
        match self {
            Self::Json(v) => Ok(v),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Json,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as bytes `Vec<u8>`
    pub fn get_bytes(&self) -> Result<&Vec<u8>> {
        match self {
            Self::Bytes(v) => Ok(v),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Bytes,
                actual: self.data_type(),
            }),
        }
    }

    /// Convenience method to determine if is of type `Data::Null`
    pub fn is_null(&self) -> bool {
        match self {
            Self::Null(_) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::iter::FromIterator;

    use serde_json::json;

    use crate::response::SelectionResults;
    use crate::tests::{date_time_utc, to_string_vec};

    use super::*;
    use super::Data::Boolean as BooD;
    use super::Data::BooleanArray as BooAD;
    use super::Data::Bytes as BytD;
    use super::Data::BytesArray as BytAD;
    use super::Data::Double as DubD;
    use super::Data::DoubleArray as DubAD;
    use super::Data::Float as FltD;
    use super::Data::FloatArray as FltAD;
    use super::Data::Int as IntD;
    use super::Data::IntArray as IntAD;
    use super::Data::Json as JsnD;
    use super::Data::Long as LngD;
    use super::Data::LongArray as LngAD;
    use super::Data::Null as NulD;
    use super::Data::String as StrD;
    use super::Data::StringArray as StrAD;
    use super::Data::Timestamp as TimD;
    use super::Data::TimestampArray as TimAD;
    use super::DataType::Boolean as BooT;
    use super::DataType::BooleanArray as BooAT;
    use super::DataType::Bytes as BytT;
    use super::DataType::BytesArray as BytAT;
    use super::DataType::Double as DubT;
    use super::DataType::DoubleArray as DubAT;
    use super::DataType::Float as FltT;
    use super::DataType::FloatArray as FltAT;
    use super::DataType::Int as IntT;
    use super::DataType::IntArray as IntAT;
    use super::DataType::Json as JsnT;
    use super::DataType::Long as LngT;
    use super::DataType::LongArray as LngAT;
    use super::DataType::String as StrT;
    use super::DataType::StringArray as StrAT;
    use super::DataType::Timestamp as TimT;
    use super::DataType::TimestampArray as TimAT;

    #[test]
    fn broker_response_deserialises_results_table_for_all_types_correctly() {
        let json: Value = json!({
            "resultTable":{
                "dataSchema":{
                    "columnDataTypes":[
                        "STRING_ARRAY","INT_ARRAY","TIMESTAMP_ARRAY","BOOLEAN_ARRAY","LONG_ARRAY",
                        "FLOAT_ARRAY","DOUBLE_ARRAY","BYTES_ARRAY","STRING","INT","LONG","FLOAT",
                        "DOUBLE","BOOLEAN","TIMESTAMP","JSON","BYTES"
                    ],
                    "columnNames":[
                        "names","gameIds","datesPlayed","gamesWon","scores","handicapAdjustedScores",
                        "handicapAdjustedScores_highPrecision","rawArray","handle","age","totalScore",
                        "avgScore","avgScore_highPrecision","hasPlayed","dateOfBirth","extra","raw"
                    ]
                },
                "rows":[
                    [
                        ["A", "1"], [1, 2], [1577875528000i64, 1580553928000i64], [true, false],
                        [3, 6], [2.1, 4.9], [2.15, 4.99], ["ab", "ab"], "A", 10, 9, 4.5, 4.55,
                        true, 1293840000000i64, {"a": "b"}, "ab"
                    ],
                    [
                        ["B", "2"], [1, 2], ["2020-01-01 10:45:28.0", "2020-02-01 10:45:28.0"],
                        [true, false], [3, 6], [2.1, 4.9], [2.15, 4.99], ["cd", "ef"], "B", 10, 9,
                        4.5, 4.55, true, "2011-01-01 00:00:00.0", "{\"a\": \"b\"}", [171]
                    ],
                    [[], [], [], [], [], [], [], [], "C", 0, 0,  0,  0, false, 0, "\"a\"", ""],
                    [[], [], [], [], [], [], [], [], "D", 0, 0,  0,  0, false, 0, "0", []],
                    [[], [], [], [], [], [], [], [], "E", 0, 0,  0,  0, false, 0, "null", []],
                    [[], [], [], [], [], [], [], [], "F", 0, 0,  0,  0, false, 0, null, []],
                    [[], [], [], [], [], [], [], [], "G", 0, 0,  0,  0, false, 0, {}, []]
                ]
            },
            "exceptions": [],
            "numServersQueried": 1,
            "numServersResponded": 1,
            "numSegmentsQueried": 1,
            "numSegmentsProcessed": 1,
            "numSegmentsMatched": 1,
            "numConsumingSegmentsQueried": 0,
            "numDocsScanned": 10,
            "numEntriesScannedInFilter": 0,
            "numEntriesScannedPostFilter": 250,
            "numGroupsLimitReached": false,
            "totalDocs": 97889,
            "timeUsedMs": 6,
            "segmentStatistics": [],
            "traceInfo": {},
            "minConsumingFreshnessTimeMs": 0
        });
        let broker_response: BrokerResponse = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, BrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(ResultTable {
                data_schema: RespSchema {
                    column_data_types: vec![
                        StrAT, IntAT, TimAT, BooAT, LngAT, FltAT, DubAT, BytAT, StrT, IntT, LngT,
                        FltT, DubT, BooT, TimT, JsnT, BytT,
                    ],
                    column_name_to_index: BiMap::from_iter(vec![
                        ("names".to_string(), 0),
                        ("gameIds".to_string(), 1),
                        ("datesPlayed".to_string(), 2),
                        ("gamesWon".to_string(), 3),
                        ("scores".to_string(), 4),
                        ("handicapAdjustedScores".to_string(), 5),
                        ("handicapAdjustedScores_highPrecision".to_string(), 6),
                        ("rawArray".to_string(), 7),
                        ("handle".to_string(), 8),
                        ("age".to_string(), 9),
                        ("totalScore".to_string(), 10),
                        ("avgScore".to_string(), 11),
                        ("avgScore_highPrecision".to_string(), 12),
                        ("hasPlayed".to_string(), 13),
                        ("dateOfBirth".to_string(), 14),
                        ("extra".to_string(), 15),
                        ("raw".to_string(), 16),
                    ]),
                },
                rows: DataRows::new(vec![
                    vec![
                        StrAD(to_string_vec(vec!["A", "1"])), IntAD(vec![1, 2]),
                        TimAD(vec![date_time_2020_01_01t10_45_28z(), date_time_2020_02_01t10_45_28z()]),
                        BooAD(vec![true, false]), LngAD(vec![3, 6]), FltAD(vec![2.1, 4.9]),
                        DubAD(vec![2.15, 4.99]), BytAD(vec![vec![171], vec![171]]), StrD("A".to_string()),
                        IntD(10), LngD(9), FltD(4.5), DubD(4.55), BooD(true),
                        TimD(date_time_2011_01_01t00_00_00z()), JsnD(json!({"a": "b"})),
                        BytD(vec![171]),
                    ],
                    vec![
                        StrAD(to_string_vec(vec!["B", "2"])), IntAD(vec![1, 2]),
                        TimAD(vec![date_time_2020_01_01t10_45_28z(), date_time_2020_02_01t10_45_28z()]),
                        BooAD(vec![true, false]), LngAD(vec![3, 6]), FltAD(vec![2.1, 4.9]),
                        DubAD(vec![2.15, 4.99]), BytAD(vec![vec![205], vec![239]]), StrD("B".to_string()),
                        IntD(10), LngD(9), FltD(4.5), DubD(4.55), BooD(true),
                        TimD(date_time_2011_01_01t00_00_00z()), JsnD(json!({"a": "b"})),
                        BytD(vec![171]),
                    ],
                    vec![
                        StrAD(vec![]), IntAD(vec![]), TimAD(vec![]), BooAD(vec![]), LngAD(vec![]),
                        FltAD(vec![]), DubAD(vec![]), BytAD(vec![]), StrD("C".to_string()),
                        IntD(0), LngD(0), FltD(0.0), DubD(0.0), BooD(false), TimD(epoch()),
                        JsnD(json!("a")), BytD(vec![]),
                    ],
                    vec![
                        StrAD(vec![]), IntAD(vec![]), TimAD(vec![]), BooAD(vec![]), LngAD(vec![]),
                        FltAD(vec![]), DubAD(vec![]), BytAD(vec![]), StrD("D".to_string()),
                        IntD(0), LngD(0), FltD(0.0), DubD(0.0), BooD(false), TimD(epoch()),
                        JsnD(json!(0)), BytD(vec![]),
                    ],
                    vec![
                        StrAD(vec![]), IntAD(vec![]), TimAD(vec![]), BooAD(vec![]), LngAD(vec![]),
                        FltAD(vec![]), DubAD(vec![]), BytAD(vec![]), StrD("E".to_string()),
                        IntD(0), LngD(0), FltD(0.0), DubD(0.0), BooD(false), TimD(epoch()),
                        NulD(JsnT), BytD(vec![]),
                    ],
                    vec![
                        StrAD(vec![]), IntAD(vec![]), TimAD(vec![]), BooAD(vec![]), LngAD(vec![]),
                        FltAD(vec![]), DubAD(vec![]), BytAD(vec![]), StrD("F".to_string()),
                        IntD(0), LngD(0), FltD(0.0), DubD(0.0), BooD(false), TimD(epoch()),
                        NulD(JsnT), BytD(vec![]),
                    ],
                    vec![
                        StrAD(vec![]), IntAD(vec![]), TimAD(vec![]), BooAD(vec![]), LngAD(vec![]),
                        FltAD(vec![]), DubAD(vec![]), BytAD(vec![]), StrD("G".to_string()),
                        IntD(0), LngD(0), FltD(0.0), DubD(0.0), BooD(false), TimD(epoch()),
                        JsnD(json!({})), BytD(vec![]),
                    ],
                ]),
            }),
            exceptions: vec![],
            trace_info: Default::default(),
            num_servers_queried: 1,
            num_servers_responded: 1,
            num_segments_queried: 1,
            num_segments_processed: 1,
            num_segments_matched: 1,
            num_consuming_segments_queried: 0,
            num_docs_scanned: 10,
            num_entries_scanned_in_filter: 0,
            num_entries_scanned_post_filter: 250,
            num_groups_limit_reached: false,
            total_docs: 97889,
            time_used_ms: 6,
            min_consuming_freshness_time_ms: 0,
        });
    }

    #[test]
    fn broker_response_deserialises_pql_aggregation_query_correctly() {
        let json: Value = json!({
            "selectionResults": {
                "columns": ["cnt", "extra"],
                "results": [[97889, json!({"a": "b"})]]
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
        let broker_response: BrokerResponse = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, BrokerResponse {
            aggregation_results: vec![],
            selection_results: Some(SelectionResults::new(
                to_string_vec(vec!["cnt", "extra"]),
                vec![vec![json!(97889), json!({"a": "b"})]],
            )),
            result_table: None,
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
    fn broker_response_deserialises_sql_aggregation_query_correctly() {
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
        let broker_response: BrokerResponse = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, BrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(ResultTable {
                data_schema: RespSchema {
                    column_data_types: vec![LngT],
                    column_name_to_index: BiMap::from_iter(vec![("cnt".to_string(), 0)]),
                },
                rows: DataRows::new(vec![vec![LngD(97889)]]),
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
    fn broker_response_deserialises_aggregation_group_by_response_correctly() {
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
        let broker_response: BrokerResponse = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, BrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(ResultTable {
                data_schema: RespSchema {
                    column_data_types: vec![StrT, LngT, DubT],
                    column_name_to_index: BiMap::from_iter(vec![
                        ("teamID".to_string(), 0),
                        ("cnt".to_string(), 1),
                        ("sum_homeRuns".to_string(), 2),
                    ]),
                },
                rows: DataRows::new(vec![
                    vec![StrD("ANA".to_string()), LngD(337), DubD(1324.0)],
                    vec![StrD("BL2".to_string()), LngD(197), DubD(136.0)],
                    vec![StrD("ARI".to_string()), LngD(727), DubD(2715.0)],
                    vec![StrD("BL1".to_string()), LngD(48), DubD(24.0)],
                    vec![StrD("ALT".to_string()), LngD(17), DubD(2.0)],
                    vec![StrD("ATL".to_string()), LngD(1951), DubD(7312.0)],
                    vec![StrD("BFN".to_string()), LngD(122), DubD(105.0)],
                    vec![StrD("BL3".to_string()), LngD(36), DubD(32.0)],
                    vec![StrD("BFP".to_string()), LngD(26), DubD(20.0)],
                    vec![StrD("BAL".to_string()), LngD(2380), DubD(9164.0)],
                ]),
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
    fn broker_response_deserialises_exception_correctly() {
        let error_message: &str = concat!(
        "QueryExecutionError:\n",
        "java.lang.NumberFormatException: For input string: \"UA\"\n",
        "\tat sun.misc.FloatingDecimal.readJavaFormatString(FloatingDecimal.java:2043)\n",
        "\tat sun.misc.FloatingDecimal.parseDouble(FloatingDecimal.java:110)\n",
        "\tat java.lang.Double.parseDouble(Double.java:538)\n",
        "\tat org.apache.pinot.core.segment.index.readers.StringDictionary.getDoubleValue(StringDictionary.java:58)\n",
        "\tat org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator.getNextBlock(DictionaryBasedAggregationOperator.java:81)\n",
        "\tat org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator.getNextBlock(DictionaryBasedAggregationOperator.java:47)\n",
        "\tat org.apache.pinot.core.operator.BaseOperator.nextBlock(BaseOperator.java:48)\n",
        "\tat org.apache.pinot.core.operator.CombineOperator$1.runJob(CombineOperator.java:102)\n",
        "\tat org.apache.pinot.core.util.trace.TraceRunnable.run(TraceRunnable.java:40)\n",
        "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\n",
        "\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)\n",
        "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\n",
        "\tat shaded.com.google.common.util.concurrent.TrustedListenableFutureTask$TrustedFutureInterruptibleTask.runInterruptibly(TrustedListenableFutureTask.java:111)\n",
        "\tat shaded.com.google.common.util.concurrent.InterruptibleTask.run(InterruptibleTask.java:58)",
        );
        let json: Value = json!({
            "resultTable": {
                "dataSchema": {
                    "columnDataTypes": ["DOUBLE"],
                    "columnNames": ["max(league)"]
                },
                "rows":[]
            },
            "exceptions": [{
                "errorCode": 200,
                "message": error_message.clone(),
            }],
            "numServersQueried": 1,
            "numServersResponded": 1,
            "numSegmentsQueried": 12
            ,"numSegmentsProcessed": 0,
            "numSegmentsMatched": 0,
            "numConsumingSegmentsQueried": 0,
            "numDocsScanned": 0,
            "numEntriesScannedInFilter": 0,
            "numEntriesScannedPostFilter": 0,
            "numGroupsLimitReached": false,
            "totalDocs": 97889,
            "timeUsedMs": 5,
            "segmentStatistics": [],
            "traceInfo": {},
            "minConsumingFreshnessTimeMs": 0
        });
        let broker_response: BrokerResponse = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, BrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(ResultTable {
                data_schema: RespSchema {
                    column_data_types: vec![DubT],
                    column_name_to_index: BiMap::from_iter(vec![("max(league)".to_string(), 0)]),
                },
                rows: DataRows::new(vec![]),
            }),
            exceptions: vec![Exception {
                error_code: 200,
                message: error_message.to_string(),
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
    fn data_maps_to_data_type() {
        assert_eq!(Data::Int(0).data_type(), DataType::Int);
        assert_eq!(Data::Long(0).data_type(), DataType::Long);
        assert_eq!(Data::Float(0.0).data_type(), DataType::Float);
        assert_eq!(Data::Double(0.0).data_type(), DataType::Double);
        assert_eq!(Data::String("".to_string()).data_type(), DataType::String);
        assert_eq!(Data::Boolean(false).data_type(), DataType::Boolean);
        assert_eq!(Data::Timestamp(epoch()).data_type(), DataType::Timestamp);
        assert_eq!(Data::Json(json!({})).data_type(), DataType::Json);
        assert_eq!(Data::Bytes(vec![]).data_type(), DataType::Bytes);
    }

    #[test]
    fn data_get_int_returns_only_for_appropriate_type() {
        assert_eq!(Data::Int(0).get_int().unwrap(), 0);
        match Data::Int(0).get_bytes().unwrap_err() {
            Error::IncorrectResultDataType { requested, actual } => {
                assert_eq!(requested, DataType::Bytes);
                assert_eq!(actual, DataType::Int);
            }
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn data_get_long_returns_only_for_appropriate_type() {
        assert_eq!(Data::Long(0).get_long().unwrap(), 0);
        match Data::Long(0).get_bytes().unwrap_err() {
            Error::IncorrectResultDataType { requested, actual } => {
                assert_eq!(requested, DataType::Bytes);
                assert_eq!(actual, DataType::Long);
            }
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn data_get_float_returns_only_for_appropriate_type() {
        assert_eq!(Data::Float(0.0).get_float().unwrap(), 0.0);
        match Data::Float(0.0).get_bytes().unwrap_err() {
            Error::IncorrectResultDataType { requested, actual } => {
                assert_eq!(requested, DataType::Bytes);
                assert_eq!(actual, DataType::Float);
            }
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn data_get_double_returns_only_for_appropriate_type() {
        assert_eq!(Data::Double(0.0).get_double().unwrap(), 0.0);
        match Data::Double(0.0).get_bytes().unwrap_err() {
            Error::IncorrectResultDataType { requested, actual } => {
                assert_eq!(requested, DataType::Bytes);
                assert_eq!(actual, DataType::Double);
            }
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn data_get_boolean_returns_only_for_appropriate_type() {
        assert_eq!(Data::Boolean(false).get_boolean().unwrap(), false);
        match Data::Boolean(false).get_bytes().unwrap_err() {
            Error::IncorrectResultDataType { requested, actual } => {
                assert_eq!(requested, DataType::Bytes);
                assert_eq!(actual, DataType::Boolean);
            }
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn data_get_string_returns_only_for_appropriate_type() {
        assert_eq!(Data::String("".to_string()).get_string().unwrap(), "".to_string());
        match Data::String("".to_string()).get_bytes().unwrap_err() {
            Error::IncorrectResultDataType { requested, actual } => {
                assert_eq!(requested, DataType::Bytes);
                assert_eq!(actual, DataType::String);
            }
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn data_get_timestamp_returns_only_for_appropriate_type() {
        assert_eq!(Data::Timestamp(epoch()).get_timestamp().unwrap(), epoch());
        match Data::Timestamp(epoch()).get_bytes().unwrap_err() {
            Error::IncorrectResultDataType { requested, actual } => {
                assert_eq!(requested, DataType::Bytes);
                assert_eq!(actual, DataType::Timestamp);
            }
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn data_get_json_returns_only_for_appropriate_type() {
        assert_eq!(Data::Json(json!({})).get_json().unwrap(), &json!({}));
        match Data::Json(json!({})).get_bytes().unwrap_err() {
            Error::IncorrectResultDataType { requested, actual } => {
                assert_eq!(requested, DataType::Bytes);
                assert_eq!(actual, DataType::Json);
            }
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn data_get_bytes_returns_only_for_appropriate_type() {
        let expected: Vec<u8> = Vec::new();
        assert_eq!(Data::Bytes(vec![]).get_bytes().unwrap(), &expected);
        match Data::Bytes(vec![]).get_string().unwrap_err() {
            Error::IncorrectResultDataType { requested, actual } => {
                assert_eq!(requested, DataType::String);
                assert_eq!(actual, DataType::Bytes);
            }
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn data_rows_is_null_returns_true_only_if_null() {
        assert!(Data::Null(DataType::Json).is_null());
        assert!(!Data::Int(0).is_null());
    }


    #[test]
    fn selection_results_get_row_count_provides_correct_number_of_rows() {
        assert_eq!(test_selection_results().get_results_count(), 1);
    }

    #[test]
    fn selection_results_get_column_count_provides_correct_number_of_columns() {
        assert_eq!(test_selection_results().get_column_count(), 2);
    }

    #[test]
    fn selection_results_get_column_name_provides_correct_name() {
        assert_eq!(test_selection_results().get_column_name(1).unwrap(), "extra");
    }

    #[test]
    fn selection_results_get_column_name_returns_error_for_out_of_bounds() {
        match test_selection_results().get_column_name(3).unwrap_err() {
            Error::InvalidResultColumnIndex(index) => assert_eq!(index, 3),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn selection_results_get_row_provides_correct_row() {
        assert_eq!(
            test_selection_results().get_row(0).unwrap(),
            &vec![json!(48547), json!({"a": "b"})]
        );
    }

    #[test]
    fn selection_results_get_row_returns_error_for_out_of_bounds() {
        match test_selection_results().get_row(1).unwrap_err() {
            Error::InvalidResultRowIndex(index) => assert_eq!(index, 1),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn selection_results_get_data_returns_error_for_out_of_bounds() {
        match test_selection_results().get_data(1, 0).unwrap_err() {
            Error::InvalidResultRowIndex(index) => assert_eq!(index, 1),
            _ => panic!("Incorrect error kind"),
        }
        match test_selection_results().get_data(0, 2).unwrap_err() {
            Error::InvalidResultColumnIndex(index) => assert_eq!(index, 2),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn selection_results_get_data_provides_correct_data() {
        assert_eq!(test_selection_results().get_data(0, 0).unwrap(), &json!(48547));
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

    #[test]
    fn data_rows_get_row_count_provides_correct_number_of_rows() {
        assert_eq!(test_data_rows().get_row_count(), 1);
    }

    #[test]
    fn data_rows_get_row_provides_correct_row() {
        assert_eq!(test_data_rows().get_row(0).unwrap(), &vec![LngD(97889), IntD(0)]);
    }

    #[test]
    fn data_rows_get_row_returns_error_for_out_of_bounds() {
        match test_data_rows().get_row(1).unwrap_err() {
            Error::InvalidResultRowIndex(index) => assert_eq!(index, 1),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn data_rows_get_data_provides_correct_data() {
        assert_eq!(test_data_rows().get_data(0, 1).unwrap(), &IntD(0));
    }

    #[test]
    fn data_rows_get_data_returns_error_for_out_of_bounds() {
        match test_data_rows().get_data(1, 0).unwrap_err() {
            Error::InvalidResultRowIndex(index) => assert_eq!(index, 1),
            _ => panic!("Incorrect error kind"),
        }
        match test_data_rows().get_data(0, 2).unwrap_err() {
            Error::InvalidResultColumnIndex(index) => assert_eq!(index, 2),
            _ => panic!("Incorrect error kind"),
        }
    }

    pub fn test_broker_response_json() -> Value {
        json!({
            "resultTable": {
                "dataSchema": {
                    "columnDataTypes": ["LONG", "INT"],
                    "columnNames": ["cnt", "cnt2"]
                },
                "rows": [[97889, 0]]
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
        })
    }

    pub fn test_broker_response() -> BrokerResponse {
        BrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(test_result_table()),
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
        }
    }

    pub fn test_selection_results() -> SelectionResults {
        SelectionResults::new(
            to_string_vec(vec!["cnt", "extra"]),
            vec![vec![json!(48547), json!({"a": "b"})]],
        )
    }

    pub fn test_result_table() -> ResultTable {
        ResultTable {
            data_schema: test_resp_schema(),
            rows: test_data_rows(),
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

    pub fn test_data_rows() -> DataRows {
        DataRows::new(vec![vec![LngD(97889), IntD(0)]])
    }

    fn date_time_2020_01_01t10_45_28z() -> DateTime<Utc> {
        date_time_utc(2020, 1, 1, 10, 45, 28)
    }

    fn date_time_2020_02_01t10_45_28z() -> DateTime<Utc> {
        date_time_utc(2020, 2, 1, 10, 45, 28)
    }

    fn date_time_2011_01_01t00_00_00z() -> DateTime<Utc> {
        date_time_utc(2011, 1, 1, 0, 0, 0)
    }

    fn epoch() -> DateTime<Utc> {
        date_time_utc(1970, 1, 1, 0, 0, 0)
    }
}