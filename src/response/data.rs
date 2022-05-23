use chrono::{DateTime, Utc};
use serde::de::Error as SerdeError;
use serde::Deserialize;
use serde_json::Value;

use crate::errors::{Error, Result};
use crate::response::DataType;
use crate::response::deserialise::{
    deserialize_bytes_array_from_json,
    deserialize_bytes_from_json,
    deserialize_double_from_json,
    deserialize_doubles_from_json,
    deserialize_float_from_json,
    deserialize_floats_from_json,
    deserialize_json_from_json,
    deserialize_timestamp_from_json,
    deserialize_timestamps_from_json,
};

use super::sql::{FromRow, RespSchema};

/// A row of `Data`
#[derive(Clone, Debug, PartialEq)]
pub struct DataRow {
    row: Vec<Data>,
}

impl DataRow {
    pub fn new(row: Vec<Data>) -> Self {
        Self { row }
    }

    /// Returns a `Data` entry given a column index
    pub fn get_data(&self, column_index: usize) -> Result<&Data> {
        self.row.get(column_index).ok_or(Error::InvalidResultColumnIndex(column_index))
    }

    /// Convenience method that returns a int entry by calling `Data::get_int()`
    /// given a column index
    pub fn get_int(&self, column_index: usize) -> Result<i32> {
        self.get_data(column_index)?.get_int()
    }

    /// Convenience method that returns a long entry by calling `Data::get_long()`
    /// given a column index
    pub fn get_long(&self, column_index: usize) -> Result<i64> {
        self.get_data(column_index)?.get_long()
    }

    /// Convenience method that returns a float entry by calling `Data::get_float()`
    /// given a column index
    pub fn get_float(&self, column_index: usize) -> Result<f32> {
        self.get_data(column_index)?.get_float()
    }

    /// Convenience method that returns a double entry by calling `Data::get_double()`
    /// given a column index
    pub fn get_double(&self, column_index: usize) -> Result<f64> {
        self.get_data(column_index)?.get_double()
    }

    /// Convenience method that returns a boolean entry by calling `Data::get_boolean()`
    /// given a column index
    pub fn get_boolean(&self, column_index: usize) -> Result<bool> {
        self.get_data(column_index)?.get_boolean()
    }

    /// Convenience method that returns a timestamp entry by calling `Data::get_timestamp()`
    /// given a column index
    pub fn get_timestamp(&self, column_index: usize) -> Result<DateTime<Utc>> {
        self.get_data(column_index)?.get_timestamp()
    }

    /// Convenience method that returns a string entry by calling `Data::get_string()`
    /// given a column index
    pub fn get_string(&self, column_index: usize) -> Result<&str> {
        self.get_data(column_index)?.get_string()
    }

    /// Convenience method that returns a json entry by calling `Data::get_json()`
    /// given a column index
    pub fn get_json(&self, column_index: usize) -> Result<&Value> {
        self.get_data(column_index)?.get_json()
    }

    /// Convenience method that returns a bytes entry by calling `Data::get_bytes()`
    /// given a column index
    pub fn get_bytes(&self, column_index: usize) -> Result<&Vec<u8>> {
        self.get_data(column_index)?.get_bytes()
    }

    /// Convenience method that returns a bytes entry by calling `Data::is_null()`
    /// given a column index
    pub fn is_null(&self, column_index: usize) -> Result<bool> {
        Ok(self.get_data(column_index)?.is_null())
    }
}

impl FromRow for DataRow {
    fn from_row(
        data_schema: &RespSchema,
        row: Vec<Value>,
    ) -> std::result::Result<Self, serde_json::Error> {
        deserialize_row(data_schema, row)
    }
}

fn deserialize_row(
    data_schema: &RespSchema,
    row: Vec<Value>,
) -> std::result::Result<DataRow, serde_json::Error> {
    let row = row
        .into_iter()
        .enumerate()
        .map(|(column_index, raw_data)| {
            let data_type = data_schema
                .get_column_data_type(column_index)
                .map_err(|_| SerdeError::custom(format!(
                    "column index of {} not found in data schema when deserializing rows",
                    column_index
                )))?;
            deserialize_data(&data_type, raw_data)
                .map_err(|e| SerdeError::custom(format!(
                    "Issue encountered when deserializing value with column index {}: {}",
                    column_index, e
                )))
        })
        .collect::<std::result::Result<Vec<Data>, serde_json::Error>>()?;
    Ok(DataRow::new(row))
}

fn deserialize_data(
    data_type: &DataType,
    raw_data: Value,
) -> std::result::Result<Data, serde_json::Error> {
    if raw_data.is_null() {
        return Ok(Data::Null(*data_type));
    }
    let data = match data_type {
        DataType::Int => Data::Int(Deserialize::deserialize(raw_data)?),
        DataType::Long => Data::Long(Deserialize::deserialize(raw_data)?),
        DataType::Float => Data::Float(deserialize_float_from_json(raw_data)?),
        DataType::Double => Data::Double(deserialize_double_from_json(raw_data)?),
        DataType::Boolean => Data::Boolean(Deserialize::deserialize(raw_data)?),
        DataType::Timestamp => Data::Timestamp(deserialize_timestamp_from_json(raw_data)?),
        DataType::String => Data::String(Deserialize::deserialize(raw_data)?),
        DataType::Bytes => Data::Bytes(deserialize_bytes_from_json(raw_data)?),
        DataType::IntArray => Data::IntArray(Deserialize::deserialize(raw_data)?),
        DataType::LongArray => Data::LongArray(Deserialize::deserialize(raw_data)?),
        DataType::FloatArray => Data::FloatArray(deserialize_floats_from_json(raw_data)?),
        DataType::DoubleArray => Data::DoubleArray(deserialize_doubles_from_json(raw_data)?),
        DataType::BooleanArray => Data::BooleanArray(Deserialize::deserialize(raw_data)?),
        DataType::TimestampArray => Data::TimestampArray(deserialize_timestamps_from_json(raw_data)?),
        DataType::StringArray => Data::StringArray(Deserialize::deserialize(raw_data)?),
        DataType::BytesArray => Data::BytesArray(deserialize_bytes_array_from_json(raw_data)?),
        DataType::Json => {
            let value = deserialize_json_from_json(raw_data)?;
            if value.is_null() {
                Data::Null(DataType::Json)
            } else {
                Data::Json(value)
            }
        }
    };
    Ok(data)
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
            Self::Null(data_type) => *data_type,
        }
    }

    /// Returns as int
    pub fn get_int(&self) -> Result<i32> {
        match self {
            Self::Int(v) => Ok(*v),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Int,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as long
    pub fn get_long(&self) -> Result<i64> {
        match self {
            Self::Long(v) => Ok(*v),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Long,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as float
    pub fn get_float(&self) -> Result<f32> {
        match self {
            Self::Float(v) => Ok(*v),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Float,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as double
    pub fn get_double(&self) -> Result<f64> {
        match self {
            Self::Double(v) => Ok(*v),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Double,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as boolean
    pub fn get_boolean(&self) -> Result<bool> {
        match self {
            Self::Boolean(v) => Ok(*v),
            _ => Err(Error::IncorrectResultDataType {
                requested: DataType::Boolean,
                actual: self.data_type(),
            }),
        }
    }

    /// Returns as time stamp (`i64` where 0 is 1970-01-01 00:00:00...)
    pub fn get_timestamp(&self) -> Result<DateTime<Utc>> {
        match self {
            Self::Timestamp(v) => Ok(*v),
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
        matches!(self, Self::Null(_))
    }
}

#[cfg(test)]
pub mod tests {
    use std::iter::FromIterator;

    use bimap::BiMap;
    use serde_json::json;

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
    fn data_row_deserializes_for_string() {
        let data_schema: RespSchema = RespSchema::new(
            vec![StrAT, StrT, StrT],
            BiMap::from_iter(vec![
                ("names".to_string(), 0),
                ("handle".to_string(), 1),
                ("null".to_string(), 2),
            ]),
        );
        let row: Vec<Value> = Deserialize::deserialize(json! {[["A", "1"], "B", null]}).unwrap();
        let data_row = DataRow::from_row(&data_schema, row).unwrap();
        assert_eq!(data_row, DataRow::new(vec![
            StrAD(to_string_vec(vec!["A", "1"])), StrD("B".to_string()), NulD(StrT),
        ]));
    }

    #[test]
    fn data_row_deserializes_for_int() {
        let data_schema: RespSchema = RespSchema::new(
            vec![IntAT, IntT, IntT],
            BiMap::from_iter(vec![
                ("scores".to_string(), 0),
                ("total".to_string(), 1),
                ("null".to_string(), 2),
            ]),
        );
        let row: Vec<Value> = Deserialize::deserialize(json! {[[1, 2], 3, null]}).unwrap();
        let data_row = DataRow::from_row(&data_schema, row).unwrap();
        assert_eq!(data_row, DataRow::new(vec![IntAD(vec![1, 2]), IntD(3), NulD(IntT)]));
    }

    #[test]
    fn data_row_deserializes_for_long() {
        let data_schema: RespSchema = RespSchema::new(
            vec![LngAT, LngT, LngT],
            BiMap::from_iter(vec![
                ("scores".to_string(), 0),
                ("total".to_string(), 1),
                ("null".to_string(), 2),
            ]),
        );
        let row: Vec<Value> = Deserialize::deserialize(json! {[[1, 2], 3, null]}).unwrap();
        let data_row = DataRow::from_row(&data_schema, row).unwrap();
        assert_eq!(data_row, DataRow::new(vec![LngAD(vec![1, 2]), LngD(3), NulD(LngT)]));
    }

    #[test]
    fn data_row_deserializes_for_float() {
        let data_schema: RespSchema = RespSchema::new(
            vec![FltAT, FltT, FltT, FltT],
            BiMap::from_iter(vec![
                ("scores".to_string(), 0),
                ("total".to_string(), 1),
                ("neg_infinity".to_string(), 2),
                ("null".to_string(), 3),
            ]),
        );
        let row: Vec<Value> = Deserialize::deserialize(json! {[
            [1.1, 2.2, "-Infinity"], 3.3, "-Infinity", null
        ]}).unwrap();
        let data_row = DataRow::from_row(&data_schema, row).unwrap();
        assert_eq!(data_row, DataRow::new(vec![
            FltAD(vec![1.1, 2.2, f32::MIN]), FltD(3.3), FltD(f32::MIN), NulD(FltT),
        ]));
    }

    #[test]
    fn data_row_deserializes_for_double() {
        let data_schema: RespSchema = RespSchema::new(
            vec![DubAT, DubT, DubT, DubT],
            BiMap::from_iter(vec![
                ("scores".to_string(), 0),
                ("total".to_string(), 1),
                ("neg_infinity".to_string(), 2),
                ("null".to_string(), 3),
            ]),
        );
        let row: Vec<Value> = Deserialize::deserialize(json! {
            [[1.1, 2.2, "-Infinity"], 3.3, "-Infinity", null]
        }).unwrap();
        let data_row = DataRow::from_row(&data_schema, row).unwrap();
        assert_eq!(data_row, DataRow::new(vec![
            DubAD(vec![1.1, 2.2, f64::MIN]), DubD(3.3), DubD(f64::MIN), NulD(DubT),
        ]));
    }

    #[test]
    fn data_row_deserializes_for_boolean() {
        let data_schema: RespSchema = RespSchema::new(
            vec![BooAT, BooT, BooT, BooT],
            BiMap::from_iter(vec![
                ("gamesPlayed".to_string(), 0),
                ("hasPlayed".to_string(), 1),
                ("hasWon".to_string(), 2),
                ("null".to_string(), 3),
            ]),
        );
        let row: Vec<Value> = Deserialize::deserialize(json! {[
            [true, false], true, false, null
        ]}).unwrap();
        let data_row = DataRow::from_row(&data_schema, row).unwrap();
        assert_eq!(data_row, DataRow::new(vec![
            BooAD(vec![true, false]), BooD(true), BooD(false), NulD(BooT),
        ]));
    }

    #[test]
    fn data_row_deserializes_for_timestamp() {
        let data_schema: RespSchema = RespSchema::new(
            vec![TimAT, TimT, TimT, TimT],
            BiMap::from_iter(vec![
                ("datesPlayed".to_string(), 0),
                ("dateOfBirth".to_string(), 1),
                ("lastPlayed".to_string(), 2),
                ("null".to_string(), 3),
            ]),
        );
        let row: Vec<Value> = Deserialize::deserialize(json! {[
            ["2020-01-01 10:45:28.0", 1577875528000i64, "2020-02-01 10:45:28.0", 1580553928000i64],
            "2011-01-01 00:00:00.0", 1293840000000i64, null
        ]}).unwrap();
        let data_row = DataRow::from_row(&data_schema, row).unwrap();

        assert_eq!(data_row, DataRow::new(vec![
            TimAD(vec![date_time_2020_01_01t10_45_28z(), date_time_2020_01_01t10_45_28z(),
                       date_time_2020_02_01t10_45_28z(), date_time_2020_02_01t10_45_28z()]),
            TimD(date_time_2011_01_01t00_00_00z()), TimD(date_time_2011_01_01t00_00_00z()),
            NulD(TimT),
        ]));
    }

    #[test]
    fn data_row_deserializes_for_bytes() {
        let data_schema: RespSchema = RespSchema::new(
            vec![BytAT, BytT, BytT, BytT],
            BiMap::from_iter(vec![
                ("rawArray".to_string(), 0),
                ("rawScore".to_string(), 1),
                ("rawStats".to_string(), 2),
                ("null".to_string(), 3),
            ]),
        );
        let row: Vec<Value> = Deserialize::deserialize(json! {[
            ["ab", [171]], "", [], null
        ]}).unwrap();
        let data_row = DataRow::from_row(&data_schema, row).unwrap();
        assert_eq!(data_row, DataRow::new(vec![
            BytAD(vec![vec![171], vec![171]]), BytD(vec![]), BytD(vec![]), NulD(BytT),
        ]));
    }

    #[test]
    fn data_row_deserializes_for_json() {
        let data_schema: RespSchema = RespSchema::new(
            vec![
                JsnT, JsnT, JsnT, JsnT, JsnT, JsnT, JsnT,
                JsnT, JsnT, JsnT, JsnT, JsnT, JsnT, JsnT,
            ],
            BiMap::from_iter(vec![
                ("objectLiteral".to_string(), 0),
                ("objectLiteralEmpty".to_string(), 1),
                ("objectString".to_string(), 2),
                ("objectStringEmpty".to_string(), 3),
                ("listLiteral".to_string(), 4),
                ("listLiteralEmpty".to_string(), 5),
                ("listString".to_string(), 6),
                ("listStringEmpty".to_string(), 7),
                ("string".to_string(), 8),
                ("stringEmpty".to_string(), 9),
                ("number".to_string(), 10),
                ("numberString".to_string(), 11),
                ("nullLiteral".to_string(), 12),
                ("nullString".to_string(), 13),
            ]),
        );
        let row: Vec<Value> = Deserialize::deserialize(json! {[
            json!({"a": "b"}), json!({}), json!("{\"a\": \"b\"}"),json!("{}"),
            json!([1, 2]), json!([]), json!("[1, 2]"), json!("[]"),
            json!("\"a\""), json!(""),
            json!(0), json!("0"),
            json!(null), json!("null"),
        ]}).unwrap();
        let data_row = DataRow::from_row(&data_schema, row).unwrap();
        assert_eq!(data_row, DataRow::new(vec![
            JsnD(json!({"a": "b"})), JsnD(json!({})), JsnD(json!({"a": "b"})), JsnD(json!({})),
            JsnD(json!([1, 2])), JsnD(json!([])), JsnD(json!([1, 2])), JsnD(json!([])),
            JsnD(Value::String("a".to_string())), JsnD(Value::String("".to_string())),
            JsnD(Value::Number(serde_json::Number::from(0))),
            JsnD(Value::Number(serde_json::Number::from(0))),
            NulD(JsnT), NulD(JsnT),
        ]));
    }

    #[test]
    fn data_rows_get_data_provides_correct_data() {
        assert_eq!(test_data_row().get_data(1).unwrap(), &IntD(0));
    }

    #[test]
    fn data_rows_get_data_returns_error_for_out_of_bounds() {
        match test_data_row().get_data(2).unwrap_err() {
            Error::InvalidResultColumnIndex(index) => assert_eq!(index, 2),
            _ => panic!("Incorrect error kind"),
        }
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
    fn data_is_null_returns_true_only_if_null() {
        assert!(Data::Null(DataType::Json).is_null());
        assert!(!Data::Int(0).is_null());
    }

    pub fn test_data_row() -> DataRow {
        DataRow::new(vec![LngD(97889), IntD(0)])
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