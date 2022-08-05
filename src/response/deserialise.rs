use chrono::{DateTime, Utc};
use chrono::serde::ts_milliseconds;
use serde::{Deserialize, Deserializer};
use serde::de::{DeserializeSeed, Error as SerdeError, MapAccess, Visitor};
use serde_json::{Result, Value};

use crate::response::sql::{FromRow, RespSchema};

const NEG_INFINITY_STRINGS: [&str; 3] = ["\"-Infinity\"", "-Infinity", "-∞"];
const POS_INFINITY_STRINGS: [&str; 3] = ["\"Infinity\"", "Infinity", "∞"];

/// Converts Pinot floats into `Vec<f32>` using `deserialize_floats_from_json()`.
pub fn deserialize_floats<'de, D>(
    deserializer: D
) -> std::result::Result<Vec<f32>, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let raw_value: Value = Deserialize::deserialize(deserializer)?;
    deserialize_floats_from_json(raw_value).map_err(D::Error::custom)
}

/// Converts Pinot floats into `f32` using `deserialize_float_from_json()`.
pub fn deserialize_float<'de, D>(
    deserializer: D
) -> std::result::Result<f32, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let raw_value: Value = Deserialize::deserialize(deserializer)?;
    deserialize_float_from_json(raw_value).map_err(D::Error::custom)
}

/// Converts Pinot floats into `Vec<f32>` using `deserialize_float_from_json()`.
pub fn deserialize_floats_from_json(raw_value: Value) -> Result<Vec<f32>> {
    let raw_floats: Vec<Value> = Deserialize::deserialize(raw_value)?;
    raw_floats
        .into_iter()
        .map(deserialize_float_from_json)
        .collect()
}

/// Converts Pinot float into `f32`.
///
/// The default pinot float value may be returned as a string labelled '-Infinity'.
/// This function intercepts such cases and provides `f32::MIN`.
/// If positive infinity is encountered, `f32::MAX` is provided.
pub fn deserialize_float_from_json(raw_value: Value) -> Result<f32> {
    Ok(match raw_value {
        Value::String(string) => {
            if NEG_INFINITY_STRINGS.contains(&string.as_str()) {
                f32::MIN
            } else if POS_INFINITY_STRINGS.contains(&string.as_str()) {
                f32::MAX
            } else {
                Deserialize::deserialize(Value::String(string))?
            }
        }
        variant => Deserialize::deserialize(variant)?
    })
}

/// Converts Pinot doubles into `Vec<f64>` using `deserialize_doubles_from_json()`.
pub fn deserialize_doubles<'de, D>(
    deserializer: D
) -> std::result::Result<Vec<f64>, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let raw_value: Value = Deserialize::deserialize(deserializer)?;
    deserialize_doubles_from_json(raw_value).map_err(D::Error::custom)
}

/// Converts Pinot floats into `f64` using `deserialize_double_from_json()`.
pub fn deserialize_double<'de, D>(
    deserializer: D
) -> std::result::Result<f64, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let raw_value: Value = Deserialize::deserialize(deserializer)?;
    deserialize_double_from_json(raw_value).map_err(D::Error::custom)
}

/// Converts Pinot doubles into `Vec<f64>` using `deserialize_double_from_json()`.
pub fn deserialize_doubles_from_json(raw_value: Value) -> Result<Vec<f64>> {
    let raw_doubles: Vec<Value> = Deserialize::deserialize(raw_value)?;
    raw_doubles
        .into_iter()
        .map(deserialize_double_from_json)
        .collect()
}

/// Converts Pinot float into `f64`.
///
/// The default pinot double value may be returned as a string labelled '-Infinity'.
/// This function intercepts such cases and provides `f64::MIN`.
/// If positive infinity is encountered, `f64::MAX` is provided.
pub fn deserialize_double_from_json(raw_value: Value) -> Result<f64> {
    Ok(match raw_value {
        Value::String(string) => {
            if NEG_INFINITY_STRINGS.contains(&string.as_str()) {
                f64::MIN
            } else if POS_INFINITY_STRINGS.contains(&string.as_str()) {
                f64::MAX
            } else {
                Deserialize::deserialize(Value::String(string))?
            }
        }
        variant => Deserialize::deserialize(variant)?
    })
}

/// Converts Pinot timestamps into `Vec<DateTime<Utc>>` using `deserialize_timestamps_from_json()`.
pub fn deserialize_timestamps<'de, D>(
    deserializer: D
) -> std::result::Result<Vec<DateTime<Utc>>, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    deserialize_timestamps_from_json(Deserialize::deserialize(deserializer)?)
        .map_err(D::Error::custom)
}

/// Converts Pinot timestamps into `DateTime<Utc>` using `deserialize_timestamp_from_json()`.
pub fn deserialize_timestamp<'de, D>(
    deserializer: D
) -> std::result::Result<DateTime<Utc>, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let raw_date: Value = Deserialize::deserialize(deserializer)?;
    deserialize_timestamp_from_json(raw_date).map_err(D::Error::custom)
}

/// Converts Pinot timestamps into `Vec<DateTime<Utc>>` using `deserialize_timestamp_from_json()`.
pub fn deserialize_timestamps_from_json(
    raw_value: Value
) -> Result<Vec<DateTime<Utc>>> {
    let raw_dates: Vec<Value> = Deserialize::deserialize(raw_value)?;
    raw_dates
        .into_iter()
        .map(deserialize_timestamp_from_json)
        .collect()
}

/// Converts Pinot timestamp into `DateTime<Utc>`.
///
/// Timestamps which are returned as json strings are mapped from the pinot format
/// of "%Y-%m-%d %H:%M:%S%.f" to the rfc3339 of "%Y-%m-%d %H:%M:%S%.fz", assuming naively
/// the timezone to be UTC, which it should be given timestamps are stored as millisecond
/// epoch values.
///
/// Timestamps which are returned as json numbers are assumed to be millisecond epoch values.
pub fn deserialize_timestamp_from_json(raw_value: Value) -> Result<DateTime<Utc>> {
    match raw_value {
        Value::Number(number) => ts_milliseconds::deserialize(number),
        Value::String(string) => DateTime::deserialize(
            Value::String(append_z_to_force_timestamp_rfc3339(string))),
        variant => Deserialize::deserialize(variant),
    }
}

fn append_z_to_force_timestamp_rfc3339(mut timestamp: String) -> String {
    timestamp.push('z');
    timestamp
}

/// Converts Pinot bytes array into `Vec<Vec<u8>>` using `deserialize_bytes_array_from_json()`.
pub fn deserialize_bytes_array<'de, D>(
    deserializer: D
) -> std::result::Result<Vec<Vec<u8>>, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let raw_value: Value = Deserialize::deserialize(deserializer)?;
    deserialize_bytes_array_from_json(raw_value).map_err(D::Error::custom)
}

/// Converts Pinot bytes into `Vec<u8>` using `deserialize_bytes_from_json()`.
pub fn deserialize_bytes<'de, D>(
    deserializer: D
) -> std::result::Result<Vec<u8>, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let raw_value: Value = Deserialize::deserialize(deserializer)?;
    deserialize_bytes_from_json(raw_value).map_err(D::Error::custom)
}

/// Converts Pinot bytes array into `Vec<Vec<u8>>` using `deserialize_bytes_array_from_json()`.
pub fn deserialize_bytes_array_from_json(
    raw_data: Value
) -> Result<Vec<Vec<u8>>> {
    let raw_values: Vec<Value> = Deserialize::deserialize(raw_data)?;
    raw_values
        .into_iter()
        .map(deserialize_bytes_from_json)
        .collect()
}

/// Converts Pinot bytes into `Vec<u8>`.
///
/// Bytes values which are returned as json strings, which are assumed to be in hex string format,
/// are decoded into `Vec<u8>` and then repackaged into a json array.
pub fn deserialize_bytes_from_json(raw_value: Value) -> Result<Vec<u8>> {
    match raw_value {
        Value::String(data) => hex::decode(data).map_err(serde_json::Error::custom),
        variant => Deserialize::deserialize(variant),
    }
}

/// Deserializes json value potentially packaged into a string
/// by calling `deserialize_json_from_json()`.
pub fn deserialize_json<'de, D>(
    deserializer: D
) -> std::result::Result<Value, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let raw_value: Value = Deserialize::deserialize(deserializer)?;
    deserialize_json_from_json(raw_value).map_err(D::Error::custom)
}

/// Deserializes json value potentially packaged into a string.
/// This is because the Pinot API returns json fields as strings.
///
/// Json values which are returned as non-empty json strings are deserialized into json objects.
/// Empty strings are returned as json strings.
/// All other json types return as are.
pub fn deserialize_json_from_json(raw_value: Value) -> Result<Value> {
    match raw_value {
        Value::String(string) => if string.is_empty() {
            Ok(Value::String(string))
        } else {
            serde_json::from_str(&string)
        },
        variant => Deserialize::deserialize(variant),
    }
}

/// Implementation of `FromRow` for all implementers of `Deserialize`.
///
/// Utilizes a deserializing wrapper which defers to the Json deserializer
/// except when deserializing to a map or a structure, in which case
/// it provides the column names as keys.
impl<'de, T: Deserialize<'de>> FromRow for T {
    fn from_row(
        data_schema: &RespSchema,
        row: Vec<Value>,
    ) -> Result<Self> {
        let d = JsonRowDeserializer::new(data_schema.clone(), row);
        let a: T = T::deserialize(d)?;
        Ok(a)
    }
}

/// Define a struct/enum deserialization method which
/// defers to a deserializer accessed by a getter method
macro_rules! defer_fieldless_struct_deserialization {
    ($getter:ident, $method:ident) => {
        fn $method<V: Visitor<'de>>(self, name: &'static str, visitor: V) -> Result<V::Value> {
            self.$getter().$method(name, visitor)
        }
    };
}

/// Define a deserialization method which defers
/// to a deserializer accessed by a getter method
macro_rules! defer_base_deserialization {
    ($getter:ident, $method:ident) => {
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
            self.$getter().$method(visitor)
        }
    };
}

/// A deserializing wrapper which defers to `serde_json::Deserializer`
/// except when deserializing to a map or a structure, in which case
/// it provides the column names as keys by implementing `MapAccess`.
struct JsonRowDeserializer {
    schema: RespSchema,
    row: Vec<Value>,
    requested: usize,
}

impl JsonRowDeserializer {
    pub fn new(schema: RespSchema, row: Vec<Value>) -> Self {
        Self { schema, row, requested: 0 }
    }

    fn row_as_json_value(self) -> Value {
        Value::Array(self.row)
    }
}

impl<'de> Deserializer<'de> for JsonRowDeserializer {
    type Error = serde_json::Error;

    defer_base_deserialization!(row_as_json_value, deserialize_any);
    defer_base_deserialization!(row_as_json_value, deserialize_bool);
    defer_base_deserialization!(row_as_json_value, deserialize_i8);
    defer_base_deserialization!(row_as_json_value, deserialize_i16);
    defer_base_deserialization!(row_as_json_value, deserialize_i32);
    defer_base_deserialization!(row_as_json_value, deserialize_i64);
    defer_base_deserialization!(row_as_json_value, deserialize_u8);
    defer_base_deserialization!(row_as_json_value, deserialize_u16);
    defer_base_deserialization!(row_as_json_value, deserialize_u32);
    defer_base_deserialization!(row_as_json_value, deserialize_u64);
    defer_base_deserialization!(row_as_json_value, deserialize_f32);
    defer_base_deserialization!(row_as_json_value, deserialize_f64);
    defer_base_deserialization!(row_as_json_value, deserialize_char);
    defer_base_deserialization!(row_as_json_value, deserialize_str);
    defer_base_deserialization!(row_as_json_value, deserialize_string);
    defer_base_deserialization!(row_as_json_value, deserialize_bytes);
    defer_base_deserialization!(row_as_json_value, deserialize_byte_buf);
    defer_base_deserialization!(row_as_json_value, deserialize_option);
    defer_base_deserialization!(row_as_json_value, deserialize_unit);
    defer_fieldless_struct_deserialization!(row_as_json_value, deserialize_unit_struct);
    defer_fieldless_struct_deserialization!(row_as_json_value, deserialize_newtype_struct);
    defer_base_deserialization!(row_as_json_value, deserialize_seq);
    defer_base_deserialization!(row_as_json_value, deserialize_identifier);
    defer_base_deserialization!(row_as_json_value, deserialize_ignored_any);


    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        self.row_as_json_value().deserialize_tuple(len, visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.row_as_json_value().deserialize_tuple_struct(name, len, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_map(self)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_map(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.row_as_json_value().deserialize_enum(name, variants, visitor)
    }
}

/// Provides key information from `RespSchema`.
///
/// As the row may need to be packed up into a `Value::Array` for
/// deferred deserialization method, this implementation uses a `Vec`
/// and navigates through the row backwards rather than converting
/// it into an iterator.
impl<'de> MapAccess<'de> for JsonRowDeserializer {
    type Error = serde_json::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>> {
        self.requested += 1;
        if self.requested > self.schema.get_column_count() {
            return Ok(None);
        }
        let column_index = self.schema.get_column_count() - self.requested;
        let column_name = self.schema.get_column_name(column_index).unwrap();
        seed.deserialize(Value::String(column_name.to_string())).map(Some)
    }

    fn next_value_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<K::Value> {
        let value = self.row.pop().ok_or_else(|| serde_json::Error::invalid_length(
            self.requested, &format!("{:?}", self.schema).as_str()))?;
        seed.deserialize(value)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::iter::FromIterator;
    use serde_json::json;

    use crate::response::{
        DataType::Boolean as BooT,
        DataType::BooleanArray as BooAT,
        DataType::Bytes as BytT,
        DataType::BytesArray as BytAT,
        DataType::Double as DubT,
        DataType::DoubleArray as DubAT,
        DataType::Float as FltT,
        DataType::FloatArray as FltAT,
        DataType::Int as IntT,
        DataType::IntArray as IntAT,
        DataType::Json as JsnT,
        DataType::Long as LngT,
        DataType::LongArray as LngAT,
        DataType::String as StrT,
        DataType::StringArray as StrAT,
        DataType::Timestamp as TimT,
        DataType::TimestampArray as TimAT,
    };
    use crate::response::raw::RawRespSchema;
    use crate::tests::{date_time_utc_milli, to_string_vec};

    use super::*;

    #[test]
    fn deserialize_timestamp_handles_strings_and_epochs() {
        let datetime = deserialize_timestamp(json! {"1949-10-02 10:11:49.1234"}).unwrap();
        assert_eq!(datetime, date_time_utc_milli(1949, 10, 2, 10, 11, 49, 1234));
        let datetime = deserialize_timestamp(json! {1577875528000i64}).unwrap();
        assert_eq!(datetime, date_time_utc_milli(2020, 1, 1, 10, 45, 28, 0));
    }

    #[test]
    fn deserialize_timestamps_handles_strings_and_epochs() {
        let datetimes = deserialize_timestamps(json! {[
            "1949-10-02 10:11:49.1234", 1577875528000i64
        ]}).unwrap();
        assert_eq!(datetimes, vec![
            date_time_utc_milli(1949, 10, 2, 10, 11, 49, 1234),
            date_time_utc_milli(2020, 1, 1, 10, 45, 28, 0),
        ]);
    }

    #[test]
    fn deserialize_bytes_handles_hexstrings_and_number_arrays() {
        let bytes = deserialize_bytes(json!("ab")).unwrap();
        assert_eq!(bytes, vec![171]);
        let bytes = deserialize_bytes(json!([171])).unwrap();
        assert_eq!(bytes, vec![171]);
    }

    #[test]
    fn deserialize_bytes_array_handles_hexstrings_and_number_arrays() {
        let bytes = deserialize_bytes_array(json!(["ab", [171]])).unwrap();
        assert_eq!(bytes, vec![vec![171], vec![171]]);
    }

    #[test]
    fn deserialize_json_value_converts_only_non_empty_string_json() {
        assert_eq!(deserialize_json(json!("")).unwrap(), json!(""));
        assert_eq!(deserialize_json(json!(171)).unwrap(), json!(171));
        assert_eq!(deserialize_json(json!("171")).unwrap(), json!(171));
        assert_eq!(deserialize_json(json!([171])).unwrap(), json!([171]));
        assert_eq!(deserialize_json(json!("[171]")).unwrap(), json!([171]));
        assert_eq!(deserialize_json(json!("\"a\"")).unwrap(), json!("a"));
        assert_eq!(deserialize_json(json!({"a": "b"})).unwrap(), json!({"a": "b"}));
        assert_eq!(deserialize_json(json!("{\"a\": \"b\"}")).unwrap(), json!({"a": "b"}));
    }

    #[test]
    fn pinot_row_deserializable_to_struct_for_int_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            v_int: i32,
            v_ints: Vec<i32>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![IntT, IntAT],
            column_names: to_string_vec(vec!["v_int", "v_ints"]),
        });
        let values = vec![json!(1), json!([1, 2])];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow { v_int: 1, v_ints: vec![1, 2] });
    }

    #[test]
    fn pinot_row_deserializable_to_struct_for_long_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            v_long: i64,
            v_longs: Vec<i64>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![LngT, LngAT],
            column_names: to_string_vec(vec!["v_long", "v_longs"]),
        });
        let values = vec![json!(2), json!([2, 3])];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow { v_long: 2, v_longs: vec![2, 3] });
    }

    #[test]
    fn pinot_row_deserializable_to_struct_for_float_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            v_float: f32,
            #[serde(deserialize_with = "deserialize_float")]
            v_float_neg_inf: f32,
            #[serde(deserialize_with = "deserialize_floats")]
            v_floats: Vec<f32>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![FltT, FltT, FltAT],
            column_names: to_string_vec(vec!["v_float", "v_float_neg_inf", "v_floats"]),
        });
        let values = vec![json!(4.1), json!("-Infinity"), json!([4.1, 4.2, "-Infinity"])];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow {
            v_float: 4.1,
            v_float_neg_inf: f32::MIN,
            v_floats: vec![4.1, 4.2, f32::MIN],
        });
    }

    #[test]
    fn pinot_row_deserializable_to_struct_for_double_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            v_double: f64,
            #[serde(deserialize_with = "deserialize_double")]
            v_double_neg_inf: f64,
            #[serde(deserialize_with = "deserialize_doubles")]
            v_doubles: Vec<f64>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![DubT, DubT, DubAT],
            column_names: to_string_vec(vec!["v_double", "v_double_neg_inf", "v_doubles"]),
        });
        let values = vec![json!(5.1), json!("-Infinity"), json!([5.1, 5.2, "-Infinity"])];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow {
            v_double: 5.1,
            v_double_neg_inf: f64::MIN,
            v_doubles: vec![5.1, 5.2, f64::MIN],
        });
    }

    #[test]
    fn pinot_row_deserializable_to_struct_for_boolean_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            v_boolean: bool,
            v_booleans: Vec<bool>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![BooT, BooAT],
            column_names: to_string_vec(vec!["v_boolean", "v_booleans"]),
        });
        let values = vec![json!(true), json!([true, false])];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow { v_boolean: true, v_booleans: vec![true, false] });
    }

    #[test]
    fn pinot_row_deserializable_to_struct_for_string_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            v_string: String,
            v_strings: Vec<String>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![StrT, StrAT],
            column_names: to_string_vec(vec!["v_string", "v_strings"]),
        });
        let values = vec![json!("a"), json!(["a", "b"])];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow {
            v_string: "a".to_string(),
            v_strings: to_string_vec(vec!["a", "b"]),
        });
    }

    #[test]
    fn pinot_row_deserializable_to_struct_for_epoach_timestamp_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            #[serde(deserialize_with = "deserialize_timestamp")]
            v_timestamp: DateTime<Utc>,
            #[serde(deserialize_with = "deserialize_timestamps")]
            v_timestamps: Vec<DateTime<Utc>>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![TimT, TimAT],
            column_names: to_string_vec(vec!["v_timestamp", "v_timestamps"]),
        });
        let values = vec![
            json!(1577875528000i64), json!([1577875528000i64, 1577875528000i64]),
        ];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow {
            v_timestamp: date_time_utc_milli(2020, 1, 1, 10, 45, 28, 0),
            v_timestamps: vec![
                date_time_utc_milli(2020, 1, 1, 10, 45, 28, 0),
                date_time_utc_milli(2020, 1, 1, 10, 45, 28, 0),
            ],
        });
    }

    #[test]
    fn pinot_row_deserializable_to_struct_for_string_timestamp_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            #[serde(deserialize_with = "deserialize_timestamp")]
            v_timestamp_from_string: DateTime<Utc>,
            v_timestamp_as_string: String,
            #[serde(deserialize_with = "deserialize_timestamps")]
            v_timestamps_from_strings: Vec<DateTime<Utc>>,
            v_timestamps_as_strings: Vec<String>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![TimT, TimT, TimAT, TimAT],
            column_names: to_string_vec(vec![
                "v_timestamp_from_string", "v_timestamp_as_string",
                "v_timestamps_from_strings", "v_timestamps_as_strings",
            ]),
        });
        let values = vec![
            json!("1949-10-02 10:11:49.1234"), json!("1949-10-02 10:11:49.1234"),
            json!(["1949-10-02 10:11:49.1234", "2020-01-01 10:45:28.0"]),
            json!(["1949-10-02 10:11:49.1234", "2020-01-01 10:45:28.0"]),
        ];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow {
            v_timestamp_from_string: date_time_utc_milli(1949, 10, 2, 10, 11, 49, 1234),
            v_timestamp_as_string: "1949-10-02 10:11:49.1234".to_string(),
            v_timestamps_from_strings: vec![
                date_time_utc_milli(1949, 10, 2, 10, 11, 49, 1234),
                date_time_utc_milli(2020, 1, 1, 10, 45, 28, 0),
            ],
            v_timestamps_as_strings: to_string_vec(vec!["1949-10-02 10:11:49.1234", "2020-01-01 10:45:28.0"]),
        });
    }

    #[test]
    fn pinot_row_deserializable_to_struct_for_bytes_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            #[serde(deserialize_with = "deserialize_bytes")]
            v_bytes: Vec<u8>,
            v_hex: String,
            #[serde(deserialize_with = "deserialize_bytes_array")]
            v_bytes_arr: Vec<Vec<u8>>,
            v_hex_arr: Vec<String>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![BytT, BytT, BytAT, BytAT],
            column_names: to_string_vec(vec!["v_bytes", "v_hex", "v_bytes_arr", "v_hex_arr"]),
        });
        let values = vec![json!("ab"), json!("ab"), json!(["ab", [171]]), json!(["ab"])];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow {
            v_bytes: vec![171],
            v_hex: "ab".to_string(),
            v_bytes_arr: vec![vec![171], vec![171]],
            v_hex_arr: to_string_vec(vec!["ab"]),
        });
    }

    #[test]
    fn pinot_row_deserializable_to_struct_for_json_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            #[serde(deserialize_with = "deserialize_json")]
            v_json: Value,
            #[serde(deserialize_with = "deserialize_json")]
            v_json_from_string: Value,
            v_json_as_string: String,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![JsnT, JsnT, JsnT],
            column_names: to_string_vec(vec!["v_json", "v_json_from_string", "v_json_as_string"]),
        });
        let values = vec![json!({"a": "b"}), json!("{\"a\": \"b\"}"), json!("{\"a\": \"b\"}")];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow {
            v_json: json!({"a": "b"}),
            v_json_from_string: json!({"a": "b"}),
            v_json_as_string: "{\"a\": \"b\"}".to_string(),
        });
    }

    #[test]
    fn pinot_row_deserializable_to_fieldless_struct() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow(
            i32,
            #[serde(deserialize_with = "deserialize_json")]
            Value,
        );
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![IntT, JsnT],
            column_names: to_string_vec(vec!["v_int", "v_json"]),
        });
        let values = vec![json!(1), json!({"a": "b"})];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow(1, json!({"a": "b"})));
    }

    #[test]
    fn pinot_row_deserializable_to_map() {
        type TestRow = HashMap<String, String>;
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![IntT, StrT],
            column_names: to_string_vec(vec!["v_int", "v_string"]),
        });
        let values = vec![json!("1"), json!("name")];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow::from_iter(vec![
            ("v_int".to_string(), "1".to_string()),
            ("v_string".to_string(), "name".to_string()),
        ]));
    }

    #[test]
    fn pinot_row_deserializable_to_fieldless_tuple() {
        type TestRow = (i32, Value);
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![IntT, JsnT],
            column_names: to_string_vec(vec!["v_int", "v_json"]),
        });
        let values = vec![json!(1), json!({"a": "b"})];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, (1, json!({"a": "b"})));
    }
}