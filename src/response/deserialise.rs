use chrono::{DateTime, Utc};
use chrono::serde::ts_milliseconds;
use serde::de::Error as SerdeError;
use serde::Deserialize;
use serde_json::{Number, Result, Value};

use crate::response::DataType;
use crate::response::sql::{FromRow, RespSchema};

const NEG_INFINITY_STRINGS: [&'static str; 3] = ["\"-Infinity\"", "-Infinity", "-∞"];
const POS_INFINITY_STRINGS: [&'static str; 3] = ["\"Infinity\"", "Infinity", "∞"];

/// Takes a json value representing a colum value from Pinot,
/// along with its Pinot type, and makes the following transformations
/// to allow smoother deserialization with `serde:de`.
///
/// Timestamps which are returned as json strings are mapped from the pinot format
/// of "%Y-%m-%d %H:%M:%S%.f" to the rfc3339 of "%Y-%m-%d %H:%M:%S%.fz", assuming naively
/// the timezone to be UTC, which it should be given timestamps are stored as millisecond
/// epoch values.
///
/// Bytes values which are returned as json strings, which are assumed to be in hex string format,
/// are decoded into `Vec<u8>` and then repackaged into a json array.
///
/// Json values which are returned as non-empty json strings are deserialized into json objects.
pub fn sanitize_json_value(
    data_type: &DataType,
    raw_value: Value,
) -> Result<Value> {
    if raw_value.is_null() {
        return Ok(raw_value);
    }
    let value = match data_type {
        DataType::Timestamp => format_string_timestamp_rfc3339(raw_value),
        DataType::TimestampArray => format_string_timestamps_rfc3339(raw_value)?,
        DataType::Bytes => decode_and_repack_hex_string(raw_value)?,
        DataType::BytesArray => decode_and_repack_hex_strings(raw_value)?,
        DataType::Json => deserialize_json_from_json(raw_value)?,
        _ => raw_value,
    };
    Ok(value)
}

/// Converts Pinot floats into `Vec<f32>` using `deserialize_float_from_json()`.
pub fn deserialize_floats_from_json(raw_value: Value) -> Result<Vec<f32>> {
    let raw_floats: Vec<Value> = Deserialize::deserialize(raw_value)?;
    raw_floats
        .into_iter()
        .map(|raw_float| deserialize_float_from_json(raw_float))
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

/// Converts Pinot doubles into `Vec<f64>` using `deserialize_double_from_json()`.
pub fn deserialize_doubles_from_json(raw_value: Value) -> Result<Vec<f64>> {
    let raw_doubles: Vec<Value> = Deserialize::deserialize(raw_value)?;
    raw_doubles
        .into_iter()
        .map(|raw_double| deserialize_double_from_json(raw_double))
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
        .map(|raw_date| deserialize_timestamp_from_json(raw_date))
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
            format_string_timestamp_rfc3339(Value::String(string))),
        variant => Deserialize::deserialize(variant),
    }
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
        .map(|raw_value| deserialize_bytes_from_json(raw_value))
        .collect()
}

/// Converts Pinot bytes into `Vec<u8>`.
///
/// Bytes values which are returned as json strings, which are assumed to be in hex string format,
/// are decoded into `Vec<u8>` and then repackaged into a json array.
pub fn deserialize_bytes_from_json(raw_value: Value) -> Result<Vec<u8>> {
    match raw_value {
        Value::String(data) => decode_hex_string(data),
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

/// Cheap and probably inefficient means of arbitrary
/// struct deserialization by making an intermediate
/// json map for a row.
impl<'de, T: Deserialize<'de>> FromRow for T {
    fn from_row(
        data_schema: &RespSchema,
        row: Vec<Value>,
    ) -> Result<Self> {
        let row_map = vec_row_to_json_map(data_schema, row)?;
        Deserialize::deserialize(row_map)
    }
}

fn vec_row_to_json_map(
    data_schema: &RespSchema,
    vec_row: Vec<Value>,
) -> Result<Value> {
    let mut map_row: serde_json::Map<String, Value> = serde_json::Map::with_capacity(vec_row.len());
    for (column_index, value) in vec_row.into_iter().enumerate() {
        let column_name = data_schema.get_column_name(column_index)
            .map_err(|_| serde_json::Error::custom(format!(
                "Column index of {} not found in data schema when deserializing rows",
                column_index
            )))?;
        map_row.insert(column_name.to_string(), value);
    }
    Ok(Value::Object(map_row))
}

fn format_string_timestamps_rfc3339(raw_value: Value) -> Result<Value> {
    let raw_timestamps: Vec<Value> = Deserialize::deserialize(raw_value)?;
    let timestamps: Vec<Value> = raw_timestamps
        .into_iter()
        .map(|timestamp| format_string_timestamp_rfc3339(timestamp))
        .collect();
    Ok(Value::Array(timestamps))
}

fn format_string_timestamp_rfc3339(raw_value: Value) -> Value {
    match raw_value {
        Value::String(string) => Value::String(
            append_z_to_force_timestamp_rfc3339(string)),
        variant => variant,
    }
}

fn append_z_to_force_timestamp_rfc3339(mut timestamp: String) -> String {
    timestamp.push_str("z");
    timestamp
}

fn decode_and_repack_hex_strings(
    raw_value: Value
) -> Result<Value> {
    let raw_bytes_array: Vec<Value> = Deserialize::deserialize(raw_value)?;
    let bytes_array: Vec<Value> = raw_bytes_array
        .into_iter()
        .map(|raw_byte_array| decode_and_repack_hex_string(raw_byte_array))
        .collect::<Result<Vec<Value>>>()?;
    Ok(Value::Array(bytes_array))
}

fn decode_and_repack_hex_string(raw_value: Value) -> Result<Value> {
    Ok(match raw_value {
        Value::String(hex) => repack_bytes_into_json(decode_hex_string(hex)?),
        variant => variant,
    })
}

fn repack_bytes_into_json(bytes: Vec<u8>) -> Value {
    let json_bytes: Vec<Value> = bytes
        .into_iter()
        .map(|b| Value::Number(Number::from(b)))
        .collect();
    Value::Array(json_bytes)
}

fn decode_hex_string(data: String) -> Result<Vec<u8>> {
    hex::decode(data).map_err(serde_json::Error::custom)
}

#[cfg(test)]
pub mod tests {
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
    fn sanitize_json_value_returns_string_unmodified() {
        assert_eq!(sanitize_json_value(&StrT, json!("a")).unwrap(), json!("a"));
        assert_eq!(sanitize_json_value(&StrAT, json!(["a", "b"])).unwrap(), json!(["a", "b"]));
    }

    #[test]
    fn sanitize_json_value_returns_int_unmodified() {
        assert_eq!(sanitize_json_value(&IntT, json!(1)).unwrap(), json!(1));
        assert_eq!(sanitize_json_value(&IntAT, json!([1, 1])).unwrap(), json!([1, 1]));
    }

    #[test]
    fn sanitize_json_value_returns_long_unmodified() {
        assert_eq!(sanitize_json_value(&LngT, json!(1)).unwrap(), json!(1));
        assert_eq!(sanitize_json_value(&LngAT, json!([1, 1])).unwrap(), json!([1, 1]));
    }

    #[test]
    fn sanitize_json_value_returns_float_unmodified() {
        assert_eq!(sanitize_json_value(&FltT, json!(1.1)).unwrap(), json!(1.1));
        assert_eq!(sanitize_json_value(&FltAT, json!([1.1, 1.1])).unwrap(), json!([1.1, 1.1]));
    }

    #[test]
    fn sanitize_json_value_returns_double_unmodified() {
        assert_eq!(sanitize_json_value(&DubT, json!(1.1)).unwrap(), json!(1.1));
        assert_eq!(sanitize_json_value(&DubAT, json!([1.1, 1.1])).unwrap(), json!([1.1, 1.1]));
    }

    #[test]
    fn sanitize_json_value_returns_boolean_unmodified() {
        assert_eq!(sanitize_json_value(&BooT, json!(true)).unwrap(), json!(true));
        assert_eq!(sanitize_json_value(&BooAT, json!([true, false])).unwrap(), json!([true, false]));
    }

    #[test]
    fn sanitize_json_value_converts_only_string_timestamps() {
        assert_eq!(sanitize_json_value(&TimT, json!(123124)).unwrap(), json!(123124));
        assert_eq!(
            sanitize_json_value(&TimT, json!("1999-10-02 10:11:49.123")).unwrap(),
            json!("1999-10-02 10:11:49.123z")
        );
        assert_eq!(
            sanitize_json_value(&TimAT, json!([123124, "1949-10-02 10:11:49.1234"])).unwrap(),
            json!([123124, "1949-10-02 10:11:49.1234z"])
        );
    }

    #[test]
    fn sanitize_json_value_converts_only_string_bytes() {
        assert_eq!(sanitize_json_value(&BytT, json!([171])).unwrap(), json!([171]));
        assert_eq!(sanitize_json_value(&BytT, json!("ab")).unwrap(), json!([171]));
        assert_eq!(sanitize_json_value(&BytAT, json!([[171], "ab"])).unwrap(), json!([[171], [171]]));
    }

    #[test]
    fn sanitize_json_value_converts_only_non_empty_string_json() {
        assert_eq!(sanitize_json_value(&JsnT, json!("")).unwrap(), json!(""));
        assert_eq!(sanitize_json_value(&JsnT, json!(171)).unwrap(), json!(171));
        assert_eq!(sanitize_json_value(&JsnT, json!("171")).unwrap(), json!(171));
        assert_eq!(sanitize_json_value(&JsnT, json!([171])).unwrap(), json!([171]));
        assert_eq!(sanitize_json_value(&JsnT, json!("[171]")).unwrap(), json!([171]));
        assert_eq!(sanitize_json_value(&JsnT, json!("\"a\"")).unwrap(), json!("a"));
        assert_eq!(sanitize_json_value(&JsnT, json!({"a": "b"})).unwrap(), json!({"a": "b"}));
        assert_eq!(sanitize_json_value(&JsnT, json!("{\"a\": \"b\"}")).unwrap(), json!({"a": "b"}));
    }

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
            v_floats: Vec<f32>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![FltT, FltAT],
            column_names: to_string_vec(vec!["v_float", "v_floats"]),
        });
        let values = vec![json!(4.1), json!([4.1, 4.2])];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow { v_float: 4.1, v_floats: vec![4.1, 4.2] });
    }


    #[test]
    fn pinot_row_deserializable_to_struct_for_double_fields() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct TestRow {
            v_double: f64,
            v_doubles: Vec<f64>,
        }
        let data_schema = RespSchema::from(RawRespSchema {
            column_data_types: vec![DubT, DubAT],
            column_names: to_string_vec(vec!["v_double", "v_doubles"]),
        });
        let values = vec![json!(5.1), json!([5.1, 5.2])];
        let test_row = TestRow::from_row(&data_schema, values).unwrap();
        assert_eq!(test_row, TestRow { v_double: 5.1, v_doubles: vec![5.1, 5.2] });
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
}