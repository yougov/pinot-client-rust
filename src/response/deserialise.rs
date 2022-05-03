use serde::de::Error as SerdeError;
use serde::Deserialize;
use serde_json::Value;

use crate::response::sql::{FromRow, RespSchema};

/// Cheap and probably inefficient means of arbitrary
/// struct deserialization by making an intermediate
/// json map for each row.
impl<'de, T: Deserialize<'de>> FromRow for T {
    fn from_row(
        data_schema: &RespSchema,
        row: Vec<Value>,
    ) -> std::result::Result<Self, serde_json::Error> {
        let row_map = vec_row_to_json_map(data_schema, row)?;
        Deserialize::deserialize(row_map)
    }
}

fn vec_row_to_json_map(
    data_schema: &RespSchema,
    vec_row: Vec<Value>,
) -> std::result::Result<Value, serde_json::Error> {
    let mut map_row: serde_json::Map<String, Value> = serde_json::Map::with_capacity(vec_row.len());
    for (column_index, value) in vec_row.into_iter().enumerate() {
        let column_name = data_schema
            .get_column_name(column_index)
            .map_err(|_| serde_json::Error::custom(format!(
                "column index of {} not found in data schema when deserializing rows",
                column_index
            )))?;
        map_row.insert(column_name.to_string(), value);
    }
    Ok(Value::Object(map_row))
}

#[cfg(test)]
pub mod tests {
    #[test]
    fn todo() {
        todo!()
    }
}