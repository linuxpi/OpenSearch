/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, GenericListArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex::Regex;

use super::json_common::StringArrayView;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ArrayAnyCompareUdf::new()));
    ctx.register_udf(ScalarUDF::from(ArrayAnyBetweenUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayAnyCompareUdf {
    signature: Signature,
}

impl ArrayAnyCompareUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for ArrayAnyCompareUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayAnyCompareUdf {
    fn name(&self) -> &str {
        "array_any_compare"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_types("array_any_compare", arg_types)?;
        Ok(DataType::Boolean)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        check_types("array_any_compare", arg_types)?;
        Ok(vec![arg_types[0].clone(), DataType::Utf8, DataType::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        check_arity("array_any_compare", args.args.len())?;
        let rows = args.number_rows;
        let list_ref = args.args[0].clone().into_array(rows)?;
        let list = as_list("array_any_compare", &list_ref)?;
        let value_ref = args.args[1].clone().into_array(rows)?;
        let operation_ref = args.args[2].clone().into_array(rows)?;
        let values = StringArrayView::from_array(&value_ref)?;
        let operations = StringArrayView::from_array(&operation_ref)?;
        let mut builder = BooleanBuilder::with_capacity(rows);

        for row_index in 0..rows {
            if list.is_null(row_index) {
                builder.append_null();
                continue;
            }
            let (Some(value), Some(operation)) =
                (values.cell(row_index), operations.cell(row_index))
            else {
                builder.append_null();
                continue;
            };
            builder.append_option(compare_row(&list.value(row_index), value, operation)?);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayAnyBetweenUdf {
    signature: Signature,
}

impl ArrayAnyBetweenUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for ArrayAnyBetweenUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayAnyBetweenUdf {
    fn name(&self) -> &str {
        "array_any_between"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_types("array_any_between", arg_types)?;
        Ok(DataType::Boolean)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        check_types("array_any_between", arg_types)?;
        Ok(vec![arg_types[0].clone(), DataType::Utf8, DataType::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        check_arity("array_any_between", args.args.len())?;
        let rows = args.number_rows;
        let list_ref = args.args[0].clone().into_array(rows)?;
        let list = as_list("array_any_between", &list_ref)?;
        let lower_ref = args.args[1].clone().into_array(rows)?;
        let upper_ref = args.args[2].clone().into_array(rows)?;
        let lowers = StringArrayView::from_array(&lower_ref)?;
        let uppers = StringArrayView::from_array(&upper_ref)?;
        let mut builder = BooleanBuilder::with_capacity(rows);

        for row_index in 0..rows {
            if list.is_null(row_index) {
                builder.append_null();
                continue;
            }
            let (Some(lower), Some(upper)) = (lowers.cell(row_index), uppers.cell(row_index))
            else {
                builder.append_null();
                continue;
            };
            builder.append_option(between_row(&list.value(row_index), lower, upper)?);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn check_arity(name: &str, actual: usize) -> Result<()> {
    if actual != 3 {
        return plan_err!("{name} expects 3 arguments, got {actual}");
    }
    Ok(())
}

fn check_types(name: &str, types: &[DataType]) -> Result<()> {
    check_arity(name, types.len())?;
    if !matches!(types[0], DataType::List(_)) {
        return plan_err!("{name}: arg 0 expected List, got {:?}", types[0]);
    }
    for (index, data_type) in types[1..].iter().enumerate() {
        if !matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return plan_err!(
                "{name}: arg {} expected string, got {data_type:?}",
                index + 1
            );
        }
    }
    Ok(())
}

fn as_list<'a>(name: &str, array: &'a ArrayRef) -> Result<&'a GenericListArray<i32>> {
    array
        .as_any()
        .downcast_ref::<GenericListArray<i32>>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{name}: expected ListArray, got {:?}",
                array.data_type()
            ))
        })
}

fn compare_row(row: &ArrayRef, value: &str, operation: &str) -> Result<Option<bool>> {
    let strings = StringArrayView::from_array(row)?;
    let regex = match operation {
        "like" => Some(like_regex(value, true)?),
        "ilike" => Some(like_regex(value, false)?),
        "regex" => Some(Regex::new(value).map_err(|error| {
            DataFusionError::Plan(format!(
                "array_any_compare: invalid regex '{value}': {error}"
            ))
        })?),
        _ => None,
    };
    let mut saw_null = false;
    for index in 0..row.len() {
        let Some(candidate) = strings.cell(index) else {
            saw_null = true;
            continue;
        };
        let matched = match operation {
            "gt" => candidate > value,
            "gte" => candidate >= value,
            "lt" => candidate < value,
            "lte" => candidate <= value,
            "like" | "ilike" | "regex" => regex.as_ref().unwrap().is_match(candidate),
            other => return plan_err!("array_any_compare: unsupported operation '{other}'"),
        };
        if matched {
            return Ok(Some(true));
        }
    }
    Ok(if saw_null { None } else { Some(false) })
}

fn between_row(row: &ArrayRef, lower: &str, upper: &str) -> Result<Option<bool>> {
    let strings = StringArrayView::from_array(row)?;
    let mut saw_null = false;
    for index in 0..row.len() {
        let Some(candidate) = strings.cell(index) else {
            saw_null = true;
            continue;
        };
        if candidate >= lower && candidate <= upper {
            return Ok(Some(true));
        }
    }
    Ok(if saw_null { None } else { Some(false) })
}

fn like_regex(pattern: &str, case_sensitive: bool) -> Result<Regex> {
    let mut expression = String::from("^");
    for character in pattern.chars() {
        match character {
            '%' => expression.push_str(".*"),
            '_' => expression.push('.'),
            other => expression.push_str(&regex::escape(&other.to_string())),
        }
    }
    expression.push('$');
    if !case_sensitive {
        expression = format!("(?i:{expression})");
    }
    Regex::new(&expression).map_err(|error| {
        DataFusionError::Plan(format!("array_any_compare: invalid LIKE pattern: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BooleanArray, ListBuilder, StringBuilder};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::ScalarValue;

    fn lists(rows: &[Option<&[Option<&str>]>]) -> ArrayRef {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in rows {
            match row {
                None => builder.append_null(),
                Some(values) => {
                    for value in *values {
                        match value {
                            Some(value) => builder.values().append_value(value),
                            None => builder.values().append_null(),
                        }
                    }
                    builder.append(true);
                }
            }
        }
        Arc::new(builder.finish())
    }

    fn invoke(udf: &dyn ScalarUDFImpl, args: Vec<ColumnarValue>, rows: usize) -> BooleanArray {
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(index, arg)| Arc::new(Field::new(format!("arg{index}"), arg.data_type(), true)))
            .collect();
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        })
        .unwrap()
        .into_array(rows)
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .clone()
    }

    #[test]
    fn compare_handles_empty_null_and_null_elements() {
        let result = invoke(
            &ArrayAnyCompareUdf::new(),
            vec![
                ColumnarValue::Array(lists(&[
                    Some(&[Some("prod"), Some("blue")]),
                    Some(&[]),
                    None,
                    Some(&[None]),
                ])),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("orange".into()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("gt".into()))),
            ],
            4,
        );
        assert!(result.value(0));
        assert!(!result.value(1));
        assert!(result.is_null(2));
        assert!(result.is_null(3));
    }

    #[test]
    fn patterns_and_between_match_any_element() {
        let values = lists(&[Some(&[Some("prod"), Some("blue")])]);
        for (pattern, operation) in [("pro%", "like"), ("pro.*", "regex")] {
            let result = invoke(
                &ArrayAnyCompareUdf::new(),
                vec![
                    ColumnarValue::Array(values.clone()),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern.into()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(operation.into()))),
                ],
                1,
            );
            assert!(result.value(0));
        }
        let between = invoke(
            &ArrayAnyBetweenUdf::new(),
            vec![
                ColumnarValue::Array(values),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".into()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("c".into()))),
            ],
            1,
        );
        assert!(between.value(0));
    }
}
