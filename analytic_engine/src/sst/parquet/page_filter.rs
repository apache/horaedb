use std::{collections::HashSet, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray,
    },
    datatypes::{DataType, SchemaRef},
    error::ArrowError,
};
use datafusion::{
    logical_expr::utils::expr_to_columns,
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
    physical_plan::file_format::ParquetFileMetrics,
    prelude::Expr,
};
use datafusion_common::{Column, DataFusionError, Result, ScalarValue};
use log::{debug, trace};
use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    basic::{ConvertedType, LogicalType},
    errors::ParquetError,
    file::{metadata::RowGroupMetaData, page_index::index::Index},
    format::PageLocation,
    schema::types::ColumnDescriptor,
};
use parquet_ext::ParquetMetaData;

#[derive(Debug)]
pub(crate) struct PagePruningPredicate {
    predicates: Vec<PruningPredicate>,
}

fn to_columns(expr: &Expr) -> Result<HashSet<Column>> {
    let mut using_columns = HashSet::new();
    expr_to_columns(expr, &mut using_columns)?;

    Ok(using_columns)
}

impl PagePruningPredicate {
    /// Create a new [`PagePruningPredicate`]
    pub fn try_new(exprs: &[Expr], schema: SchemaRef) -> Result<Self> {
        let predicates = exprs
            .into_iter()
            .filter_map(|predicate| match to_columns(predicate) {
                Ok(columns) if columns.len() == 1 => {
                    match PruningPredicate::try_new(predicate.clone(), schema.clone()) {
                        Ok(p) => Some(Ok(p)),
                        _ => None,
                    }
                }
                _ => None,
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { predicates })
    }

    /// Returns a [`RowSelection`] for the given file
    pub fn prune(
        &self,
        row_groups: &[usize],
        file_metadata: &ParquetMetaData,
        file_metrics: &ParquetFileMetrics,
    ) -> Result<Option<RowSelection>> {
        if self.predicates.is_empty() {
            return Ok(None);
        }

        let page_index_predicates = &self.predicates;
        let groups = file_metadata.row_groups();

        let file_offset_indexes = file_metadata.offset_indexes();
        let file_page_indexes = file_metadata.page_indexes();
        let (file_offset_indexes, file_page_indexes) =
            match (file_offset_indexes, file_page_indexes) {
                (Some(o), Some(i)) => (o, i),
                _ => {
                    trace!(
                        "skip page pruning due to lack of indexes. Have offset: {} file: {}",
                        file_offset_indexes.is_some(),
                        file_page_indexes.is_some()
                    );
                    return Ok(None);
                }
            };

        let mut row_selections = Vec::with_capacity(page_index_predicates.len());
        for predicate in page_index_predicates {
            // `extract_page_index_push_down_predicates` only return predicate with one col.
            //  when building `PruningPredicate`, some single column filter like `abs(i) =
            // 1`  will be rewrite to `lit(true)`, so may have an empty
            // required_columns.
            let col_id = if let Some(&col_id) = predicate.need_input_columns_ids().iter().next() {
                col_id
            } else {
                continue;
            };

            let mut selectors = Vec::with_capacity(row_groups.len());
            for r in row_groups.iter() {
                let rg_offset_indexes = file_offset_indexes.get(*r);
                let rg_page_indexes = file_page_indexes.get(*r);
                if let (Some(rg_page_indexes), Some(rg_offset_indexes)) =
                    (rg_page_indexes, rg_offset_indexes)
                {
                    selectors.extend(
                        prune_pages_in_one_row_group(
                            &groups[*r],
                            predicate,
                            rg_offset_indexes.get(col_id),
                            rg_page_indexes.get(col_id),
                            groups[*r].column(col_id).column_descr(),
                            file_metrics,
                        )
                        .map_err(|e| {
                            ArrowError::ParquetError(format!(
                                "Fail in prune_pages_in_one_row_group: {e}"
                            ))
                        }),
                    );
                } else {
                    trace!(
                        "Did not have enough metadata to prune with page indexes, \
                         falling back, falling back to all rows",
                    );
                    // fallback select all rows
                    let all_selected = vec![RowSelector::select(groups[*r].num_rows() as usize)];
                    selectors.push(all_selected);
                }
            }
            debug!(
                "Use filter and page index create RowSelection {:?} from predicate: {:?}",
                &selectors,
                predicate.predicate_expr(),
            );
            row_selections.push(selectors.into_iter().flatten().collect::<Vec<_>>());
        }

        let final_selection = combine_multi_col_selection(row_selections);
        Ok(Some(final_selection))
    }
}

/// Intersects the [`RowSelector`]s
///
/// For exampe, given:
/// * `RowSelector1: [ Skip(0~199), Read(200~299)]`
/// * `RowSelector2: [ Skip(0~99), Read(100~249), Skip(250~299)]`
///
/// The final selection is the intersection of these  `RowSelector`s:
/// * `final_selection:[ Skip(0~199), Read(200~249), Skip(250~299)]`
fn combine_multi_col_selection(row_selections: Vec<Vec<RowSelector>>) -> RowSelection {
    row_selections
        .into_iter()
        .map(RowSelection::from)
        .reduce(|s1, s2| s1.and_then(&s2))
        .unwrap()
}

fn parquet_to_arrow_decimal_type(parquet_column: &ColumnDescriptor) -> Option<DataType> {
    let type_ptr = parquet_column.self_type_ptr();
    match type_ptr.get_basic_info().logical_type() {
        _ => match type_ptr.get_basic_info().converted_type() {
            _ => None,
        },
    }
}
fn prune_pages_in_one_row_group(
    group: &RowGroupMetaData,
    predicate: &PruningPredicate,
    col_offset_indexes: Option<&Vec<PageLocation>>,
    col_page_indexes: Option<&Index>,
    col_desc: &ColumnDescriptor,
    metrics: &ParquetFileMetrics,
) -> Result<Vec<RowSelector>> {
    let num_rows = group.num_rows() as usize;
    if let (Some(col_offset_indexes), Some(col_page_indexes)) =
        (col_offset_indexes, col_page_indexes)
    {
        let target_type = parquet_to_arrow_decimal_type(col_desc);
        let pruning_stats = PagesPruningStatistics {
            col_page_indexes,
            col_offset_indexes,
            target_type: &target_type,
        };

        match predicate.prune(&pruning_stats) {
            Ok(values) => {
                let mut vec = Vec::with_capacity(values.len());
                let row_vec = create_row_count_in_each_page(col_offset_indexes, num_rows);
                assert_eq!(row_vec.len(), values.len());
                let mut sum_row = *row_vec.first().unwrap();
                let mut selected = *values.first().unwrap();
                trace!("Pruned to to {:?} using {:?}", values, pruning_stats);
                for (i, &f) in values.iter().enumerate().skip(1) {
                    if f == selected {
                        sum_row += *row_vec.get(i).unwrap();
                    } else {
                        let selector = if selected {
                            RowSelector::select(sum_row)
                        } else {
                            RowSelector::skip(sum_row)
                        };
                        vec.push(selector);
                        sum_row = *row_vec.get(i).unwrap();
                        selected = f;
                    }
                }

                let selector = if selected {
                    RowSelector::select(sum_row)
                } else {
                    RowSelector::skip(sum_row)
                };
                vec.push(selector);
                return Ok(vec);
            }
            // stats filter array could not be built
            // return a result which will not filter out any pages
            Err(e) => {
                debug!("Error evaluating page index predicate values {}", e);
                metrics.predicate_evaluation_errors.add(1);
                return Ok(vec![RowSelector::select(group.num_rows() as usize)]);
            }
        }
    }
    Err(DataFusionError::ParquetError(ParquetError::General(
        "Got some error in prune_pages_in_one_row_group, plz try open the debuglog mode"
            .to_string(),
    )))
}

fn create_row_count_in_each_page(location: &Vec<PageLocation>, num_rows: usize) -> Vec<usize> {
    let mut vec = Vec::with_capacity(location.len());
    location.windows(2).for_each(|x| {
        let start = x[0].first_row_index as usize;
        let end = x[1].first_row_index as usize;
        vec.push(end - start);
    });
    vec.push(num_rows - location.last().unwrap().first_row_index as usize);
    vec
}

/// Wraps one col page_index in one rowGroup statistics in a way
/// that implements [`PruningStatistics`]
#[derive(Debug)]
struct PagesPruningStatistics<'a> {
    col_page_indexes: &'a Index,
    col_offset_indexes: &'a Vec<PageLocation>,
    // target_type means the logical type in schema: like 'DECIMAL' is the logical type, but the
    // real physical type in parquet file may be `INT32, INT64, FIXED_LEN_BYTE_ARRAY`
    target_type: &'a Option<DataType>,
}

// Extract the min or max value calling `func` from page idex
macro_rules! get_min_max_values_for_page_index {
    ($self:expr, $func:ident) => {{
        match $self.col_page_indexes {
            Index::NONE => None,
            Index::INT32(index) => match $self.target_type {
                _ => {
                    let vec = &index.indexes;
                    Some(Arc::new(Int32Array::from_iter(
                        vec.iter().map(|x| x.$func().cloned()),
                    )))
                }
            },
            Index::INT64(index) => match $self.target_type {
                _ => {
                    let vec = &index.indexes;
                    Some(Arc::new(Int64Array::from_iter(
                        vec.iter().map(|x| x.$func().cloned()),
                    )))
                }
            },
            Index::FLOAT(index) => {
                let vec = &index.indexes;
                Some(Arc::new(Float32Array::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
            }
            Index::DOUBLE(index) => {
                let vec = &index.indexes;
                Some(Arc::new(Float64Array::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
            }
            Index::BOOLEAN(index) => {
                let vec = &index.indexes;
                Some(Arc::new(BooleanArray::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
            }
            Index::BYTE_ARRAY(index) => match $self.target_type {
                _ => {
                    let vec = &index.indexes;
                    let array: StringArray = vec
                        .iter()
                        .map(|x| x.$func())
                        .map(|x| x.and_then(|x| std::str::from_utf8(x.as_ref()).ok()))
                        .collect();
                    Some(Arc::new(array))
                }
            },
            Index::INT96(_) => {
                //Todo support these type
                None
            }
            Index::FIXED_LEN_BYTE_ARRAY(index) => match $self.target_type {
                _ => None,
            },
        }
    }};
}

// Convert the bytes array to i128.
// The endian of the input bytes array must be big-endian.
pub(crate) fn from_bytes_to_i128(b: &[u8]) -> i128 {
    // The bytes array are from parquet file and must be the big-endian.
    // The endian is defined by parquet format, and the reference document
    // https://github.com/apache/parquet-format/blob/54e53e5d7794d383529dd30746378f19a12afd58/src/main/thrift/parquet.thrift#L66
    i128::from_be_bytes(sign_extend_be(b))
}

// Copy from the arrow-rs
// https://github.com/apache/arrow-rs/blob/733b7e7fd1e8c43a404c3ce40ecf741d493c21b4/parquet/src/arrow/buffer/bit_util.rs#L55
// Convert the byte slice to fixed length byte array with the length of 16
fn sign_extend_be(b: &[u8]) -> [u8; 16] {
    assert!(b.len() <= 16, "Array too large, expected less than 16");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; 16] } else { [0u8; 16] };
    for (d, s) in result.iter_mut().skip(16 - b.len()).zip(b) {
        *d = *s;
    }
    result
}

impl<'a> PruningStatistics for PagesPruningStatistics<'a> {
    fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
        get_min_max_values_for_page_index!(self, min)
    }

    fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
        get_min_max_values_for_page_index!(self, max)
    }

    fn num_containers(&self) -> usize {
        self.col_offset_indexes.len()
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        match self.col_page_indexes {
            Index::NONE => None,
            Index::BOOLEAN(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::INT32(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::INT64(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::FLOAT(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::DOUBLE(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::INT96(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::BYTE_ARRAY(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::FIXED_LEN_BYTE_ARRAY(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
        }
    }
}
