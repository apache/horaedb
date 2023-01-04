// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::cmp;

use common_types::{schema::Schema, time::TimeRange};
use snafu::OptionExt;
use table_engine::table::TableId;

use super::{
    factory::{FactoryRef, ObjectStorePickerRef},
    file::FileHandle,
};
use crate::{
    space::SpaceId,
    sst::{factory::SstReaderOptions, file::SstMetaData, reader},
    table::sst_util,
    table_options::StorageFormatOptions,
};

pub struct SstMetaReader {
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub factory: FactoryRef,
    pub read_opts: SstReaderOptions,
    pub store_picker: ObjectStorePickerRef,
}

impl SstMetaReader {
    pub async fn fetch_metas(&self, files: &[FileHandle]) -> reader::Result<Vec<SstMetaData>> {
        let mut sst_metas = Vec::with_capacity(files.len());
        for f in files {
            let path = sst_util::new_sst_file_path(self.space_id, self.table_id, f.id());
            let mut reader = self
                .factory
                .new_sst_reader(&self.read_opts, &path, &self.store_picker, f.size())
                .context(reader::OtherNoCause {
                    msg: format!("no sst reader found for the file:{:?}", path),
                })?;
            let meta_data = reader.meta_data().await?;
            sst_metas.push(meta_data.clone());
        }

        Ok(sst_metas)
    }
}

/// Merge sst meta of given `sst_metas`, panic if `sst_metas` is empty.
///
/// The size and row_num of the merged meta is initialized to 0.
pub fn merge_sst_meta(sst_metas: &[SstMetaData], schema: Schema) -> SstMetaData {
    let mut min_key = &sst_metas[0].min_key;
    let mut max_key = &sst_metas[0].max_key;
    let mut time_range_start = sst_metas[0].time_range.inclusive_start();
    let mut time_range_end = sst_metas[0].time_range.exclusive_end();
    let mut max_sequence = sst_metas[0].max_sequence;
    // TODO(jiacai2050): what if format of different file is different?
    // pick first now
    let storage_format = sst_metas[0].storage_format();

    if sst_metas.len() > 1 {
        for file in &sst_metas[1..] {
            min_key = cmp::min(&file.min_key, min_key);
            max_key = cmp::max(&file.max_key, max_key);
            time_range_start = cmp::min(file.time_range.inclusive_start(), time_range_start);
            time_range_end = cmp::max(file.time_range.exclusive_end(), time_range_end);
            max_sequence = cmp::max(file.max_sequence, max_sequence);
        }
    }

    SstMetaData {
        min_key: min_key.clone(),
        max_key: max_key.clone(),
        time_range: TimeRange::new(time_range_start, time_range_end).unwrap(),
        max_sequence,
        schema,
        storage_format_opts: StorageFormatOptions::new(storage_format),
        // bloom filter is rebuilt when write sst, so use default here
        bloom_filter: None,
    }
}
