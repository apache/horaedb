use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use common_types::record_batch::RecordBatchWithKey;
use datafusion::{
    datasource::{listing::PartitionedFile, object_store::ObjectStoreUrl},
    physical_plan::{
        file_format::{FileScanConfig, ParquetExec},
        SendableRecordBatchStream, Statistics,
    },
};
use futures::Stream;
use object_store::{ObjectMeta, ObjectStoreRef, Path};
use parquet_ext::{DataCacheRef, MetaCacheRef};
use table_engine::predicate::PredicateRef;

use crate::sst::{
    factory::SstReaderOptions,
    file::SstMetaData,
    reader::{Result, SstReader},
};

struct ParquetSstReader<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    storage: &'a ObjectStoreRef,

    schema: ArrowSchemaRef,
    meta_cache: Option<MetaCacheRef>,
    data_cache: Option<DataCacheRef>,
    predicate: PredicateRef,
}

impl<'a> ParquetSstReader<'a> {
    pub fn new(path: &'a Path, storage: &'a ObjectStoreRef, options: &SstReaderOptions) -> Self {
        // storage.head(path);
        let schema_to_read = options.projected_schema.to_projected_arrow_schema();
        // let object_meta = ObjectMeta {
        //     location: path,
        //     // we don't care about the "last modified" field
        //     last_modified: Default::default(),
        //     size: file_size,
        // };
        Self {
            path,
            storage,
            schema: schema_to_read,
            meta_cache: options.meta_cache.clone(),
            data_cache: options.data_cache.clone(),
            predicate: options.predicate,
        }
    }

    async fn read_record_batches(&mut self) -> Result<SendableRecordBatchStream> {
        // create ParquetExec node
        let object_meta = ObjectMeta {
            location: self.path.clone(),
            // we don't care about the "last modified" field
            last_modified: Default::default(),
            size: 0, // FIXME
        };
        let expr = self.predicate.filter_expr("timestamp");
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("iox://iox/").expect("valid object store URL"),
            file_schema: self.schema.clone(), // TODO: parse from sst meta
            file_groups: vec![vec![PartitionedFile {
                object_meta,
                partition_values: vec![],
                range: None,
                extensions: None,
            }]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
        };
        let exec = ParquetExec::new(base_config, Some(expr), None);

        // set up "fake" DataFusion session
        // let object_store = Arc::clone(&self.object_store);
        // let session_ctx = SessionContext::new();
        // let task_ctx = Arc::new(TaskContext::from(&session_ctx));
        // task_ctx
        //     .runtime_env()
        //     .register_object_store("iox", "iox", object_store);

        // Ok(Box::pin(RecordBatchStreamAdapter::new(
        //     Arc::clone(&schema),
        //     futures::stream::once(execute_stream(Arc::new(exec),
        // task_ctx)).try_flatten(), )))
        todo!()
    }
}

#[async_trait]
impl<'a> SstReader for ParquetSstReader<'a> {
    async fn meta_data(&mut self) -> Result<&SstMetaData> {
        todo!()
    }

    async fn read(
        &mut self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>> {
        todo!()
    }
}
