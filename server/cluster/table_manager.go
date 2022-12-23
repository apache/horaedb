// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// TableManager manages table metadata by schema.
type TableManager interface {
	// Load load table meta data from storage.
	Load(ctx context.Context) error
	// GetTable get table with schemaName and tableName, the second output parameter bool: returns true if the table exists.
	GetTable(schemaName string, tableName string) (storage.Table, bool, error)
	// GetTablesByIDs get tables with tableIDs.
	GetTablesByIDs(tableIDs []storage.TableID) []storage.Table
	// CreateTable create table with schemaName and tableName.
	CreateTable(ctx context.Context, schemaName string, tableName string, partitioned bool) (storage.Table, error)
	// DropTable drop table with schemaName and tableName.
	DropTable(ctx context.Context, schemaName string, tableName string) error
	// GetSchemaByName get schema with schemaName.
	GetSchemaByName(schemaName string) (storage.Schema, bool)
	// GetSchemas get all schemas in cluster.
	GetSchemas() []storage.Schema
	// GetOrCreateSchema get or create schema with schemaName.
	GetOrCreateSchema(ctx context.Context, schemaName string) (storage.Schema, bool, error)
}

type Tables struct {
	tables     map[string]storage.Table          // tableName -> table
	tablesByID map[storage.TableID]storage.Table // tableID -> table
}

type TableManagerImpl struct {
	storage       storage.Storage
	clusterID     storage.ClusterID
	schemaIDAlloc id.Allocator
	tableIDAlloc  id.Allocator

	// RWMutex is used to protect following fields.
	lock         sync.RWMutex
	schemas      map[string]storage.Schema    // schemaName -> schema
	schemaTables map[storage.SchemaID]*Tables // schemaName -> tables
}

func NewTableManagerImpl(storage storage.Storage, clusterID storage.ClusterID, schemaIDAlloc id.Allocator, tableIDAlloc id.Allocator) TableManager {
	return &TableManagerImpl{
		storage:       storage,
		clusterID:     clusterID,
		schemaIDAlloc: schemaIDAlloc,
		tableIDAlloc:  tableIDAlloc,
	}
}

func (m *TableManagerImpl) Load(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.loadSchemas(ctx); err != nil {
		return errors.WithMessage(err, "load schemas")
	}

	if err := m.loadTables(ctx); err != nil {
		return errors.WithMessage(err, "load tables")
	}

	return nil
}

func (m *TableManagerImpl) GetTable(schemaName, tableName string) (storage.Table, bool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.getTable(schemaName, tableName)
}

func (m *TableManagerImpl) GetTablesByIDs(tableIDs []storage.TableID) []storage.Table {
	m.lock.RLock()
	defer m.lock.RUnlock()

	result := make([]storage.Table, 0, len(tableIDs))
	for _, tables := range m.schemaTables {
		for _, tableID := range tableIDs {
			table, ok := tables.tablesByID[tableID]
			if !ok {
				log.Warn("table not exists", zap.Uint64("tableID", uint64(tableID)))
				continue
			}
			result = append(result, table)
		}
	}

	return result
}

func (m *TableManagerImpl) CreateTable(ctx context.Context, schemaName string, tableName string, partitioned bool) (storage.Table, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, exists, err := m.getTable(schemaName, tableName)
	if err != nil {
		return storage.Table{}, errors.WithMessage(err, "get table")
	}

	if exists {
		return storage.Table{}, ErrTableAlreadyExists
	}

	// Create table in storage.
	schema, ok := m.schemas[schemaName]
	if !ok {
		return storage.Table{}, ErrSchemaNotFound.WithCausef("schema name:%s", schemaName)
	}

	id, err := m.tableIDAlloc.Alloc(ctx)
	if err != nil {
		return storage.Table{}, errors.WithMessagef(err, "alloc table id, table name:%s", tableName)
	}

	table := storage.Table{
		ID:          storage.TableID(id),
		Name:        tableName,
		SchemaID:    schema.ID,
		CreatedAt:   uint64(time.Now().UnixMilli()),
		Partitioned: partitioned,
	}
	err = m.storage.CreateTable(ctx, storage.CreateTableRequest{
		ClusterID: m.clusterID,
		SchemaID:  schema.ID,
		Table:     table,
	})

	if err != nil {
		return storage.Table{}, errors.WithMessage(err, "storage create table")
	}

	// Update table in memory.
	_, ok = m.schemaTables[schema.ID]
	if !ok {
		m.schemaTables[schema.ID] = &Tables{
			tables:     make(map[string]storage.Table),
			tablesByID: make(map[storage.TableID]storage.Table),
		}
	}
	tables := m.schemaTables[schema.ID]
	tables.tables[tableName] = table
	tables.tablesByID[table.ID] = table

	return table, nil
}

func (m *TableManagerImpl) DropTable(ctx context.Context, schemaName string, tableName string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	schema, ok := m.schemas[schemaName]
	if !ok {
		return nil
	}

	table, ok := m.schemaTables[schema.ID].tables[tableName]
	if !ok {
		return nil
	}

	// Delete table in storage.
	err := m.storage.DeleteTable(ctx, storage.DeleteTableRequest{
		ClusterID: m.clusterID,
		SchemaID:  schema.ID,
		TableName: tableName,
	})
	if err != nil {
		return errors.WithMessagef(err, "storage delete table")
	}

	tables := m.schemaTables[schema.ID]
	delete(tables.tables, tableName)
	delete(tables.tablesByID, table.ID)
	return nil
}

func (m *TableManagerImpl) GetSchemaByName(schemaName string) (storage.Schema, bool) {
	schema, ok := m.schemas[schemaName]
	return schema, ok
}

func (m *TableManagerImpl) GetSchemas() []storage.Schema {
	m.lock.RLock()
	defer m.lock.RUnlock()

	schemas := make([]storage.Schema, len(m.schemas))

	for _, schema := range m.schemas {
		schemas = append(schemas, schema)
	}

	return schemas
}

func (m *TableManagerImpl) GetOrCreateSchema(ctx context.Context, schemaName string) (storage.Schema, bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	schema, ok := m.schemas[schemaName]
	if ok {
		return schema, true, nil
	}

	id, err := m.schemaIDAlloc.Alloc(ctx)
	if err != nil {
		return storage.Schema{}, false, errors.WithMessage(err, "alloc schema id")
	}

	schema = storage.Schema{
		ID:        storage.SchemaID(id),
		ClusterID: m.clusterID,
		Name:      schemaName,
		CreatedAt: uint64(time.Now().UnixMilli()),
	}

	// Create schema in storage.
	if err = m.storage.CreateSchema(ctx, storage.CreateSchemaRequest{
		ClusterID: m.clusterID,
		Schema:    schema,
	}); err != nil {
		return storage.Schema{}, false, errors.WithMessage(err, "storage create schema")
	}
	// Update schema in memory.
	m.schemas[schemaName] = schema
	return schema, false, nil
}

func (m *TableManagerImpl) loadSchemas(ctx context.Context) error {
	schemasResult, err := m.storage.ListSchemas(ctx, storage.ListSchemasRequest{ClusterID: m.clusterID})
	if err != nil {
		return errors.WithMessage(err, "list schemas")
	}
	log.Debug("load schema", zap.String("data", fmt.Sprintf("%+v", schemasResult)))

	// Reset data in memory.
	m.schemas = make(map[string]storage.Schema, len(schemasResult.Schemas))
	for _, schema := range schemasResult.Schemas {
		m.schemas[schema.Name] = schema
	}

	return nil
}

func (m *TableManagerImpl) loadTables(ctx context.Context) error {
	// Reset data in memory.
	m.schemaTables = make(map[storage.SchemaID]*Tables, len(m.schemas))
	for _, schema := range m.schemas {
		tablesResult, err := m.storage.ListTables(ctx, storage.ListTableRequest{
			ClusterID: m.clusterID,
			SchemaID:  schema.ID,
		})
		if err != nil {
			return errors.WithMessage(err, "list tables")
		}
		log.Debug("load table", zap.String("schema", fmt.Sprintf("%+v", schema)), zap.String("tables", fmt.Sprintf("%+v", tablesResult)))

		for _, table := range tablesResult.Tables {
			tables, ok := m.schemaTables[table.SchemaID]
			if !ok {
				tables = &Tables{
					tables:     make(map[string]storage.Table, 0),
					tablesByID: make(map[storage.TableID]storage.Table, 0),
				}
				m.schemaTables[table.SchemaID] = tables
			}

			tables.tables[table.Name] = table
			tables.tablesByID[table.ID] = table
		}
	}
	return nil
}

func (m *TableManagerImpl) getTable(schemaName, tableName string) (storage.Table, bool, error) {
	schema, ok := m.schemas[schemaName]
	if !ok {
		return storage.Table{}, false, ErrSchemaNotFound.WithCausef("schema name", schemaName)
	}
	tables, ok := m.schemaTables[schema.ID]
	if !ok {
		return storage.Table{}, false, nil
	}

	table, ok := tables.tables[tableName]
	return table, ok, nil
}
