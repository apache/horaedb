// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! SQL statement

use sqlparser::ast::{
    ColumnDef, ObjectName, SqlOption, Statement as SqlStatement, TableConstraint,
};

/// Statement representations
#[derive(Debug, PartialEq)]
pub enum Statement {
    /// ANSI SQL AST node
    Standard(Box<SqlStatement>),
    // Other extensions
    /// CREATE TABLE
    Create(CreateTable),
    /// Drop TABLE
    Drop(DropTable),
    Describe(DescribeTable),
    AlterModifySetting(AlterModifySetting),
    AlterAddColumn(AlterAddColumn),
    /// SHOW CREATE TABLE
    ShowCreate(ShowCreate),
    ShowDatabase,
    ShowTables,
    Exists(ExistsTable),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ShowCreateObject {
    Table,
}

#[derive(Debug, PartialEq)]
pub struct CreateTable {
    /// Create if not exists
    pub if_not_exists: bool,
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub engine: String,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`.
    pub options: Vec<SqlOption>,
}

#[derive(Debug, PartialEq)]
pub struct DropTable {
    /// Table name
    pub name: ObjectName,
    pub if_exists: bool,
    pub engine: String,
}

#[derive(Debug, PartialEq)]
pub struct DescribeTable {
    pub table_name: ObjectName,
}

#[derive(Debug, PartialEq)]
pub struct AlterModifySetting {
    pub table_name: ObjectName,
    pub options: Vec<SqlOption>,
}

#[derive(Debug, PartialEq)]
pub struct AlterAddColumn {
    pub table_name: ObjectName,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, PartialEq)]
pub struct ShowCreate {
    pub obj_type: ShowCreateObject,
    pub obj_name: ObjectName,
}

#[derive(Debug, PartialEq)]
pub struct ExistsTable {
    pub table_name: ObjectName,
}
