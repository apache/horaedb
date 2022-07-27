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
    ShowDatabases,
    ShowTables,
    Exists(ExistsTable),
}

#[derive(Debug, PartialEq)]
pub struct TableName(ObjectName);

impl TableName {
    pub fn is_empty(&self) -> bool {
        self.0 .0.is_empty()
    }
}

impl ToString for TableName {
    fn to_string(&self) -> String {
        self.0
             .0
            .iter()
            .map(|ident| ident.value.as_str())
            .collect::<Vec<_>>()
            .join(".")
    }
}

impl From<ObjectName> for TableName {
    fn from(object_name: ObjectName) -> Self {
        Self(object_name)
    }
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
    pub table_name: TableName,
    pub columns: Vec<ColumnDef>,
    pub engine: String,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`.
    pub options: Vec<SqlOption>,
}

#[derive(Debug, PartialEq)]
pub struct DropTable {
    /// Table name
    pub table_name: TableName,
    pub if_exists: bool,
    pub engine: String,
}

#[derive(Debug, PartialEq)]
pub struct DescribeTable {
    pub table_name: TableName,
}

#[derive(Debug, PartialEq)]
pub struct AlterModifySetting {
    pub table_name: TableName,
    pub options: Vec<SqlOption>,
}

#[derive(Debug, PartialEq)]
pub struct AlterAddColumn {
    pub table_name: TableName,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, PartialEq)]
pub struct ShowCreate {
    pub obj_type: ShowCreateObject,
    pub table_name: TableName,
}

#[derive(Debug, PartialEq)]
pub struct ExistsTable {
    pub table_name: TableName,
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Ident;

    use super::*;

    #[test]
    fn test_table_name() {
        let testcases = vec![
            (
                ObjectName(vec![
                    Ident::with_quote('`', "schema"),
                    Ident::with_quote('`', "table"),
                ]),
                "schema.table",
            ),
            (
                ObjectName(vec![Ident::new("schema"), Ident::new("table")]),
                "schema.table",
            ),
        ];

        for (object_name, expected) in testcases {
            assert_eq!(TableName::from(object_name).to_string(), expected);
        }
    }
}
