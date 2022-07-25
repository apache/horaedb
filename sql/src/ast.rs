// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! SQL statement

use sqlparser::{
    ast::{
        ColumnDef, ColumnOption, ColumnOptionDef, DataType, Expr, Ident, ObjectName,
        ReferentialAction, SqlOption, Statement as SqlStatement, TableConstraint,
    },
    tokenizer::Token,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CeresColumnOptionDef {
    pub name: Option<Ident>,
    pub option: CeresColumnOption,
}

impl From<ColumnOptionDef> for CeresColumnOptionDef {
    fn from(c: ColumnOptionDef) -> Self {
        Self {
            name: c.name,
            option: c.option.into(),
        }
    }
}

impl From<CeresColumnOptionDef> for ColumnOptionDef {
    fn from(c: CeresColumnOptionDef) -> Self {
        Self {
            name: c.name,
            option: c.option.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CeresColumnOption {
    Null,
    NotNull,
    Default(Expr),
    Unique {
        is_primary: bool,
        is_timestamp: bool,
    },
    ForeignKey {
        foreign_table: ObjectName,
        referred_columns: Vec<Ident>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Check(Expr),
    DialectSpecific(Vec<Token>),
    CharacterSet(ObjectName),
    Comment(String),
}

impl From<CeresColumnOption> for ColumnOption {
    fn from(c: CeresColumnOption) -> Self {
        use CeresColumnOption::*;
        match c {
            Null => Self::Null,
            NotNull => Self::NotNull,
            Default(e) => Self::Default(e),
            Unique { is_primary, .. } => Self::Unique { is_primary },
            ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            } => Self::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            },
            Check(e) => Self::Check(e),
            DialectSpecific(v) => Self::DialectSpecific(v),
            CharacterSet(name) => Self::CharacterSet(name),
            Comment(s) => Self::Comment(s),
        }
    }
}

impl From<ColumnOption> for CeresColumnOption {
    fn from(c: ColumnOption) -> Self {
        use ColumnOption::*;
        match c {
            Null => Self::Null,
            NotNull => Self::NotNull,
            Default(e) => Self::Default(e),
            Unique { is_primary } => Self::Unique {
                is_primary,
                is_timestamp: false,
            },
            ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            } => Self::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            },
            Check(e) => Self::Check(e),
            DialectSpecific(v) => Self::DialectSpecific(v),
            CharacterSet(name) => Self::CharacterSet(name),
            Comment(s) => Self::Comment(s),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CeresColumnDef {
    pub name: Ident,
    pub data_type: DataType,
    pub collation: Option<ObjectName>,
    pub options: Vec<CeresColumnOptionDef>,
}

impl From<CeresColumnDef> for ColumnDef {
    fn from(c: CeresColumnDef) -> Self {
        Self {
            name: c.name,
            data_type: c.data_type,
            collation: c.collation,
            options: c.options.into_iter().map(|option| option.into()).collect(),
        }
    }
}

impl From<ColumnDef> for CeresColumnDef {
    fn from(c: ColumnDef) -> Self {
        Self {
            name: c.name,
            data_type: c.data_type,
            collation: c.collation,
            options: c.options.into_iter().map(|option| option.into()).collect(),
        }
    }
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
