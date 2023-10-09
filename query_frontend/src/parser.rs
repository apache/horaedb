// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL parser
//!
//! Some codes are copied from datafusion: <https://github.com/apache/arrow/blob/9d86440946b8b07e03abb94fad2da278affae08f/rust/datafusion/src/sql/parser.rs#L74>

use logger::debug;
use macros::define_result;
use paste::paste;
use sqlparser::{
    ast::{
        ColumnDef, ColumnOption, ColumnOptionDef, DataType, Expr, Ident, ObjectName, SetExpr,
        Statement as SqlStatement, TableConstraint, TableFactor, TableWithJoins,
    },
    dialect::{keywords::Keyword, Dialect, MySqlDialect},
    parser::{IsOptional::Mandatory, Parser as SqlParser, ParserError},
    tokenizer::{Token, Tokenizer},
};
use table_engine::ANALYTIC_ENGINE_TYPE;

use crate::{
    ast::{
        AlterAddColumn, AlterModifySetting, CreateTable, DescribeTable, DropTable, ExistsTable,
        HashPartition, KeyPartition, Partition, RandomPartition, ShowCreate, ShowCreateObject,
        ShowTables, Statement,
    },
    partition,
};

define_result!(ParserError);

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG))
    };
}

const TS_KEY: &str = "__ts_key";
const TAG: &str = "TAG";
const DICTIONARY: &str = "DICTIONARY";
const UNSIGN: &str = "UNSIGN";
const MODIFY: &str = "MODIFY";
const SETTING: &str = "SETTING";

macro_rules! is_custom_column {
    ($name: ident) => {
        paste! {
            #[inline]
            pub  fn [<is_ $name:lower _column>](opt: &ColumnOption) -> bool {
                match opt {
                    ColumnOption::DialectSpecific(tokens) => {
                        if let [Token::Word(word)] = &tokens[..] {
                            return word.value == $name;
                        }
                    }
                    _ => return false,
                }
                return false;
            }

        }
    };
}

is_custom_column!(TAG);
is_custom_column!(DICTIONARY);
is_custom_column!(UNSIGN);

/// Get the comment from the [`ColumnOption`] if it is a comment option.
#[inline]
pub fn get_column_comment(opt: &ColumnOption) -> Option<String> {
    if let ColumnOption::Comment(comment) = opt {
        return Some(comment.clone());
    }

    None
}

/// Get the default value expr from  [`ColumnOption`] if it is a default-value
/// option.
pub fn get_default_value(opt: &ColumnOption) -> Option<Expr> {
    if let ColumnOption::Default(expr) = opt {
        return Some(expr.clone());
    }

    None
}

/// Returns true when is a TIMESTAMP KEY table constraint
pub fn is_timestamp_key_constraint(constraint: &TableConstraint) -> bool {
    if let TableConstraint::Unique {
        name: Some(Ident {
            value,
            quote_style: None,
        }),
        columns: _,
        is_primary: false,
    } = constraint
    {
        return value == TS_KEY;
    }
    false
}

/// SQL Parser with ceresdb dialect support
pub struct Parser<'a> {
    parser: SqlParser<'a>,
}

impl<'a> Parser<'a> {
    // Parse the specified tokens with dialect
    fn new_with_dialect(sql: &str, dialect: &'a dyn Dialect) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        let parser = SqlParser::new(dialect);

        Ok(Parser {
            parser: parser.with_tokens(tokens),
        })
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
        // Use MySqlDialect, so we can support "`" and chinese characters.
        let dialect = &MySqlDialect {};
        let mut parser = Parser::new_with_dialect(sql, dialect)?;
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token().token);
            }

            let statement = parser.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }

        debug!("Parser parsed sql, sql:{}, stmts:{:#?}", sql, stmts);

        Ok(stmts)
    }

    // Report unexpected token
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T> {
        parser_err!(format!("Expected {expected}, found: {found}"))
    }

    // Parse a new expression
    fn parse_statement(&mut self) -> Result<Statement> {
        match self.parser.peek_token().token {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        // Move one token forward
                        self.parser.next_token();
                        // Use custom parse
                        self.parse_create()
                    }
                    Keyword::DROP => {
                        // Move one token forward
                        self.parser.next_token();
                        // Use custom parse
                        self.parse_drop()
                    }
                    Keyword::DESCRIBE | Keyword::DESC => {
                        self.parser.next_token();
                        self.parse_describe()
                    }
                    Keyword::ALTER => {
                        self.parser.next_token();
                        self.parse_alter()
                    }
                    Keyword::SHOW => {
                        self.parser.next_token();
                        self.parse_show()
                    }
                    Keyword::EXISTS => {
                        self.parser.next_token();
                        self.parse_exists()
                    }
                    _ => {
                        // use the native parser
                        let mut statement = self.parser.parse_statement()?;
                        maybe_normalize_table_name(&mut statement);
                        Ok(Statement::Standard(Box::new(statement)))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(Statement::Standard(Box::new(
                    self.parser.parse_statement()?,
                )))
            }
        }
    }

    pub fn parse_alter(&mut self) -> Result<Statement> {
        let nth1_token = self.parser.peek_token().token;
        let nth2_token = self.parser.peek_nth_token(2).token;
        let nth3_token = self.parser.peek_nth_token(3).token;
        if let (Token::Word(nth1_word), Token::Word(nth2_word), Token::Word(nth3_word)) =
            (nth1_token, nth2_token, nth3_token)
        {
            // example: ALTER TABLE test_ttl modify SETTING ttl='8d'
            if let (Keyword::TABLE, MODIFY, SETTING) = (
                nth1_word.keyword,
                nth2_word.value.to_uppercase().as_str(),
                nth3_word.value.to_uppercase().as_str(),
            ) {
                return self.parse_alter_modify_setting();
            }
            // examples:
            // ALTER TABLE test_table ADD COLUMN col_17 STRING TAG
            // ALTER TABLE test_table ADD COLUMN (col_18 STRING TAG, col_19 UNIT64)
            if let (Keyword::TABLE, Keyword::ADD, Keyword::COLUMN) =
                (nth1_word.keyword, nth2_word.keyword, nth3_word.keyword)
            {
                return self.parse_alter_add_column();
            }
        }
        Ok(Statement::Standard(Box::new(self.parser.parse_alter()?)))
    }

    pub fn parse_show(&mut self) -> Result<Statement> {
        if self.consume_token("TABLES") {
            Ok(self.parse_show_tables()?)
        } else if self.consume_token("DATABASES") {
            Ok(Statement::ShowDatabases)
        } else if self.consume_token("CREATE") {
            Ok(self.parse_show_create()?)
        } else {
            self.expected("create/tables/databases", self.parser.peek_token().token)
        }
    }

    fn parse_show_tables(&mut self) -> Result<Statement> {
        let pattern = match self.parser.next_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => Some(self.parser.parse_literal_string()?),
                _ => return self.expected("like", self.parser.peek_token().token),
            },
            Token::SemiColon | Token::EOF => None,
            _ => return self.expected(";", self.parser.peek_token().token),
        };
        Ok(Statement::ShowTables(ShowTables { pattern }))
    }

    fn parse_show_create(&mut self) -> Result<Statement> {
        let obj_type = match self.parser.expect_one_of_keywords(&[Keyword::TABLE])? {
            Keyword::TABLE => Ok(ShowCreateObject::Table),
            keyword => parser_err!(format!(
                "Unable to map keyword to ShowCreateObject: {keyword:?}"
            )),
        }?;

        let table_name = self.parser.parse_object_name()?.into();

        Ok(Statement::ShowCreate(ShowCreate {
            obj_type,
            table_name,
        }))
    }

    fn parse_alter_add_column(&mut self) -> Result<Statement> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parser.parse_object_name()?.into();
        self.parser
            .expect_keywords(&[Keyword::ADD, Keyword::COLUMN])?;
        let (mut columns, _) = self.parse_columns()?;
        if columns.is_empty() {
            let column_def = self.parse_column_def()?;
            columns.push(column_def);
        }
        Ok(Statement::AlterAddColumn(AlterAddColumn {
            table_name,
            columns,
        }))
    }

    fn parse_alter_modify_setting(&mut self) -> Result<Statement> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parser.parse_object_name()?.into();
        if self.consume_token(MODIFY) && self.consume_token(SETTING) {
            let options = self
                .parser
                .parse_comma_separated(SqlParser::parse_sql_option)?;
            Ok(Statement::AlterModifySetting(AlterModifySetting {
                table_name,
                options,
            }))
        } else {
            unreachable!()
        }
    }

    pub fn parse_describe(&mut self) -> Result<Statement> {
        let _ = self.parser.parse_keyword(Keyword::TABLE);
        let table_name = self.parser.parse_object_name()?.into();
        Ok(Statement::Describe(DescribeTable { table_name }))
    }

    // Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?.into();
        let (columns, constraints) = self.parse_columns()?;

        // Parse the partition clause, starting with `PARTITION BY ...`
        let partition = self.maybe_parse_partition(Keyword::PARTITION, &columns)?;

        // ENGINE = ...
        let engine = self.parse_table_engine()?;

        // WITH ...
        let options = self.parser.parse_options(Keyword::WITH)?;

        // Only String Column Can Be Dictionary Encoded
        for c in &columns {
            let mut is_dictionary = false;
            for op in &c.options {
                if is_dictionary_column(&op.option) {
                    is_dictionary = true;
                }
            }
            if c.data_type != DataType::String && is_dictionary {
                return parser_err!(format!(
                    "Only string column can be dictionary encoded: {:?}",
                    c.to_string()
                ));
            }
        }

        Ok(Statement::Create(Box::new(CreateTable {
            if_not_exists,
            table_name,
            columns,
            engine,
            constraints,
            options,
            partition,
        })))
    }

    pub fn parse_drop(&mut self) -> Result<Statement> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?.into();
        let engine = self.parse_table_engine()?;

        Ok(Statement::Drop(DropTable {
            table_name,
            if_exists,
            engine,
        }))
    }

    pub fn parse_exists(&mut self) -> Result<Statement> {
        let _ = self.parser.parse_keyword(Keyword::TABLE);
        let table_name = self.parser.parse_object_name()?.into();
        Ok(Statement::Exists(ExistsTable { table_name }))
    }

    // Copy from sqlparser
    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>)> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok((Vec::new(), constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token().token {
                columns.push(self.parse_column_def()?);
            } else {
                return self.expected(
                    "column name or constraint definition",
                    self.parser.peek_token().token,
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token().token,
                );
            }
        }

        build_timestamp_key_constraint(&columns, &mut constraints);

        Ok((columns, constraints))
    }

    /// Parses the set of valid formats
    fn parse_table_engine(&mut self) -> Result<String> {
        // TODO make ENGINE as a keyword
        if !self.consume_token("ENGINE") {
            return Ok(ANALYTIC_ENGINE_TYPE.to_string());
        }

        self.parser.expect_token(&Token::Eq)?;

        match self.parser.next_token().token {
            Token::Word(w) => Ok(w.value),
            unexpected => self.expected("Engine is missing", unexpected),
        }
    }

    // Copy from sqlparser
    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.parser.parse_identifier()?;
        let data_type = self.parser.parse_data_type()?;
        let collation = if self.parser.parse_keyword(Keyword::COLLATE) {
            Some(self.parser.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parser.parse_identifier()?);
                if let Some(option) = self.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.parser.peek_token().token,
                    );
                }
            } else if let Some(option) = self.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    // Copy from sqlparser by boyan
    fn parse_optional_table_constraint(&mut self) -> Result<Option<TableConstraint>> {
        let name = if self.parser.parse_keyword(Keyword::CONSTRAINT) {
            Some(self.parser.parse_identifier()?)
        } else {
            None
        };
        match self.parser.next_token().token {
            Token::Word(w) if w.keyword == Keyword::PRIMARY => {
                self.parser.expect_keyword(Keyword::KEY)?;
                let columns = self
                    .parser
                    .parse_parenthesized_column_list(Mandatory, false)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary: true,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::TIMESTAMP => {
                self.parser.expect_keyword(Keyword::KEY)?;
                let columns = self
                    .parser
                    .parse_parenthesized_column_list(Mandatory, false)?;
                // TODO(boyan), TableConstraint doesn't support dialect right now
                // we use unique constraint as TIMESTAMP KEY constraint.
                Ok(Some(TableConstraint::Unique {
                    name: Some(Ident {
                        value: TS_KEY.to_owned(),
                        quote_style: None,
                    }),
                    columns,
                    is_primary: false,
                }))
            }
            unexpected => {
                if name.is_some() {
                    self.expected("PRIMARY, TIMESTAMP", unexpected)
                } else {
                    self.parser.prev_token();
                    Ok(None)
                }
            }
        }
    }

    // Copy from sqlparser  by boyan
    fn parse_optional_column_option(&mut self) -> Result<Option<ColumnOption>> {
        if self.parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            Ok(Some(ColumnOption::NotNull))
        } else if self.parser.parse_keyword(Keyword::NULL) {
            Ok(Some(ColumnOption::Null))
        } else if self.parser.parse_keyword(Keyword::DEFAULT) {
            Ok(Some(ColumnOption::Default(self.parser.parse_expr()?)))
        } else if self
            .parser
            .parse_keywords(&[Keyword::PRIMARY, Keyword::KEY])
        {
            Ok(Some(ColumnOption::Unique { is_primary: true }))
        } else if self
            .parser
            .parse_keywords(&[Keyword::TIMESTAMP, Keyword::KEY])
        {
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword(TS_KEY),
            ])))
        } else if self.consume_token(TAG) {
            // Support TAG for ceresdb
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword(TAG),
            ])))
        } else if self.consume_token(DICTIONARY) {
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword(DICTIONARY),
            ])))
        } else if self.consume_token(UNSIGN) {
            // Support unsign for ceresdb
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword(UNSIGN),
            ])))
        } else if self.parser.parse_keyword(Keyword::COMMENT) {
            Ok(Some(ColumnOption::Comment(
                self.parser.parse_literal_string()?,
            )))
        } else {
            Ok(None)
        }
    }

    fn maybe_parse_partition(
        &mut self,
        keyword: Keyword,
        columns: &[ColumnDef],
    ) -> Result<Option<Partition>> {
        // PARTITION BY ...
        if !self.parser.parse_keyword(keyword) {
            return Ok(None);
        }
        self.parser.expect_keyword(Keyword::BY)?;

        // Parse partition strategy.
        self.parse_and_check_partition_strategy(columns)
    }

    fn parse_and_check_partition_strategy(
        &mut self,
        columns: &[ColumnDef],
    ) -> Result<Option<Partition>> {
        if let Some(key) = self.maybe_parse_and_check_random_partition()? {
            return Ok(Some(Partition::Random(key)));
        }

        if let Some(key) = self.maybe_parse_and_check_key_partition(columns)? {
            return Ok(Some(Partition::Key(key)));
        }
        if let Some(hash) = self.maybe_parse_and_check_hash_partition(columns)? {
            return Ok(Some(Partition::Hash(hash)));
        }

        Ok(None)
    }

    fn maybe_parse_and_check_random_partition(&mut self) -> Result<Option<RandomPartition>> {
        if !self.consume_token("RANDOM") {
            return Ok(None);
        }

        // Parse the clause `PARTITIONS ...`.
        let partition_num = self.parse_partition_num()?.unwrap_or(1);
        Ok(Some(RandomPartition { partition_num }))
    }

    fn maybe_parse_and_check_hash_partition(
        &mut self,
        columns: &[ColumnDef],
    ) -> Result<Option<HashPartition>> {
        // Parse first part: "PARTITION BY HASH(expr)".
        let linear = if self.consume_token("HASH") {
            false
        } else if self.consume_tokens(&["LINEAR", "HASH"]) {
            true
        } else {
            return Ok(None);
        };

        // TODO: support all valid exprs not only column expr.
        let expr = self.parse_and_check_expr_in_hash(columns)?;

        let partition_num = self.parse_partition_num()?.unwrap_or(1);

        // Parse successfully.
        Ok(Some(HashPartition {
            linear,
            partition_num,
            expr,
        }))
    }

    fn maybe_parse_and_check_key_partition(
        &mut self,
        columns: &[ColumnDef],
    ) -> Result<Option<KeyPartition>> {
        let linear = if self.consume_token("KEY") {
            false
        } else if self.consume_tokens(&["LINEAR", "KEY"]) {
            true
        } else {
            return Ok(None);
        };

        let key_columns = self
            .parser
            .parse_parenthesized_column_list(Mandatory, false)
            .map_err(|e| {
                ParserError::ParserError(format!("Fail to parse partition key, err:{e}"))
            })?;

        // Ensure at least one column for partition key.
        if key_columns.is_empty() {
            return parser_err!(
                "except at least one partition key, default partition key is unsupported now"
                    .to_string()
            );
        }

        // Validate all columns composing partition key:
        //  - The column must exist;
        //  - The column must be a tag;
        for key_col in &key_columns {
            let col_def = match columns.iter().find(|c| c.name.value == key_col.value) {
                Some(v) => v,
                None => {
                    return parser_err!(format!(
                        "partition key contains non-existent column:{}",
                        key_col.value,
                    ))
                }
            };
            let tag_column = col_def.options.iter().any(|opt| is_tag_column(&opt.option));
            if !tag_column {
                return parser_err!(format!(
                    "partition key must be tag, key name:{:?}",
                    key_col.value
                ));
            }
        }

        let partition_num = self.parse_partition_num()?.unwrap_or(1);
        let partition_key = key_columns.into_iter().map(|v| v.value).collect();

        // Parse successfully.
        Ok(Some(KeyPartition {
            linear,
            partition_num,
            partition_key,
        }))
    }

    // Parse second part: "PARTITIONS num".
    //
    // If not found, return `Ok(None)`.
    fn parse_partition_num(&mut self) -> Result<Option<u64>> {
        let partition_num = if self.parser.parse_keyword(Keyword::PARTITIONS) {
            match self.parser.parse_number_value()? {
                sqlparser::ast::Value::Number(v, _) => match v.parse::<u64>() {
                    Ok(v) => v,
                    Err(e) => {
                        return parser_err!(format!("invalid partition num, raw:{v}, err:{e}"))
                    }
                },
                v => return parser_err!(format!("expect partition number, found:{v}")),
            }
        } else {
            return Ok(None);
        };

        if partition_num > partition::MAX_PARTITION_NUM {
            parser_err!(format!(
                "partition num must be <= MAX_PARTITION_NUM, MAX_PARTITION_NUM:{}, set partition num:{}",
                partition::MAX_PARTITION_NUM, partition_num
            ))
        } else {
            Ok(Some(partition_num))
        }
    }

    fn parse_and_check_expr_in_hash(&mut self, columns: &[ColumnDef]) -> Result<Expr> {
        let expr = self.parser.parse_expr()?;
        if let Expr::Nested(inner) = expr {
            match inner.as_ref() {
                Expr::Identifier(id) => {
                    if check_column_expr_validity_in_hash(id, columns) {
                        Ok(*inner)
                    } else {
                        parser_err!(format!("Expect column(tag, type: int, tiny int, small int, big int), search by column name:{id}"))
                    }
                },

                other => parser_err!(
                    format!("Only column expr in hash partition now, example: HASH(column name), found:{other:?}")
                ),
            }
        } else {
            parser_err!(format!("Expect nested expr, found:{expr:?}"))
        }
    }

    fn consume_token(&mut self, expected: &str) -> bool {
        if self.parser.peek_token().to_string().to_uppercase() == *expected.to_uppercase() {
            self.parser.next_token();
            true
        } else {
            false
        }
    }

    fn consume_tokens(&mut self, expected_tokens: &[&str]) -> bool {
        for expected in expected_tokens {
            if !self.consume_token(expected) {
                return false;
            }
        }
        true
    }
}

// Valid column expr in hash should meet following conditions:
// 1. column must be a tag, tsid + timestamp can be seen as the combined unique
// key, and partition key must be the subset of it(for supporting overwritten
// insert mode). About the reason, maybe you can see: https://docs.pingcap.com/zh/tidb/stable/partitioned-table
//
// 2. column's datatype must be integer(int, tiny int, small int, big int ...).
//
// TODO: we should consider the situation: no tag column is set.
fn check_column_expr_validity_in_hash(column: &Ident, columns: &[ColumnDef]) -> bool {
    let valid_column = columns.iter().find(|col| {
        if col.name == *column {
            let is_integer = matches!(
                col.data_type,
                DataType::Int(_)
                    | DataType::TinyInt(_)
                    | DataType::SmallInt(_)
                    | DataType::BigInt(_)
                    | DataType::UnsignedInt(_)
                    | DataType::UnsignedTinyInt(_)
                    | DataType::UnsignedSmallInt(_)
                    | DataType::UnsignedBigInt(_)
            );
            let tag_column = col.options.iter().any(|opt| is_tag_column(&opt.option));
            is_integer && tag_column
        } else {
            false
        }
    });

    valid_column.is_some()
}

// Build the tskey constraint from the column definitions if any.
fn build_timestamp_key_constraint(col_defs: &[ColumnDef], constraints: &mut Vec<TableConstraint>) {
    for col_def in col_defs {
        for col in &col_def.options {
            if let ColumnOption::DialectSpecific(tokens) = &col.option {
                if let [Token::Word(token)] = &tokens[..] {
                    if token.value.eq(TS_KEY) {
                        let constraint = TableConstraint::Unique {
                            name: Some(Ident {
                                value: TS_KEY.to_owned(),
                                quote_style: None,
                            }),
                            columns: vec![col_def.name.clone()],
                            is_primary: false,
                        };
                        constraints.push(constraint);
                    }
                }
            };
        }
    }
}

/// Add quotes in table name (for example: convert table to `table`).
///
/// It is used to process table name in `SELECT`, for preventing `datafusion`
/// converting the table name to lowercase, because `CeresDB` only support
/// case-sensitive in sql.
// TODO: maybe other items(such as: alias, column name) need to be normalized,
// too.
pub fn maybe_normalize_table_name(statement: &mut SqlStatement) {
    if let SqlStatement::Query(query) = statement {
        if let SetExpr::Select(select) = query.body.as_mut() {
            select.from.iter_mut().for_each(maybe_convert_one_from);
        }
    }
}

fn maybe_convert_one_from(one_from: &mut TableWithJoins) {
    let TableWithJoins { relation, joins } = one_from;
    maybe_convert_relation(relation);
    joins.iter_mut().for_each(|join| {
        maybe_convert_relation(&mut join.relation);
    });
}

fn maybe_convert_relation(relation: &mut TableFactor) {
    if let TableFactor::Table { name, .. } = relation {
        maybe_convert_table_name(name);
    }
}

fn maybe_convert_table_name(object_name: &mut ObjectName) {
    object_name.0.iter_mut().for_each(|id| {
        if id.quote_style.is_none() {
            let _ = std::mem::replace(id, Ident::with_quote('`', id.value.clone()));
        }
    })
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::{ColumnOptionDef, DataType, Ident, ObjectName, TimezoneInfo, Value},
        parser::ParserError::ParserError,
    };

    use super::*;
    use crate::ast::TableName;

    fn expect_parse_ok(sql: &str, expected: Statement) -> Result<()> {
        let statements = Parser::parse_sql(sql)?;
        assert_eq!(
            statements.len(),
            1,
            "Expected to parse exactly one statement"
        );
        assert_eq!(statements[0], expected);
        Ok(())
    }

    /// Parses sql and asserts that the expected error message was found
    fn expect_parse_error(sql: &str, expected_error: &str) {
        match Parser::parse_sql(sql) {
            Ok(statements) => {
                panic!("Expected parse error for '{sql}', but was successful: {statements:?}");
            }
            Err(e) => {
                let error_message = e.to_string();
                assert!(
                    error_message.contains(expected_error),
                    "Expected error '{expected_error}' not found in actual error '{error_message}'"
                );
            }
        }
    }

    fn make_column_def(name: impl Into<String>, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident {
                value: name.into(),
                quote_style: None,
            },
            data_type,
            collation: None,
            options: vec![],
        }
    }

    fn make_tag_column_def(name: impl Into<String>, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident {
                value: name.into(),
                quote_style: None,
            },
            data_type,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::DialectSpecific(vec![Token::make_keyword(TAG)]),
            }],
        }
    }

    fn make_comment_column_def(
        name: impl Into<String>,
        data_type: DataType,
        comment: String,
    ) -> ColumnDef {
        ColumnDef {
            name: Ident {
                value: name.into(),
                quote_style: None,
            },
            data_type,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::Comment(comment),
            }],
        }
    }

    fn make_table_name(name: impl Into<String>) -> TableName {
        ObjectName(vec![Ident::new(name)]).into()
    }

    #[test]
    fn create_table() {
        // positive case
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 double)";
        let expected = Statement::Create(Box::new(CreateTable {
            if_not_exists: true,
            table_name: make_table_name("t"),
            columns: vec![make_column_def("c1", DataType::Double)],
            engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
            constraints: vec![],
            options: vec![],
            partition: None,
        }));
        expect_parse_ok(sql, expected).unwrap();

        // positive case, multiple columns
        let sql = "CREATE TABLE mytbl(c1 timestamp, c2 double, c3 string,) ENGINE = XX";
        let expected = Statement::Create(Box::new(CreateTable {
            if_not_exists: false,
            table_name: make_table_name("mytbl"),
            columns: vec![
                make_column_def("c1", DataType::Timestamp(None, TimezoneInfo::None)),
                make_column_def("c2", DataType::Double),
                make_column_def("c3", DataType::String),
            ],
            engine: "XX".to_string(),
            constraints: vec![],
            options: vec![],
            partition: None,
        }));
        expect_parse_ok(sql, expected).unwrap();

        // positive case, multiple columns with comment
        let sql = "CREATE TABLE mytbl(c1 timestamp, c2 double comment 'id', c3 string comment 'name',) ENGINE = XX";
        let expected = Statement::Create(Box::new(CreateTable {
            if_not_exists: false,
            table_name: make_table_name("mytbl"),
            columns: vec![
                make_column_def("c1", DataType::Timestamp(None, TimezoneInfo::None)),
                make_comment_column_def("c2", DataType::Double, "id".to_string()),
                make_comment_column_def("c3", DataType::String, "name".to_string()),
            ],
            engine: "XX".to_string(),
            constraints: vec![],
            options: vec![],
            partition: None,
        }));
        expect_parse_ok(sql, expected).unwrap();

        // Error cases: Invalid sql
        let sql = "CREATE TABLE t(c1 timestamp) AS";
        expect_parse_error(
            sql,
            "sql parser error: Expected end of statement, found: AS",
        );
    }

    #[test]
    fn test_unsign_tag_column() {
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 string tag, c2 float, c3 bigint unsign)";
        let statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Create(v) => {
                let columns = &v.columns;
                assert_eq!(3, columns.len());
                for c in columns {
                    if c.name.value == "c1" {
                        assert_eq!(1, c.options.len());
                        let opt = &c.options[0];
                        assert!(is_tag_column(&opt.option));
                    } else if c.name.value == "c2" {
                        assert_eq!(0, c.options.len());
                    } else if c.name.value == "c3" {
                        assert_eq!(1, c.options.len());
                        let opt = &c.options[0];
                        assert!(is_unsign_column(&opt.option));
                    } else {
                        panic!("failed");
                    }
                }
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_dictionary_column() {
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 string tag dictionary, c2 float dictionary, c3 bigint unsign)";
        assert!(Parser::parse_sql(sql).is_err());
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 string tag dictionary, c2 string dictionary, c3 bigint unsign)";
        let statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Create(v) => {
                let columns = &v.columns;
                assert_eq!(3, columns.len());
                for c in columns {
                    if c.name.value == "c1" {
                        assert_eq!(2, c.options.len());
                        let opt = &c.options[0];
                        assert!(is_tag_column(&opt.option));
                        let opt = &c.options[1];
                        assert!(is_dictionary_column(&opt.option));
                    } else if c.name.value == "c2" {
                        assert_eq!(1, c.options.len());
                        let opt = &c.options[0];
                        assert!(is_dictionary_column(&opt.option));
                    } else if c.name.value == "c3" {
                        assert_eq!(1, c.options.len());
                        let opt = &c.options[0];
                        assert!(is_unsign_column(&opt.option));
                    } else {
                        panic!("failed");
                    }
                }
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_dictionary_use_unstring_column() {
        let sql =
            "CREATE TABLE IF NOT EXISTS t(c1 string tag, c2 float dictionary, c3 bigint unsign)";
        assert!(Parser::parse_sql(sql).is_err());
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 string tag dictionary, c2 float dictionary, c3 bigint unsign)";
        assert!(Parser::parse_sql(sql).is_err());
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 string tag, c2 float dictionary, c3 bigint unsign dictionary)";
        assert!(Parser::parse_sql(sql).is_err());
    }

    #[test]
    fn test_comment_column() {
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 string, c2 float, c3 bigint comment 'id')";
        let statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Create(v) => {
                let columns = &v.columns;
                assert_eq!(3, columns.len());
                for c in columns {
                    if c.name.value == "c3" {
                        assert_eq!(1, c.options.len());
                        let opt = &c.options[0];
                        let comment = get_column_comment(&opt.option).unwrap();
                        assert_eq!("id", comment);
                    }
                }
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_timestamp_key_constraint() {
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 TIMESTAMP, TIMESTAMP key(c1))";
        let statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Create(v) => {
                let constraints = &v.constraints;
                assert_eq!(1, constraints.len());
                assert!(is_timestamp_key_constraint(&constraints[0]));
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_create_table_engine() {
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 double)";
        let statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Create(v) => {
                assert_eq!(v.engine, table_engine::ANALYTIC_ENGINE_TYPE.to_string())
            }
            _ => panic!("failed"),
        }

        let sql = "CREATE TABLE IF NOT EXISTS t(c1 double) ENGINE = XX";
        let statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Create(v) => assert_eq!(v.engine, "XX".to_string()),
            _ => panic!("failed"),
        }

        let sql = "CREATE TABLE IF NOT EXISTS t(c1 double) engine = XX2";
        let statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Create(v) => assert_eq!(v.engine, "XX2".to_string()),
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_alter_table_option() {
        let sql = "ALTER TABLE test_ttl modify SETTING arena_block_size='1k';";
        let statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::AlterModifySetting(v) => {
                assert_eq!(v.table_name.to_string(), "test_ttl".to_string());
                assert_eq!(v.options.len(), 1);
                assert_eq!(v.options[0].name.value, "arena_block_size".to_string());
                assert_eq!(
                    v.options[0].value,
                    Value::SingleQuotedString("1k".to_string())
                );
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_alter_table_column() {
        {
            let sql = "ALTER TABLE t ADD COLUMN (c1 DOUBLE, c2 STRING)";
            let expected = Statement::AlterAddColumn(AlterAddColumn {
                table_name: make_table_name("t"),
                columns: vec![
                    make_column_def("c1", DataType::Double),
                    make_column_def("c2", DataType::String),
                ],
            });
            expect_parse_ok(sql, expected).unwrap();
        }

        {
            let sql = "ALTER TABLE t ADD COLUMN c1 DOUBLE";
            let expected = Statement::AlterAddColumn(AlterAddColumn {
                table_name: make_table_name("t"),
                columns: vec![make_column_def("c1", DataType::Double)],
            });
            expect_parse_ok(sql, expected).unwrap();
        }
    }

    #[test]
    fn test_alter_table_tag_column() {
        {
            let sql = "ALTER TABLE t ADD COLUMN (c1 DOUBLE, c2 STRING tag)";
            let expected = Statement::AlterAddColumn(AlterAddColumn {
                table_name: make_table_name("t"),
                columns: vec![
                    make_column_def("c1", DataType::Double),
                    make_tag_column_def("c2", DataType::String),
                ],
            });
            expect_parse_ok(sql, expected).unwrap();
        }

        {
            let sql = "ALTER TABLE t ADD COLUMN c1 string tag";
            let expected = Statement::AlterAddColumn(AlterAddColumn {
                table_name: make_table_name("t"),
                columns: vec![make_tag_column_def("c1", DataType::String)],
            });
            expect_parse_ok(sql, expected).unwrap();
        }
    }

    #[test]
    fn test_drop_table() {
        let sql = "drop table test_ttl";
        let statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Drop(DropTable {
                table_name,
                if_exists,
                engine,
            }) => {
                assert_eq!(table_name.to_string(), "test_ttl".to_string());
                assert!(!if_exists);
                assert_eq!(*engine, ANALYTIC_ENGINE_TYPE.to_string());
            }
            _ => panic!("failed"),
        }

        let sql = "drop table if exists test_ttl";
        let statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Drop(DropTable {
                table_name,
                if_exists,
                engine,
            }) => {
                assert_eq!(table_name.to_string(), "test_ttl".to_string());
                assert!(if_exists);
                assert_eq!(*engine, ANALYTIC_ENGINE_TYPE.to_string());
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_exists_table() {
        {
            let sql = "EXISTS TABLE xxx_table";
            let expected = Statement::Exists(ExistsTable {
                table_name: make_table_name("xxx_table"),
            });
            expect_parse_ok(sql, expected).unwrap();
        }

        {
            let sql = "EXISTS xxx_table";
            let expected = Statement::Exists(ExistsTable {
                table_name: make_table_name("xxx_table"),
            });
            expect_parse_ok(sql, expected).unwrap()
        }
    }

    #[test]
    fn test_show_tables() {
        {
            let sql = "show tables;";
            let statements = Parser::parse_sql(sql).unwrap();
            assert_eq!(statements.len(), 1);
            assert!(matches!(
                statements[0],
                Statement::ShowTables(ShowTables { pattern: None })
            ));
        }

        {
            let sql = "show tables";
            let statements = Parser::parse_sql(sql).unwrap();
            assert_eq!(statements.len(), 1);
            assert!(matches!(
                statements[0],
                Statement::ShowTables(ShowTables { pattern: None })
            ));
        }

        {
            let sql = "show tables like '%abc%'";
            let statements = Parser::parse_sql(sql).unwrap();
            assert_eq!(statements.len(), 1);

            assert!(matches!(
                &statements[0],
                Statement::ShowTables(ShowTables { pattern }) if pattern == &Some("%abc%".to_string())
            ));
        }

        {
            let sql = "show tables '%abc%'";
            assert!(Parser::parse_sql(sql).is_err());
        }
    }

    #[test]
    fn test_normalizing_table_name_in_select() {
        {
            let sql = "select * from testa;";
            let statements = Parser::parse_sql(sql).unwrap();
            assert!(
                if let Statement::Standard(standard_statement) = &statements[0] {
                    let standard_statement_str = format!("{standard_statement}");
                    assert!(standard_statement_str.contains("`testa`"));

                    true
                } else {
                    false
                }
            )
        }

        {
            let sql = "select * from `testa`";
            let statements = Parser::parse_sql(sql).unwrap();
            assert!(
                if let Statement::Standard(standard_statement) = &statements[0] {
                    let standard_statement_str = format!("{standard_statement}");
                    assert!(standard_statement_str.contains("`testa`"));

                    true
                } else {
                    false
                }
            )
        }

        {
            let sql = "select * from `testa` join TEstB join TESTC";
            let statements = Parser::parse_sql(sql).unwrap();
            assert!(
                if let Statement::Standard(standard_statement) = &statements[0] {
                    let standard_statement_str = format!("{standard_statement}");
                    assert!(standard_statement_str.contains("`testa`"));
                    assert!(standard_statement_str.contains("`TEstB`"));
                    assert!(standard_statement_str.contains("`TESTC`"));

                    true
                } else {
                    false
                }
            )
        }
    }

    #[test]
    fn test_hash_partition() {
        HashPartitionTableCases::basic();
        HashPartitionTableCases::default_partitions();
        HashPartitionTableCases::with_defined_engine();
        HashPartitionTableCases::invalid_expr_in_hash();
        HashPartitionTableCases::invalid_partitions_num();
    }

    struct HashPartitionTableCases;

    impl HashPartitionTableCases {
        // Basic
        fn basic() {
            let sql = r#"CREATE TABLE t(c1 string, c2 int TAG, c3 bigint) PARTITION BY HASH(c2) PARTITIONS 4"#;
            let statements = Parser::parse_sql(sql).unwrap();
            assert_eq!(statements.len(), 1);
            match &statements[0] {
                Statement::Create(v) => {
                    if let Some(Partition::Hash(p)) = &v.partition {
                        assert!(!p.linear);
                        assert_eq!(
                            format!("{:?}", p.expr).as_str(),
                            r#"Identifier(Ident { value: "c2", quote_style: None })"#
                        );
                        assert_eq!(p.partition_num, 4);
                    } else {
                        panic!("failed");
                    };
                }
                _ => panic!("failed"),
            }
        }

        fn default_partitions() {
            let sql = r#"CREATE TABLE t(c1 string, c2 int TAG, c3 bigint) PARTITION BY HASH(c2)"#;
            let statements = Parser::parse_sql(sql).unwrap();
            assert_eq!(statements.len(), 1);
            match &statements[0] {
                Statement::Create(v) => {
                    if let Some(Partition::Hash(p)) = &v.partition {
                        assert!(!p.linear);
                        assert_eq!(
                            format!("{:?}", p.expr).as_str(),
                            r#"Identifier(Ident { value: "c2", quote_style: None })"#
                        );
                        assert_eq!(p.partition_num, 1);
                    } else {
                        panic!("failed");
                    };
                }
                _ => panic!("failed"),
            }
        }

        // Partition with defined engine
        fn with_defined_engine() {
            let sql = r#"CREATE TABLE t(c1 string, c2 int TAG, c3 bigint) PARTITION BY HASH(c2) PARTITIONS 4 ENGINE = XX"#;
            let statements = Parser::parse_sql(sql).unwrap();
            assert_eq!(statements.len(), 1);
            match &statements[0] {
                Statement::Create(v) => {
                    if let Some(Partition::Hash(p)) = &v.partition {
                        assert!(!p.linear);
                        assert_eq!(
                            format!("{:?}", p.expr).as_str(),
                            r#"Identifier(Ident { value: "c2", quote_style: None })"#
                        );
                        assert_eq!(p.partition_num, 4);
                    } else {
                        panic!("failed");
                    };
                }
                _ => panic!("failed"),
            }
        }

        // Partition with error in HASH(...), should return error
        fn invalid_expr_in_hash() {
            // Unsupported expr
            let sql = r#"CREATE TABLE t(c1 string, c2 int TAG, c3 bigint TAG) PARTITION BY HASH(c2, c3) PARTITIONS 4"#;
            assert!(
                matches!(Parser::parse_sql(sql), Err(e) if format!("{e:?}").contains("ParserError")
                    && format!("{e:?}").contains("Expect nested expr"))
            );

            // Column of invalid type
            let sql = r#"CREATE TABLE t(c1 string, c2 int, c3 bigint) PARTITION BY HASH(c1) PARTITIONS 4"#;
            assert!(
                matches!(Parser::parse_sql(sql), Err(e) if format!("{e:?}").contains("ParserError")
                    && format!("{e:?}").contains("Expect column"))
            );

            // Column not tag
            let sql = r#"CREATE TABLE t(c1 string, c2 int, c3 bigint) PARTITION BY HASH(c2) PARTITIONS 4"#;
            assert!(
                matches!(Parser::parse_sql(sql), Err(e) if format!("{e:?}").contains("ParserError")
                    && format!("{e:?}").contains("Expect column"))
            );
        }

        // Partitions with a invalid num
        fn invalid_partitions_num() {
            let sql = r#"CREATE TABLE t(c1 string, c2 int TAG, c3 bigint) PARTITION BY HASH(c2) PARTITIONS 'string'"#;
            assert!(
                matches!(Parser::parse_sql(sql), Err(e) if format!("{e:?}").contains("ParserError")
                    && format!("{e:?}").contains("Expected literal number"))
            );
        }
    }

    #[test]
    fn test_key_partition() {
        KeyPartitionTableCases::basic();
        KeyPartitionTableCases::default_key_partition();
        KeyPartitionTableCases::invalid_column_type();
    }

    struct KeyPartitionTableCases;

    impl KeyPartitionTableCases {
        fn basic() {
            let sql = r#"CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) PARTITION BY KEY(name) PARTITIONS 2 ENGINE=Analytic with (enable_ttl="false")"#;
            let stmt = Parser::parse_sql(sql).unwrap();
            assert_eq!(stmt.len(), 1);
            match &stmt[0] {
                Statement::Create(v) => {
                    if let Some(Partition::Key(p)) = &v.partition {
                        assert!(!p.linear);
                        assert_eq!(&p.partition_key[0], "name");
                        assert_eq!(p.partition_num, 2);
                    } else {
                        panic!("failed");
                    };
                }
                _ => panic!("failed"),
            }
        }

        fn default_key_partition() {
            let sql = r#"CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) PARTITION BY KEY() PARTITIONS 2 ENGINE=Analytic with (enable_ttl="false")"#;
            let stmt = Parser::parse_sql(sql);
            assert_eq!(
                stmt.err().unwrap(),
                ParserError(
                    "Fail to parse partition key, err:sql parser error: Expected identifier, found: )".to_string()
                )
            );
        }

        fn invalid_column_type() {
            let sql = r#"CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) PARTITION BY KEY(value) PARTITIONS 2 ENGINE=Analytic with (enable_ttl="false")"#;
            let stmt = Parser::parse_sql(sql);

            assert_eq!(
                stmt.err().unwrap(),
                ParserError(r#"partition key must be tag, key name:"value""#.to_string())
            )
        }
    }

    #[test]
    fn test_partition_num_restriction() {
        let invalid_partition_num = partition::MAX_PARTITION_NUM + 1;
        let invalid_partition_num_sql =
            create_sql_with_partition_num(partition::MAX_PARTITION_NUM + 1);
        let result = Parser::parse_sql(&invalid_partition_num_sql);
        assert_eq!(
            result.err().unwrap(),
            ParserError(format!(
                r#"partition num must be <= MAX_PARTITION_NUM, MAX_PARTITION_NUM:{}, set partition num:{}"#,
                partition::MAX_PARTITION_NUM,
                invalid_partition_num
            ))
        );

        let valid_partition_num = partition::MAX_PARTITION_NUM - 1;
        let valid_partition_num_sql = create_sql_with_partition_num(valid_partition_num);
        let result = Parser::parse_sql(&valid_partition_num_sql);
        assert!(result.is_ok());
    }

    fn create_sql_with_partition_num(partition_num: u64) -> String {
        format!(
            r#"CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL,
            `t` timestamp NOT NULL, TIMESTAMP KEY(t)) PARTITION BY KEY(name) PARTITIONS {partition_num}
            ENGINE=Analytic with (enable_ttl="false")"#
        )
    }
}
