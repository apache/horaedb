// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, sync::RwLock};

use arrow_deps::datafusion::catalog::TableReference;
use sql::plan::Plan;

pub struct Limiter {
    write_reject_list: RwLock<HashSet<String>>,
    read_reject_list: RwLock<HashSet<String>>,
}

impl Default for Limiter {
    fn default() -> Self {
        Self {
            write_reject_list: RwLock::new(HashSet::new()),
            read_reject_list: RwLock::new(HashSet::new()),
        }
    }
}

impl Limiter {
    pub fn should_limit(&self, plan: &Plan) -> bool {
        match plan {
            Plan::Query(query) => self.read_reject_list.read().unwrap().iter().any(|table| {
                query
                    .tables
                    .get(TableReference::from(table.as_str()))
                    .is_some()
            }),
            Plan::Insert(insert) => self
                .write_reject_list
                .read()
                .unwrap()
                .contains(insert.table.name()),
            _ => false,
        }
    }

    pub fn add_write_reject_list(&self, reject_list: Vec<String>) {
        self.write_reject_list
            .write()
            .unwrap()
            .extend(reject_list.into_iter())
    }

    pub fn add_read_reject_list(&self, reject_list: Vec<String>) {
        self.read_reject_list
            .write()
            .unwrap()
            .extend(reject_list.into_iter())
    }

    pub fn set_write_reject_list(&self, reject_list: Vec<String>) {
        *self.write_reject_list.write().unwrap() = reject_list.into_iter().collect();
    }

    pub fn set_read_reject_list(&self, reject_list: Vec<String>) {
        *self.read_reject_list.write().unwrap() = reject_list.into_iter().collect();
    }

    pub fn get_write_reject_list(&self) -> HashSet<String> {
        self.write_reject_list.read().unwrap().clone()
    }

    pub fn get_read_reject_list(&self) -> HashSet<String> {
        self.read_reject_list.read().unwrap().clone()
    }

    pub fn remove_write_reject_list(&self, reject_list: Vec<String>) {
        let mut write_reject_list = self.write_reject_list.write().unwrap();
        for value in reject_list {
            write_reject_list.remove(&value);
        }
    }

    pub fn remove_read_reject_list(&self, reject_list: Vec<String>) {
        let mut read_reject_list = self.read_reject_list.write().unwrap();
        for value in reject_list {
            read_reject_list.remove(&value);
        }
    }
}

#[cfg(test)]
mod tests {
    use common_types::request_id::RequestId;
    use sql::{parser::Parser, plan::Plan, planner::Planner, tests::MockMetaProvider};

    use crate::limiter::Limiter;

    fn sql_to_plan(meta_provider: &MockMetaProvider, sql: &str) -> Plan {
        let planner = Planner::new(meta_provider, RequestId::next_id(), 1);
        let mut statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        planner.statement_to_plan(statements.remove(0)).unwrap()
    }

    fn prepare() -> (MockMetaProvider, Limiter) {
        let mock = MockMetaProvider::default();

        let reject_list = vec!["test_table".to_string()];
        let limiter = Limiter::default();
        limiter.set_read_reject_list(reject_list.clone());
        limiter.set_write_reject_list(reject_list);
        (mock, limiter)
    }

    #[test]
    fn test_limiter() {
        let (mock, limiter) = prepare();
        let query = "select * from test_table";
        let query_plan = sql_to_plan(&mock, query);
        assert!(limiter.should_limit(&query_plan));

        let insert="INSERT INTO test_table(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3')";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(limiter.should_limit(&insert_plan));
    }

    #[test]
    fn test_limiter_remove() {
        let (mock, limiter) = prepare();
        let test_data = vec!["test_table".to_string()];

        let query = "select * from test_table";
        let query_plan = sql_to_plan(&mock, query);
        assert!(limiter.should_limit(&query_plan));

        let insert="INSERT INTO test_table(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3')";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(limiter.should_limit(&insert_plan));

        limiter.remove_write_reject_list(test_data.clone());
        limiter.remove_read_reject_list(test_data);
        assert!(!limiter.should_limit(&query_plan));
        assert!(!limiter.should_limit(&insert_plan));
    }

    #[test]
    fn test_limiter_add() {
        let (mock, limiter) = prepare();
        let test_data = vec!["test_table2".to_string()];

        let query = "select * from test_table2";
        let query_plan = sql_to_plan(&mock, query);
        assert!(!limiter.should_limit(&query_plan));

        let insert="INSERT INTO test_table2(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3')";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(!limiter.should_limit(&insert_plan));

        limiter.add_write_reject_list(test_data.clone());
        limiter.add_read_reject_list(test_data);
        assert!(limiter.should_limit(&query_plan));
        assert!(limiter.should_limit(&insert_plan));
    }

    #[test]
    fn test_limiter_set() {
        let (mock, limiter) = prepare();
        let test_data = vec!["test_table2".to_string()];

        let query = "select * from test_table";
        let query_plan = sql_to_plan(&mock, query);
        assert!(limiter.should_limit(&query_plan));

        let query2 = "select * from test_table2";
        let query_plan2 = sql_to_plan(&mock, query2);
        assert!(!limiter.should_limit(&query_plan2));

        let insert="INSERT INTO test_table(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3')";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(limiter.should_limit(&insert_plan));

        let insert2="INSERT INTO test_table2(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3')";
        let insert_plan2 = sql_to_plan(&mock, insert2);
        assert!(!limiter.should_limit(&insert_plan2));

        limiter.set_read_reject_list(test_data.clone());
        limiter.set_write_reject_list(test_data);
        assert!(!limiter.should_limit(&query_plan));
        assert!(!limiter.should_limit(&insert_plan));
        assert!(limiter.should_limit(&query_plan2));
        assert!(limiter.should_limit(&insert_plan2));
    }
}
