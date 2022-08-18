// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on the [`cluster::Cluster`].

#![allow(dead_code)]

use ceresdbproto_deps::ceresdbproto::storage::{Route, RouteRequest};
use cluster::ClusterRef;

use crate::{error::Result, route::Router};

pub struct ClusterBasedRouter {
    cluster: ClusterRef,
}

impl ClusterBasedRouter {
    pub fn new(cluster: ClusterRef) -> Self {
        Self { cluster }
    }
}

impl Router for ClusterBasedRouter {
    fn route(&self, _schema: &str, _req: RouteRequest) -> Result<Vec<Route>> {
        todo!();
    }
}
