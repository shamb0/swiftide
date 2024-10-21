//! This module implements the `Persist` trait for the `PgVector` struct.
//! It provides methods for setting up storage, saving individual nodes, and batch-storing multiple nodes.
//! This integration enables the Swiftide project to use `PgVector` as a storage backend.
use crate::pgvector::PgVector;
use anyhow::Result;
use async_trait::async_trait;
use swiftide_core::{
    indexing::{IndexingStream, Node},
    Persist,
};

#[async_trait]
impl Persist for PgVector {
    #[tracing::instrument(skip_all)]
    async fn setup(&self) -> Result<()> {
        tracing::info!("Setting up table {} for PgVector", &self.table_name);

        let mut tx = self.connection_pool.get_pool()?.begin().await?;

        // Create extension
        let sql = "CREATE EXTENSION IF NOT EXISTS vector";
        sqlx::query(sql).execute(&mut *tx).await?;

        // Create table
        let create_table_sql = self.generate_create_table_sql()?;
        tracing::debug!("Executing CREATE TABLE SQL: {}", create_table_sql);
        sqlx::query(&create_table_sql).execute(&mut *tx).await?;

        // Create HNSW index
        let index_sql = self.create_index_sql()?;
        tracing::debug!("Executing CREATE INDEX SQL: {}", index_sql);
        sqlx::query(&index_sql).execute(&mut *tx).await?;

        tx.commit().await?;

        tracing::info!("Table {} setup completed", &self.table_name);
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn store(&self, node: Node) -> Result<Node> {
        let mut nodes = vec![node; 1];
        self.store_nodes(&nodes).await?;

        let node = nodes.swap_remove(0);

        Ok(node)
    }

    #[tracing::instrument(skip_all)]
    async fn batch_store(&self, nodes: Vec<Node>) -> IndexingStream {
        self.store_nodes(&nodes).await.map(|()| nodes).into()
    }

    fn batch_size(&self) -> Option<usize> {
        self.batch_size
    }
}
