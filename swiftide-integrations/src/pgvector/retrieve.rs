use crate::pgvector::{PgVector, PgVectorBuilder};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use pgvector::Vector;
use sqlx::{prelude::FromRow, types::Uuid};
use swiftide_core::{
    querying::{search_strategies::SimilaritySingleEmbedding, states, Query},
    Retrieve,
};

#[allow(dead_code)]
#[derive(Debug, Clone, FromRow)]
struct VectorSearchResult {
    id: Uuid,
    chunk: String,
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[async_trait]
impl Retrieve<SimilaritySingleEmbedding<String>> for PgVector {
    #[tracing::instrument]
    async fn retrieve(
        &self,
        search_strategy: &SimilaritySingleEmbedding<String>,
        query_state: Query<states::Pending>,
    ) -> Result<Query<states::Retrieved>> {
        let embedding = query_state
            .embedding
            .as_ref()
            .ok_or_else(|| anyhow!("No embedding for query"))?;
        let embedding = Vector::from(embedding.clone());

        // let pool = self.connection_pool.get_pool().await?;
        let pool = self.connection_pool.get_pool()?;

        let default_columns: Vec<_> = PgVectorBuilder::default_fields()
            .iter()
            .map(|f| f.field_name().to_string())
            .collect();
        let vector_column_name = self.get_vector_column_name()?;

        // Start building the SQL query
        let mut sql = format!(
            "SELECT {} FROM {}",
            default_columns.join(", "),
            self.table_name
        );

        if let Some(filter) = search_strategy.filter() {
            let filter_parts: Vec<&str> = filter.split('=').collect();
            if filter_parts.len() == 2 {
                let key = filter_parts[0].trim();
                let value = filter_parts[1].trim().trim_matches('"');
                tracing::debug!(
                    "Filter being applied: key = {:#?}, value = {:#?}",
                    key,
                    value
                );

                let sql_filter = format!(
                    " WHERE meta_{}->>'{}' = '{}'",
                    PgVector::normalize_field_name(key),
                    key,
                    value
                );
                sql.push_str(&sql_filter);
            } else {
                return Err(anyhow!("Invalid filter format"));
            }
        }

        // Add the ORDER BY clause for vector similarity search
        sql.push_str(&format!(
            " ORDER BY {} <=> $1 LIMIT $2",
            &vector_column_name
        ));

        tracing::debug!("Running retrieve with SQL: {}", sql);

        let top_k = i32::try_from(search_strategy.top_k())
            .map_err(|_| anyhow!("Failed to convert top_k to i32"))?;

        let data: Vec<VectorSearchResult> = sqlx::query_as(&sql)
            .bind(embedding)
            .bind(top_k)
            .fetch_all(&pool)
            .await?;

        let docs = data.into_iter().map(|r| r.chunk).collect();

        Ok(query_state.retrieved_documents(docs))
    }
}

#[async_trait]
impl Retrieve<SimilaritySingleEmbedding> for PgVector {
    async fn retrieve(
        &self,
        search_strategy: &SimilaritySingleEmbedding,
        query: Query<states::Pending>,
    ) -> Result<Query<states::Retrieved>> {
        Retrieve::<SimilaritySingleEmbedding<String>>::retrieve(
            self,
            &search_strategy.into_concrete_filter::<String>(),
            query,
        )
        .await
    }
}
