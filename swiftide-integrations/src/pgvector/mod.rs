//! This module integrates with the pgvector database, providing functionalities to create and manage vector collections,
//! store data, and optimize indexing for efficient searches.
//!
//! pgvector is utilized in both the `indexing::Pipeline` and `query::Pipeline` modules.

#[cfg(test)]
mod tests;

mod persist;
mod pgv_table_types;
mod retrieve;
use anyhow::Result;
use derive_builder::Builder;
use sqlx::PgPool;
use std::fmt;

use pgv_table_types::{FieldConfig, MetadataConfig, PgDBConnectionPool, VectorConfig};

const DEFAULT_BATCH_SIZE: usize = 50;

/// Represents a Pgvector client with configuration options.
///
/// This struct is used to interact with the Pgvector vector database, providing methods to manage vector collections,
/// store data, and ensure efficient searches. The client can be cloned with low cost as it shares connections.
#[derive(Builder, Clone)]
#[builder(setter(into, strip_option), build_fn(error = "anyhow::Error"))]
pub struct PgVector {
    /// Database connection pool.
    #[builder(default = "PgDBConnectionPool::default()")]
    connection_pool: PgDBConnectionPool,

    /// Table name to store vectors in.
    #[builder(default = "String::from(\"swiftide_pgv_store\")")]
    table_name: String,

    /// Default sizes of vectors. Vectors can also be of different
    /// sizes by specifying the size in the vector configuration.
    vector_size: Option<i32>,

    /// Batch size for storing nodes.
    #[builder(default = "Some(DEFAULT_BATCH_SIZE)")]
    batch_size: Option<usize>,

    /// Field configuration for the Pgvector table, determining the eventual table schema.
    ///
    /// Supports multiple field types; see [`FieldConfig`] for details.
    #[builder(default)]
    fields: Vec<FieldConfig>,
}

impl fmt::Debug for PgVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Access the connection pool synchronously and determine the status.
        let connection_status = self.connection_pool.connection_status();

        f.debug_struct("PgVector")
            .field("table_name", &self.table_name)
            .field("vector_size", &self.vector_size)
            .field("batch_size", &self.batch_size)
            .field("connection_status", &connection_status)
            .finish()
    }
}

impl PgVector {
    /// Creates a new `PgVectorBuilder` instance using the default configuration.
    ///
    /// # Returns
    ///
    /// * `PgVectorBuilder` - A builder instance that can be used to configure
    ///   and construct a `PgVector` object.
    ///
    /// This function returns a default `PgVectorBuilder` that can be customized
    /// using builder methods such as `with_vector`, `with_metadata`, and others.
    pub fn builder() -> PgVectorBuilder {
        PgVectorBuilder::default()
    }

    /// Asynchronously retrieves the database connection pool from the `PgDBConnectionPool`.
    ///
    /// # Returns
    ///
    /// * `Option<&PgPool>` - A reference to the connection pool if it exists, otherwise `None`.
    ///
    /// # Arguments
    ///
    /// None
    ///
    /// # Errors
    ///
    /// This function does not return any errors.
    /// It will always return a `Some` containing a reference to the `PgPool` if available, or `None` if no pool is set.
    pub fn get_pool(&self) -> Result<PgPool> {
        self.connection_pool.get_pool()
    }
}

impl PgVectorBuilder {
    /// Tries to create a `PgVectorBuilder` from a given URL.
    ///
    /// # Arguments
    ///
    /// * `url` - A string slice that holds the URL for the Pgvector client.
    /// * `connection_max` - Optional maximum number of connections.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `PgVectorBuilder` if successful, or an error otherwise.
    ///
    /// # Errors
    ///
    /// Errors if the client fails to build or connect to the database, or if the connection pool is not initialized.
    pub async fn try_connect_to_pool(
        mut self,
        url: impl AsRef<str>,
        connection_max: Option<u32>,
    ) -> Result<Self> {
        let pool = self.connection_pool.clone().unwrap_or_default();

        self.connection_pool = Some(pool.try_connect_to_url(url, connection_max).await?);

        Ok(self)
    }

    /// The `with_vector` function adds a vector configuration to the object.
    ///
    /// # Arguments
    ///
    /// * `config` - A configuration that can be converted into a `VectorConfig`.
    ///
    /// # Returns
    ///
    /// * `&mut Self` - A mutable reference to the current object, allowing method chaining.
    ///
    /// This function will no longer panic.
    pub fn with_vector(&mut self, config: impl Into<VectorConfig>) -> &mut Self {
        // Use `get_or_insert_with` to initialize `fields` if it's `None`
        self.fields
            .get_or_insert_with(Self::default_fields)
            .push(FieldConfig::Vector(config.into()));

        self
    }

    /// Adds a metadata configuration to the object.
    ///
    /// # Arguments
    ///
    /// * `config` - The metadata configuration to add.
    ///
    /// # Returns
    ///
    /// * `&mut Self` - A mutable reference to the current object, allowing method chaining.
    ///
    /// This function will no longer panic.
    pub fn with_metadata(&mut self, config: impl Into<MetadataConfig>) -> &mut Self {
        // Use `get_or_insert_with` to initialize `fields` if it's `None`
        self.fields
            .get_or_insert_with(Self::default_fields)
            .push(FieldConfig::Metadata(config.into()));

        self
    }

    fn default_fields() -> Vec<FieldConfig> {
        vec![FieldConfig::ID, FieldConfig::Chunk]
    }
}
