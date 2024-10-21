use crate::pgvector::PgVector;
use futures_util::TryStreamExt;
use swiftide_core::{indexing, indexing::EmbeddedField, Persist};
use swiftide_core::{
    querying::{search_strategies::SimilaritySingleEmbedding, states, Query},
    Retrieve,
};
use temp_dir::TempDir;
use testcontainers::{ContainerAsync, GenericImage};

struct TestContext {
    pgv_storage: PgVector,
    _temp_dir: TempDir,
    _pgv_db_container: ContainerAsync<GenericImage>,
}

impl TestContext {
    /// Set up the test context, initializing `PostgreSQL` and `PgVector` storage
    async fn setup() -> Result<Self, Box<dyn std::error::Error>> {
        // Start PostgreSQL container and obtain the connection URL
        let (pgv_db_container, pgv_db_url, temp_dir) = swiftide_test_utils::start_postgres().await;

        tracing::info!("Postgres database URL: {:#?}", pgv_db_url);

        // Configure and build PgVector storage
        let pgv_storage = PgVector::builder()
            .try_connect_to_pool(pgv_db_url, Some(10))
            .await
            .map_err(|err| {
                tracing::error!("Failed to connect to Postgres server: {}", err);
                err
            })?
            .vector_size(384)
            .with_vector(EmbeddedField::Combined)
            .with_metadata("filter")
            .table_name("swiftide_pgvector_test".to_string())
            .build()
            .map_err(|err| {
                tracing::error!("Failed to build PgVector: {}", err);
                err
            })?;

        // Set up PgVector storage (create the table if not exists)
        pgv_storage.setup().await.map_err(|err| {
            tracing::error!("PgVector setup failed: {}", err);
            err
        })?;

        Ok(Self {
            pgv_storage,
            _temp_dir: temp_dir,
            _pgv_db_container: pgv_db_container,
        })
    }
}

#[test_log::test(tokio::test)]
async fn test_persist_setup_no_error_when_table_exists() {
    let test_context = TestContext::setup().await.expect("Test setup failed");

    test_context
        .pgv_storage
        .setup()
        .await
        .expect("PgVector setup should not fail when the table already exists");
}

#[test_log::test(tokio::test)]
async fn test_retrieve_multiple_docs_and_filter() {
    let test_context = TestContext::setup().await.expect("Test setup failed");

    let nodes = vec![
        indexing::Node::new("test_query1").with_metadata(("filter", "true")),
        indexing::Node::new("test_query2").with_metadata(("filter", "true")),
        indexing::Node::new("test_query3").with_metadata(("filter", "false")),
    ]
    .into_iter()
    .map(|node| {
        node.with_vectors([(EmbeddedField::Combined, vec![1.0; 384])]);
        node.to_owned()
    })
    .collect();

    test_context
        .pgv_storage
        .batch_store(nodes)
        .await
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let mut query = Query::<states::Pending>::new("test_query");
    query.embedding = Some(vec![1.0; 384]);

    let search_strategy = SimilaritySingleEmbedding::<()>::default();
    let result = test_context
        .pgv_storage
        .retrieve(&search_strategy, query.clone())
        .await
        .unwrap();

    assert_eq!(result.documents().len(), 3);

    let search_strategy = SimilaritySingleEmbedding::from_filter("filter = \"true\"".to_string());

    let result = test_context
        .pgv_storage
        .retrieve(&search_strategy, query.clone())
        .await
        .unwrap();

    assert_eq!(result.documents().len(), 2);

    let search_strategy = SimilaritySingleEmbedding::from_filter("filter = \"banana\"".to_string());

    let result = test_context
        .pgv_storage
        .retrieve(&search_strategy, query.clone())
        .await
        .unwrap();
    assert_eq!(result.documents().len(), 0);
}
