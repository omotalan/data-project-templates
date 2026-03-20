import pytest
import pandas as pd
import numpy as np
import os
import duckdb
from utils.chunk_processor import ChunkProcessor, ABConfig


@pytest.fixture
def sample_csv(tmp_path):
    """100-row test CSV."""
    data = {
        'user_id': np.tile(np.arange(50), 2),
        'event_type': np.random.choice(['view', 'purchase', 'cart'], 100),
        'product_id': np.random.randint(1000, 2000, 100)
    }
    pd.DataFrame(data).to_csv(tmp_path / "tiny_events.csv", index=False)
    return str(tmp_path / "tiny_events.csv")


@pytest.fixture
def sample_chunks(tmp_path):
    """Pre-built Parquet chunks for load tests."""
    chunks = []
    for i in range(3):
        df = pd.DataFrame({
            'user_id': range(i * 10, (i + 1) * 10),
            'event_type': ['purchase' if j % 3 == 0 else 'view' for j in range(10)],
            'product_id': np.random.randint(1000, 2000, 10)
        })
        path = str(tmp_path / f"chunk_{i:03d}.parquet")
        df.to_parquet(path, index=False)
        chunks.append(path)
    return chunks


@pytest.fixture
def processor():
    return ChunkProcessor(chunk_size=50)


def test_chunk_csv_to_parquet(sample_csv, tmp_path, processor):
    """100-row CSV with chunk_size=50 → 2 Parquet files."""
    chunks = processor.chunk_csv_to_parquet(sample_csv, str(tmp_path / "chunks"))

    assert len(chunks) == 2
    assert all(os.path.exists(c) for c in chunks)
    assert all(c.endswith('.parquet') for c in chunks)


def test_load_chunks_to_duckdb(sample_chunks, tmp_path):
    """3 chunks → DuckDB table with correct total rows."""
    db_path = str(tmp_path / "test.duckdb")
    processor = ChunkProcessor()

    processor.load_chunks_to_duckdb(sample_chunks, db_path=db_path, schema='raw', table_name='events')

    con = duckdb.connect(db_path)
    count = con.execute("SELECT COUNT(*) FROM raw.events").fetchone()[0]
    con.close()

    assert count == 30  # 3 chunks × 10 rows


def test_load_chunks_to_duckdb_no_duplicates(sample_chunks, tmp_path):
    """Running load twice does not duplicate rows."""
    db_path = str(tmp_path / "test.duckdb")
    processor = ChunkProcessor()

    processor.load_chunks_to_duckdb(sample_chunks, db_path=db_path)
    processor.load_chunks_to_duckdb(sample_chunks, db_path=db_path)

    con = duckdb.connect(db_path)
    count = con.execute("SELECT COUNT(*) FROM raw.events").fetchone()[0]
    con.close()

    assert count == 30  # Still 30, not 60


def test_run_ab_aggregation(sample_chunks, tmp_path):
    """Known data → correct A/B results and z-test output."""
    db_path = str(tmp_path / "test.duckdb")
    processor = ChunkProcessor()

    # Load raw data
    processor.load_chunks_to_duckdb(sample_chunks, db_path=db_path, schema='raw', table_name='events')

    # Create a minimal fct_funnel mart manually (simulates dbt output)
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS marts")
    con.execute("""
        CREATE TABLE marts.fct_funnel AS
        SELECT
            user_id,
            CASE WHEN user_id % 2 = 0 THEN 'A' ELSE 'B' END AS variant,
            MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS contacted
        FROM raw.events
        GROUP BY 1, 2
    """)
    con.close()

    results = processor.run_ab_aggregation(db_path=db_path)

    assert 'cr_A' in results
    assert 'p_value' in results
    assert 0 <= results['cr_A'] <= 1
    assert 0 <= results['cr_B'] <= 1
    assert 0 <= results['p_value'] <= 1
