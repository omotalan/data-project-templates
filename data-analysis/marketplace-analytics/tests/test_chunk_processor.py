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


def test_run_ab_aggregation(tmp_path):
    """Minimal DuckDB setup → correct structure and sane metrics."""
    db_path = str(tmp_path / "test.duckdb")
    processor = ChunkProcessor()

    # Build a tiny intermediate.int_funnel_flagged directly
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS intermediate")

    # Two users, one in each variant; only B purchases
    # This is enough to exercise both user and session branches
    con.execute("""
        CREATE TABLE intermediate.int_funnel_flagged AS
        SELECT * FROM (
            VALUES
                (1, 's1', DATE '2025-01-01', 'A', 'A', 1, 0, 0, 2),
                (2, 's2', DATE '2025-01-01', 'B', 'B', 1, 0, 1, 2)
        ) AS t(
            user_id,
            user_session,
            event_date,
            variant_user,
            variant_session,
            viewed,
            added_to_cart,
            purchased,
            total_events
        )
    """)
    con.close()

    # Act
    results = processor.run_ab_aggregation(db_path=db_path, alpha=0.05)

    # Top-level shape
    assert set(results.keys()) == {"users", "sessions"}

    for level in ("users", "sessions"):
        res = results[level]

        # Required keys
        for key in [
            "population_A", "population_B",
            "conversions_A", "conversions_B",
            "cr_A", "cr_B",
            "absolute_uplift", "relative_uplift",
            "ci_low", "ci_high",
            "mde", "underpowered",
            "z_stat", "p_value",
            "significant", "alpha",
            "srm_detected", "srm_p_value",
            "date_from", "date_to",
        ]:
            assert key in res

        # Sanity checks on ranges and consistency
        assert 0 <= res["cr_A"] <= 1
        assert 0 <= res["cr_B"] <= 1
        assert 0 <= res["p_value"] <= 1
        assert res["ci_low"] <= res["absolute_uplift"] <= res["ci_high"]
        assert res["alpha"] == 0.05