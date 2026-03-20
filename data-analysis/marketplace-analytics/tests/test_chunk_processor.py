import pytest
import pandas as pd
import numpy as np
import os
import shutil
from utils.chunk_processor import ChunkProcessor, ABConfig

# Create tiny test data
@pytest.fixture
def sample_csv(tmp_path):
    """100-row test CSV for chunking tests."""
    data = {
        'user_id': np.tile(np.arange(50), 2),
        'event_type': np.random.choice(['view', 'purchase', 'cart'], 100),
        'product_id': np.random.randint(1000, 2000, 100)
    }
    df = pd.DataFrame(data)
    csv_path = tmp_path / "tiny_events.csv"
    df.to_csv(csv_path, index=False)
    return str(csv_path)

@pytest.fixture
def sample_parquet(tmp_path):
    """Single test Parquet chunk."""
    data = pd.DataFrame({
        'user_id': [1, 2, 3, 4, 1, 2],
        'event_type': ['view', 'purchase', 'view', 'cart', 'purchase', 'view'],
        'product_id': [1001, 1002, 1003, 1001, 1002, 1003]
    })
    parquet_path = tmp_path / "test_chunk.parquet"
    data.to_parquet(parquet_path, index=False)
    return str(parquet_path)

@pytest.fixture
def processor():
    """Default processor."""
    return ChunkProcessor(chunk_size=50)  # Small chunks for tests

def test_chunk_csv_to_parquet(sample_csv, tmp_path, processor):
    """10k CSV → 2 Parquet chunks created."""
    chunks = processor.chunk_csv_to_parquet(sample_csv, str(tmp_path / "chunks"))
    
    assert len(chunks) == 3  # 100 rows / 50 = 2 + partial
    assert all(os.path.exists(c) for c in chunks)
    assert all(c.endswith('.parquet') for c in chunks)
    
    # Cleanup
    shutil.rmtree(tmp_path / "chunks")

def test_aggregate_chunk_ab_default(sample_parquet, processor):
    """Default config → expected A/B counts."""
    result = processor.aggregate_chunk_ab(sample_parquet)
    
    # Expected: users 1,3=A (odd); 2,4=B (even)
    # 2 purchases total (events 1,4)
    assert result['users_A'] == 2
    assert result['users_B'] == 2
    assert result['conv_A'] == 1  # user 1 purchase
    assert result['conv_B'] == 1  # user 2 purchase

def test_aggregate_chunk_ab_custom_config(sample_parquet):
    """Custom ABConfig changes results."""
    custom_config = ABConfig(
        conversion_event='view',  # Different success event
        variant_assignment=lambda uid: 'test' if uid <= 2 else 'control'
    )
    processor = ChunkProcessor(ab_config=custom_config)
    
    result = processor.aggregate_chunk_ab(sample_parquet)
    # Now 'view' events = success, different split
    assert result['conv_A'] > 0  # test group has views
    assert result['conv_B'] > 0  # control has views

def test_validate_representativeness_pass(tmp_path, processor):
    """Identical chunks → pass validation."""
    # Create 3 identical chunks
    data = pd.DataFrame({'user_id': range(100), 'event_type': ['view']*100})
    for i in range(3):
        data.to_parquet(tmp_path / f"chunk_{i:03d}.parquet")
    chunks = [str(tmp_path / f"chunk_{i:03d}.parquet") for i in range(3)]
    
    validation = processor.validate_chunk_representativeness(chunks)
    assert all(len(issues) == 0 for _, issues in validation)

def test_validate_representativeness_fail(tmp_path, processor):
    """Drifted chunks → correctly flagged."""
    # Chunk 0: baseline
    pd.DataFrame({'user_id': range(100), 'event_type': ['view']*100}).to_parquet(tmp_path / "chunk_000.parquet")
    
    # Chunk 1: different event dist
    drifted = pd.DataFrame({'user_id': range(100), 'event_type': ['purchase']*100})
    drifted.to_parquet(tmp_path / "chunk_001.parquet")
    
    chunks = [str(tmp_path / f"chunk_{i:03d}.parquet") for i in range(2)]
    validation = processor.validate_chunk_representativeness(chunks, tolerance=0.05)
    
    assert len(validation[0][1]) > 0  # Chunk 1 should have issues
    assert "Event 'view' drift" in " ".join(validation[0][1])

def test_pool_ztest_known_input():
    """Known aggregates → correct z-test math."""
    processor = ChunkProcessor()
    aggregates = pd.DataFrame({
        'users_A': [25000, 25000],
        'users_B': [25000, 25000],
        'conv_A': [1250, 1250],
        'conv_B': [1300, 1275]
    })
    
    results = processor.pool_and_ztest(aggregates)
    
    # Expected: 5% vs 5.2%, small uplift, p~0.13 (not sig)
    assert abs(results['cr_A'] - 0.05) < 0.001
    assert abs(results['cr_B'] - 0.052) < 0.001
    assert results['p_value'] > 0.05  # Not significant