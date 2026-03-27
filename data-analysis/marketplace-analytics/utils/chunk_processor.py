import pandas as pd
import numpy as np
import os
import duckdb
import pyarrow
from typing import List, Optional, Dict, Any, Callable
from statsmodels.stats.proportion import proportions_ztest
from scipy import stats
from math import sqrt
from dataclasses import dataclass


@dataclass
class ABConfig:
    """Self-documenting A/B experiment configuration."""
    conversion_event: str = 'purchase'
    user_id_col: str = 'user_id'
    variant_assignment: Callable = None

    def __post_init__(self):
        if self.variant_assignment is None:
            self.variant_assignment = lambda uid: np.where(uid % 2 == 0, 'A', 'B')


class ChunkProcessor:
    """
    Large dataset ingestion pipeline: CSV → Parquet chunks → DuckDB.

    Handles memory bottleneck of large CSVs (>1GB) by:
    1. Chunking CSV → columnar Parquet files
    2. Incrementally loading chunks into DuckDB
    3. Running A/B aggregation on the full unified table

    Note: chunking is an ingestion concern only.
    Once data is in DuckDB, all analytical logic runs on the full table.
    """

    def __init__(self,
                 chunk_size: int = 1000000,
                 stats_columns: List[str] | None = None,
                 ab_config: ABConfig | None = None):
        self.chunk_size = chunk_size
        self.stats_columns = stats_columns or ['event_type', 'user_id']
        self.ab_config = ab_config or ABConfig()

    def chunk_csv_to_parquet(self,
                             input_csv_path: str,
                             output_dir: str = 'data/chunks') -> List[str]:
        """
        Convert large CSV → numbered Parquet chunks. Never loads full CSV into memory.

        Returns: list of chunk file paths.
        """
        os.makedirs(output_dir, exist_ok=True)
        chunks = []
        chunk_num = 0

        for chunk_df in pd.read_csv(input_csv_path, chunksize=self.chunk_size):
            chunk_path = f"{output_dir}/chunk_{chunk_num:03d}.parquet"
            chunk_df.to_parquet(chunk_path, index=False)
            chunks.append(chunk_path)
            print(f"Wrote {chunk_path} ({len(chunk_df):,} rows)")
            chunk_num += 1

        print(f"Done: {len(chunks)} chunks created")
        return chunks

def load_chunks_to_duckdb(self,
                           chunks: List[str],
                           db_path: str,
                           schema: str = 'raw',
                           table_name: str = 'events') -> None:
    """
    Incrementally load Parquet chunks into DuckDB table.
    Appends each chunk without loading full dataset into RAM.

    Creates table on first run, appends on subsequent runs.
    Deduplicates on (user_id, user_session, event_type, event_time)
    per chunk before inserting into main table.
    """
    con = duckdb.connect(db_path)

    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        for i, chunk_path in enumerate(chunks):
            if i == 0:
                con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table_name} AS
                    SELECT * FROM read_parquet('{chunk_path}')
                    WHERE 1=0
                """)

            rows_before = con.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]

            con.execute(f"""
                INSERT INTO {schema}.{table_name}
                SELECT * FROM read_parquet('{chunk_path}') AS chunk
                WHERE NOT EXISTS (
                    SELECT 1 FROM {schema}.{table_name} AS t
                    WHERE t.user_id     = chunk.user_id
                      AND t.user_session = chunk.user_session
                      AND t.event_type   = chunk.event_type
                      AND t.event_time   = chunk.event_time
                )
            """)

            rows_after = con.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
            inserted = rows_after - rows_before
            print(f"Chunk {chunk_path}: {inserted:,} new rows inserted → {rows_after:,} total")

    finally:
        con.close()

def run_ab_aggregation(self,
                       db_path: str,
                       alpha: float = 0.05,
                       date_from: Optional[str] = None,
                       date_to: Optional[str] = None,
                       schema: str = 'intermediate',
                       view_name: str = 'int_funnel_flagged') -> Dict[str, Any]:
    """
    Run A/B z-test at user and session level from intermediate funnel view.
    Aggregates at runtime, optionally filtered by event_date window.

    Args:
        db_path:    Path to DuckDB file.
        alpha:      Significance threshold. Defaults to 0.05.
        date_from:  Optional ISO date string e.g. '2019-10-01'. Inclusive.
        date_to:    Optional ISO date string e.g. '2019-10-31'. Inclusive.
        schema:     Schema where intermediate view lives.
        view_name:  Intermediate funnel view name.

    Returns:
        Nested dict with z-test results for 'users' and 'sessions'.
    """
    time_filter = ""
    if date_from and date_to:
        time_filter = f"WHERE event_date BETWEEN '{date_from}' AND '{date_to}'"
    elif date_from or date_to:
        raise ValueError(
            "Both date_from and date_to must be provided together, or neither."
        )

    level_configs = [
        {
            'name':        'users',
            'id_col':      'user_id',
            'variant_col': 'variant_user',
        },
        {
            'name':        'sessions',
            'id_col':      'user_session',
            'variant_col': 'variant_session',
        },
    ]

    try:
        con = duckdb.connect(db_path, read_only=True)
    except duckdb.IOException as e:
        raise RuntimeError(
            f"Could not open {db_path} in read-only mode. "
            f"A write connection may still be open elsewhere."
        ) from e

    results = {}

    try:
        for cfg in level_configs:
            df = con.execute(f"""
                SELECT
                    {cfg['variant_col']}                        AS variant,
                    COUNT(DISTINCT {cfg['id_col']})             AS population,
                    SUM(purchased)                              AS conversions,
                    SUM(purchased)::FLOAT
                        / COUNT(DISTINCT {cfg['id_col']})       AS conversion_rate
                FROM {schema}.{view_name}
                {time_filter}
                GROUP BY {cfg['variant_col']}
            """).df().set_index('variant')

            present = sorted(df.index.tolist())
            if present != ['A', 'B']:
                raise ValueError(
                    f"Expected variants ['A', 'B'] at {cfg['name']} level, "
                    f"got {present}. Check intermediate view or date filter."
                )

            cr_A   = df.loc['A', 'conversion_rate']
            cr_B   = df.loc['B', 'conversion_rate']
            n_A    = df.loc['A', 'population']
            n_B    = df.loc['B', 'population']
            uplift = (cr_B / cr_A) - 1

            z_stat, p_value = proportions_ztest(
                [df.loc['A', 'conversions'], df.loc['B', 'conversions']],
                [n_A, n_B]
            )

            # Confidence interval on absolute uplift
            z_alpha = stats.norm.ppf(1 - alpha / 2)
            se      = sqrt(cr_A * (1 - cr_A) / n_A + cr_B * (1 - cr_B) / n_B)
            ci_low  = (cr_B - cr_A) - z_alpha * se
            ci_high = (cr_B - cr_A) + z_alpha * se

            # MDE at 80% power
            z_beta = stats.norm.ppf(0.80)
            mde    = (z_alpha + z_beta) * sqrt(2 * cr_A * (1 - cr_A) / n_A)

            # SRM check via chi-square on population split
            expected     = (n_A + n_B) / 2
            srm_stat     = ((n_A - expected) ** 2 / expected
                           + (n_B - expected) ** 2 / expected)
            srm_p        = 1 - stats.chi2.cdf(srm_stat, df=1)
            srm_detected = bool(srm_p < 0.01)

            results[cfg['name']] = {
                'population_A':    int(n_A),
                'population_B':    int(n_B),
                'conversions_A':   int(df.loc['A', 'conversions']),
                'conversions_B':   int(df.loc['B', 'conversions']),
                'cr_A':            float(cr_A),
                'cr_B':            float(cr_B),
                'absolute_uplift': float(cr_B - cr_A),
                'relative_uplift': float(uplift),
                'ci_low':          float(ci_low),
                'ci_high':         float(ci_high),
                'mde':             float(mde),
                'underpowered':    bool(abs(cr_B - cr_A) < mde),
                'z_stat':          float(z_stat),
                'p_value':         float(p_value),
                'significant':     bool(p_value < alpha),
                'alpha':           alpha,
                'srm_detected':    srm_detected,
                'srm_p_value':     float(srm_p),
                'date_from':       date_from,
                'date_to':         date_to,
            }

    finally:
        con.close()

    return results


"""
Call with no dates → should return same results as before

Call with a valid narrow window → population counts should drop, variants guard should still pass

Call with only date_from and no date_to → should raise ValueError immediately

Check ci_low < absolute_uplift < ci_high – if this fails the SE formula is broken

Check srm_detected is False on a clean 50/50 split – if True on balanced data, the chi-square is miscalculated
"""