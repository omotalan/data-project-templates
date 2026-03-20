import pandas as pd
import numpy as np
import os
import duckdb
from typing import List, Dict, Any, Callable
from statsmodels.stats.proportion import proportions_ztest
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
        """
        con = duckdb.connect(db_path)

        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            for i, chunk_path in enumerate(chunks):
                if i == 0:
                    # First chunk: create table with correct schema
                    con.execute(f"""
                        CREATE TABLE IF NOT EXISTS {schema}.{table_name} AS
                        SELECT * FROM read_parquet('{chunk_path}')
                        WHERE 1=0
                    """)

                con.execute(f"""
                    INSERT INTO {schema}.{table_name}
                    SELECT * FROM read_parquet('{chunk_path}')
                """)
                print(f"Loaded {chunk_path} → {schema}.{table_name}")

            total = con.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
            print(f"Done: {total:,} total rows in {schema}.{table_name}")

        finally:
            con.close()

    def run_ab_aggregation(self,
                           db_path: str,
                           schema: str = 'marts',
                           funnel_table: str = 'fct_funnel') -> Dict[str, Any]:
        """
        Run A/B z-test on full unified DuckDB table.
        Reads from dbt mart output directly.

        Returns: results dict with conversion rates, uplift, z-stat, p-value.
        """
        con = duckdb.connect(db_path)

        try:
            df = con.execute(f"""
                SELECT
                    variant,
                    COUNT(DISTINCT {self.ab_config.user_id_col}) AS users,
                    SUM(contacted) AS conversions
                FROM {schema}.{funnel_table}
                GROUP BY variant
            """).df()

        finally:
            con.close()

        df = df.set_index('variant')

        users_A = df.loc['A', 'users']
        users_B = df.loc['B', 'users']
        conv_A = df.loc['A', 'conversions']
        conv_B = df.loc['B', 'conversions']

        z_stat, p_value = proportions_ztest([conv_A, conv_B], [users_A, users_B])

        cr_A = conv_A / users_A
        cr_B = conv_B / users_B

        return {
            'total_users_A': int(users_A),
            'total_users_B': int(users_B),
            'total_conv_A': int(conv_A),
            'total_conv_B': int(conv_B),
            'cr_A': cr_A,
            'cr_B': cr_B,
            'absolute_uplift': cr_B - cr_A,
            'relative_uplift_pct': (cr_B - cr_A) / cr_A * 100,
            'z_stat': float(z_stat),
            'p_value': float(p_value),
            'alpha_005_significant': p_value < 0.05
        }
