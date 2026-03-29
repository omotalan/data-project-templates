# === COMPLETE END-TO-END EXAMPLE ===
from dotenv import load_dotenv
import sys
import os
from utils.chunk_processor import ChunkProcessor, ABConfig
import numpy as np
import subprocess
import shutil


load_dotenv()
DB_PATH = os.environ.get("DB_PATH")
if DB_PATH is None:
    raise RuntimeError(
        "DB_PATH not set. Copy .env.example to .env and fill in your local path."
    )

processor = ChunkProcessor(
    ab_config=ABConfig(
        conversion_col='purchased',
        user_id_col='user_id',
        session_id_col='user_session',
        variant_assignment=lambda uid: np.where(uid % 2 == 0, 'A', 'B')
    )
)

# Ingestion

filenames = ['2019-Oct', '2019-Nov']
for file in filenames:
    chunks = processor.chunk_csv_to_parquet(input_csv_path=DB_PATH + f'{file}.csv', output_dir=DB_PATH + f'chunks_{file}')
    processor.load_chunks_to_duckdb(chunks, db_path=DB_PATH + 'marketplace-analytics.duckdb')

# Run dbt (shell or subprocess)
## Shell: 'dbt run' from dbt_project directory

## Subprocess (set up to run from a virtual environment):
dbt_executable = shutil.which("dbt")   # resolves to the active env's dbt
if dbt_executable is None:
    raise RuntimeError("dbt not found on PATH. Make sure your virtual env is active.")

dbt_result = subprocess.run(
    [dbt_executable, "run"],
    cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dbt_project')),
    capture_output=True,
    text=True,
    env=os.environ.copy()   # passes current venv's PATH into the subprocess
)

print(dbt_result.stdout)


# Analysis
results = processor.run_ab_aggregation(db_path=DB_PATH + 'marketplace-analytics.duckdb')


# Results dict output for charts, dashboards, etc.
print(results)
