import pandas as pd
import numpy as np
import os
from typing import List, Dict, Any, Callable
from statsmodels.stats.proportion import proportions_ztest
from dataclasses import dataclass

@dataclass
class ABConfig:
    """Self-documenting A/B experiment configuration."""
    conversion_event: str = 'purchase'          # Event marking success
    user_id_col: str = 'user_id'               # Grouping key
    variant_assignment: Callable = None         # lambda uid: 'A' or 'B'
    
    def __post_init__(self):
        """Auto-default variant assignment if not provided."""
        if self.variant_assignment is None:
            self.variant_assignment = lambda uid: np.where(uid % 2 == 0, 'A', 'B')

class ChunkProcessor:
    """
    Enterprise-grade CSV chunking → Parquet → pooled A/B testing pipeline.
    
    Handles large datasets (>1GB) by:
    1. Chunking CSV → columnar Parquet
    2. Per-chunk A/B aggregation  
    3. Validation + global z-test on pooled totals
    """
    
    def __init__(self, 
                 chunk_size: int = 1000000,
                 stats_columns: List[str] | None = None,
                 ab_config: ABConfig | None = None):
        """
        Configurable initialization.
        
        stats_columns: Columns to validate representativeness across chunks
        ab_config: A/B experiment parameters (ABConfig dataclass)
        """
        self.chunk_size = chunk_size
        self.stats_columns = stats_columns or ['event_type', 'user_id']
        self.ab_config = ab_config or ABConfig()
    
    def chunk_csv_to_parquet(self, 
                           input_csv_path: str, 
                           output_dir: str = 'data/chunks') -> List[str]:
        """
        Convert large CSV → numbered Parquet chunks.
        
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
            
        print(f"Created {len(chunks)} chunks")
        return chunks
    
    def aggregate_chunk_ab(self, chunk_path: str) -> pd.Series:
        """
        Read Parquet chunk → compute A/B metrics for this chunk only.
        """
        df = pd.read_parquet(chunk_path)
        
        # Apply configurable variant assignment
        uid_col = self.ab_config.user_id_col
        df['variant'] = self.ab_config.variant_assignment(df[uid_col])
        
        # Users per variant
        users = df.groupby('variant')[uid_col].nunique()
        
        # Conversions (unique users with conversion event)
        conv_mask = df[self.ab_config.conversion_event]  # flexible column access
        conversions = df[conv_mask].groupby('variant')[uid_col].nunique()
        
        return pd.Series({
            'chunk_path': chunk_path,
            'users_A': users.get('A', 0),
            'users_B': users.get('B', 0),
            'conv_A': conversions.get('A', 0),
            'conv_B': conversions.get('B', 0)
        })
    
    def validate_chunk_representativeness(self, 
                                        chunks: List[str], 
                                        tolerance: float = 0.1) -> List[tuple]:
        """
        Validate chunks have similar statistical fingerprints.
        """
        stats = []
        for chunk_path in chunks:
            df = pd.read_parquet(chunk_path)
            chunk_stats = {'n_rows': len(df)}
            
            # Configurable stats columns
            for col in self.stats_columns:
                if col in df.columns:
                    if df[col].dtype.kind in 'biufc':  # numeric
                        chunk_stats[f'avg_{col}'] = df[col].mean()
                    else:  # categorical
                        chunk_stats[f'{col}_dist'] = df[col].value_counts(normalize=True).to_dict()
            
            stats.append(chunk_stats)
        
        # Baseline = chunk 0
        baseline = stats[0]
        validation = []
        
        for i, s in enumerate(stats[1:], 1):
            issues = []
            
            # Row count
            if abs(s['n_rows'] / baseline['n_rows'] - 1) > tolerance:
                issues.append(f"Row count drift chunk {i}")
            
            # Configurable stats
            for key in set(baseline) & set(s):
                if key.startswith(('avg_', '')) and key != 'n_rows':
                    base_val = baseline[key]
                    obs_val = s[key]
                    if isinstance(base_val, dict):  # distribution
                        for cat in base_val:
                            if abs(obs_val.get(cat, 0) - base_val[cat]) > tolerance:
                                issues.append(f"{key} '{cat}' drift chunk {i}")
                    elif abs(obs_val / base_val - 1) > tolerance:
                        issues.append(f"{key} drift chunk {i}: {obs_val/base_val:.2f}x")
            
            validation.append((i, issues))
        
        # Summary
        failed_chunks = [v for v in validation if v[1]]
        if failed_chunks:
            print(f"⚠️ {len(failed_chunks)} chunks failed validation:")
            for chunk_i, issues in failed_chunks:
                print(f"  Chunk {chunk_i}: {', '.join(issues)}")
        else:
            print("✅ All chunks representative")
            
        return validation
    
    def pool_and_ztest(self, chunk_aggregates: pd.DataFrame) -> Dict[str, Any]:
        """
        Pool chunk totals → single global A/B z-test.
        """
        totals = chunk_aggregates[['users_A', 'users_B', 'conv_A', 'conv_B']].sum()
        
        success = [totals['conv_A'], totals['conv_B']]
        nobs = [totals['users_A'], totals['users_B']]
        
        z_stat, p_value = proportions_ztest(success, nobs)
        
        cr_A = totals['conv_A'] / totals['users_A']
        cr_B = totals['conv_B'] / totals['users_B']
        uplift_pct = (cr_B - cr_A) / cr_A * 100
        
        return {
            'total_users_A': int(totals['users_A']),
            'total_users_B': int(totals['users_B']),
            'total_conv_A': int(totals['conv_A']),
            'total_conv_B': int(totals['conv_B']),
            'cr_A': cr_A,
            'cr_B': cr_B,
            'absolute_uplift_pct': cr_B - cr_A,
            'relative_uplift_pct': uplift_pct,
            'z_stat': float(z_stat),
            'p_value': float(p_value),
            'alpha_005_significant': p_value < 0.05
        }