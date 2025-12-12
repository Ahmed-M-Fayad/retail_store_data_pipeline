
import pytest
import pandas as pd
import sys
import os

# Ensure src module can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.transform.transform_pipeline import (
    standardize_columns,
    clean_brands,
    clean_products
)

def test_standardize_columns():
    df = pd.DataFrame({'Brand ID': [1], 'Brand Name': ['Nike']})
    dfs = {'brands': df}
    # Mock config
    config = {'pipeline_steps': {'column_standardization': True}}
    
    result_dfs = standardize_columns(dfs, config)
    result_cols = result_dfs['brands'].columns.tolist()
    
    assert 'brand_id' in result_cols
    assert 'brand_name' in result_cols
    assert 'Brand ID' not in result_cols

def test_clean_brands():
    df = pd.DataFrame({
        'brand_id': [1, 1, 2],
        'brand_name': ['Nike', 'Nike', None]
    })
    
    cleaned_df, msg = clean_brands(df, True)
    
    assert len(cleaned_df) == 2  # Duplicates removed
    assert cleaned_df.loc[cleaned_df['brand_id'] == 2, 'brand_name'].iloc[0] == 'Unknown' # Fillna works

def test_clean_products():
    df = pd.DataFrame({
        'product_id': [1, 2, 3],
        'product_name': ['A', 'B', 'C'],
        'brand_id': [1, 1, 1],
        'category_id': [1, 1, 1],
        'model_year': [2021, 2021, 2021],
        'list_price': [100.0, -10.0, 50.0] # Negative price
    })
    
    cleaned_df, msg = clean_products(df, True)
    
    # Negative price should be removed
    assert len(cleaned_df) == 2
    assert 2 not in cleaned_df['product_id'].values
