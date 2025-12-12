
import pytest
import pandas as pd
import numpy as np
import sys
import os

# Ensure src module can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.extract.data_quality_check import (
    check_column_standardization,
    check_duplicates,
    check_missing_values,
    check_data_types
)

def test_check_column_standardization():
    # Test case 1: Needs standardization
    df = pd.DataFrame({'ProductID': [1], 'Product Name': ['A']})
    result = check_column_standardization(df)
    assert result['needed'] == True
    assert len(result['issues']) == 3
    
    # Test case 2: Already standard
    df_clean = pd.DataFrame({'product_id': [1], 'product_name': ['A']})
    result_clean = check_column_standardization(df_clean)
    assert result_clean['needed'] == False
    assert len(result_clean['issues']) == 0

def test_check_duplicates():
    # Test case 1: Duplicates present
    df = pd.DataFrame({'id': [1, 1, 2], 'val': ['a', 'a', 'b']})
    result = check_duplicates(df)
    assert result['needed'] == True
    assert result['count'] == 1
    
    # Test case 2: No duplicates
    df_clean = pd.DataFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
    result_clean = check_duplicates(df_clean)
    assert result_clean['needed'] == False
    assert result_clean['count'] == 0

def test_check_missing_values():
    # Test case 1: Missing values
    df = pd.DataFrame({'id': [1, 2, 3], 'val': [None, 'b', 'c']})
    result = check_missing_values(df, 'test_table')
    assert result['needed'] == True
    assert 'val' in result['columns']
    assert result['columns']['val']['count'] == 1
    
    # Test case 2: No missing values
    df_clean = pd.DataFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})
    result_clean = check_missing_values(df_clean, 'test_table')
    assert result_clean['needed'] == False
    assert len(result_clean['columns']) == 0

def test_check_data_types():
    # Test case 1: Date as string
    df = pd.DataFrame({'order_date': ['2021-01-01', '2021-01-02']})
    # Pandas reads strings as object
    result = check_data_types(df, 'test_table')
    assert result['needed'] == True
    assert 'order_date' in result['issues']
    assert result['issues']['order_date']['expected'] == 'datetime'
