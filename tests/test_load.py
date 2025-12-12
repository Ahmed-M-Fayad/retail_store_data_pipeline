
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import sys
import os

# Ensure src module can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.load.sql_loader import load_data_to_sql

def test_load_data_to_sql():
    # Mock dataframe
    df = pd.DataFrame({'id': [1], 'name': ['test']})
    dfs = {'brands': df}
    
    # Mock SQLAlchemy engine
    mock_engine = MagicMock()
    
    # Patch pandas to_sql to verify it's called
    with patch('pandas.DataFrame.to_sql') as mock_to_sql:
        result = load_data_to_sql(dfs, mock_engine)
        
        assert result == True
        # Verify to_sql was called once for brands
        mock_to_sql.assert_called_once()
        args, kwargs = mock_to_sql.call_args
        
        # Check arguments: table name, engine
        assert args[0] == 'Brands'
        assert args[1] == mock_engine
        assert kwargs['if_exists'] == 'append'
