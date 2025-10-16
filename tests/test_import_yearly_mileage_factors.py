import pytest
import tempfile
import os
import pandas as pd

from scripts.import_yearly_mileage_factors import load_yearly_milaege_factors, insert_yearly_mileage_factors

######################################################################################
# --- Unit tests for method load_yearly_mileage_factors from import_mileage_factors.py
######################################################################################
@pytest.fixture
def sample_yearly_mileage_factors_csv():
    """Create a temporary CSV file with sample yearly mileage factor data."""

    data = """YEARLY_MILEAGE_FROM, YEARLY_MILEAGE_TO, FACTOR
        10001, 20000, 1.5
        20001, -1, 2.0
        """
        
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(data)
        path = f.name
    yield path
    os.unlink(path)

def test_load_yearly_mileage_factors(sample_yearly_mileage_factors_csv):
    """
    Test loading yearly mileage factors from a valid sample CSV file.
    """
    result = load_yearly_milaege_factors(sample_yearly_mileage_factors_csv)

    # Make sure Dataframe is returned
    assert isinstance(result, pd.DataFrame)

    # Check that expected columns are present
    assert "YEARLY_MILEAGE_FROM" in result.columns
    assert "YEARLY_MILEAGE_TO" in result.columns
    assert "FACTOR" in result.columns

    for _, row in result.iterrows():
        assert isinstance(row["YEARLY_MILEAGE_FROM"], float)
        assert isinstance(row["YEARLY_MILEAGE_TO"], (int, float))
        assert isinstance(row["FACTOR"], float)

        assert row["YEARLY_MILEAGE_FROM"] in [10001, 20001]
        assert row["YEARLY_MILEAGE_TO"] in [20000, 100000000]
        assert row["FACTOR"] in [1.5, 2.0]

def test_load_yearly_mileage_factors_file_not_found():
    """Raises FileNotFoundError when file does not exist."""

    with pytest.raises(FileNotFoundError):
        load_yearly_milaege_factors("non_existent_file.csv")

def test_load_yearly_mileage_empty_file():
    """Raises ValueError when CSV is empty."""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        path = f.name

    try:
        with pytest.raises(ValueError, match="empty"):
            load_yearly_milaege_factors(path)
    finally:
        os.unlink(path)

def test_load_yearly_mileage_missing_columns():
    """Raises ValueError if required columns are missing."""

    bad_data = """YEARLY_MILEAGE_FROM, YEARLY_MILEAGE_TO
        0, 500"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(bad_data)
        path = f.name

    try:
        with pytest.raises(ValueError, match="Missing required columns"):
            load_yearly_milaege_factors(path)
    finally:
        os.unlink(path)

def test_load_yearly_mileage_invalid_data():
    """Raises ValueError if data types are incorrect."""

    bad_data = """YEARLY_MILEAGE_FROM, YEARLY_MILEAGE_TO, FACTOR
        0, 5000, 0.5
        foobar, 10000, 1.0
        10001, 20000, 1.5"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(bad_data)
        path = f.name

    try:
        with pytest.raises(ValueError):
            load_yearly_milaege_factors(path)
    finally:
        os.unlink(path)


######################################################################################
# --- Unit tests for method insert_yearly_mileage_factors from import_mileage_factors.py
######################################################################################

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing insert_yearly_mileage_factors."""
    return pd.DataFrame( {
        "YEARLY_MILEAGE_FROM": [0, 5001, 10001, 20001],
        "YEARLY_MILEAGE_TO": [5000, 10000, 20000, 100000000],
        "FACTOR": [0.8, 1.0, 1.2, 1.5]
    })

def test_insert_yearly_mileage_factors_new_entries(sample_df, mocker):
    """Test inserting yearly mileages that do not exist in DB (all new)."""
    # Mock cursor
    cur = mocker.Mock()

    # Correct side_effect: SELECT → None, INSERT → ID
    cur.fetchone.side_effect = [
        None, [1],  # first row
        None, [2],  # second row
        None, [3],  # third row
        None, [4]   # fourth row
    ]

    insert_yearly_mileage_factors(sample_df, cur)

    expected_ranges = sample_df[["YEARLY_MILEAGE_FROM", "YEARLY_MILEAGE_TO"]].drop_duplicates()

    # SELECT calls
    select_calls = [call for call in cur.execute.call_args_list if call[0][0].startswith("SELECT")]
    assert len(select_calls) == len(expected_ranges)

    # INSERT calls
    insert_calls = [call for call in cur.execute.call_args_list if call[0][0].startswith("INSERT")]
    assert len(insert_calls) == len(expected_ranges)