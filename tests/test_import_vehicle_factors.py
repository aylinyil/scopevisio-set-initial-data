import pytest
import tempfile
import os
import pandas as pd

from scripts.import_vehicle_factors import load_vehicle_factors, insert_vehicle_factors

################################################################################
# --- Unit tests for method load_vehicle_factors from import_vehicle_factors.py
################################################################################
@pytest.fixture
def sample_vehicle_factors_csv():
    """Create a temporary CSV file with sample vehicle factor data."""

    data = """VEHICLE_TYPE, VEHICLE_FACTOR
        "SUV", 1.2
        "LKW", 1.4"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(data)
        path = f.name
    yield path
    os.unlink(path)

def test_load_vehicle_factors(sample_vehicle_factors_csv):
    """
    Test loading vehicle factors from a valid sample CSV file.
    """
    result = load_vehicle_factors(sample_vehicle_factors_csv)

    # Make sure Dataframe is returned
    assert isinstance(result, pd.DataFrame)

    # Check that expected columns are present
    assert "VEHICLE_TYPE" in result.columns
    assert "VEHICLE_FACTOR" in result.columns


    for _, row in result.iterrows():
        assert isinstance(row["VEHICLE_TYPE"], str)
        assert isinstance(row["VEHICLE_FACTOR"], float)

        assert row["VEHICLE_TYPE"] in ["SUV", "LKW"]
        assert row["VEHICLE_FACTOR"] in [1.2, 1.4]

    
def test_load_vehicle_factors_file_not_found():
    """Raises FileNotFoundError when file does not exist."""

    with pytest.raises(FileNotFoundError):
        load_vehicle_factors("non_existent_file.csv")


def test_load_vehicle_factors_empty_file():
    """Raises ValueError when CSV is empty."""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        path = f.name

    try:
        with pytest.raises(ValueError, match="empty"):
            load_vehicle_factors(path)
    finally:
        os.unlink(path)


def test_load_vehicle_factors_missing_columns():
    """Raises ValueError if required columns are missing."""

    bad_data = """TYPE, FACTOR
        "SUV", 1.2"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(bad_data)
        path = f.name

    try:
        with pytest.raises(ValueError, match="Missing required columns"):
            load_vehicle_factors(path)
    finally:
        os.unlink(path)

def test_load_vehicle_factors_invalid_data():
    """Raises ValueError if data types are incorrect."""

    bad_data = """VEHICLE_TYPE, VEHICLE_FACTOR
        "SUV", not_a_number"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(bad_data)
        path = f.name

    try:
        with pytest.raises(ValueError):
            load_vehicle_factors(path)
    finally:
        os.unlink(path)


################################################################################
# --- Unit tests for method insert_vehicle_factors from import_vehicle_factors.py
################################################################################

@pytest.fixture
def sample_df():
    """Sample DataFrame for testing insert_vehicle_factors."""
    return pd.DataFrame({
        "VEHICLE_TYPE": ["SUV", "LKW"],
        "VEHICLE_FACTOR": [1.2, 1.4]
    })

def test_insert_vehicle_factors_new_entries(sample_df, mocker):
    cur = mocker.Mock()

    # Correct sequence: SELECT, INSERT, SELECT, INSERT
    cur.fetchone.side_effect = [
        None,  # SUV SELECT
        [1],   # SUV INSERT
        None,  # LKW SELECT
        [2],   # LKW INSERT
    ]

    insert_vehicle_factors(sample_df, cur)

    insert_calls = [call for call in cur.execute.call_args_list if call[0][0].startswith("INSERT")]
    assert len(insert_calls) == 2


def test_insert_vehicle_factors_existing_entries(sample_df, mocker):
    """Test behavior when vehicle types already exist in DB."""
    cur = mocker.Mock()
    # SELECT returns existing IDs
    cur.fetchone.side_effect = [[10], [20]]  # SUV → 10, LKW → 20

    insert_vehicle_factors(sample_df, cur)

    # No INSERT should occur
    insert_calls = [call for call in cur.execute.call_args_list if call[0][0].startswith("INSERT")]
    assert len(insert_calls) == 0


def test_insert_vehicle_factors_mixed_entries(sample_df, mocker):
    """Test mixed scenario: some exist, some new."""
    cur = mocker.Mock()
    # SUV exists, LKW does not
    cur.fetchone.side_effect = [[10], None, [2]]  # SELECT SUV → 10, SELECT LKW → None, INSERT LKW → id 2

    insert_vehicle_factors(sample_df, cur)

    # INSERT should happen only once
    insert_calls = [call for call in cur.execute.call_args_list if call[0][0].startswith("INSERT")]
    assert len(insert_calls) == 1