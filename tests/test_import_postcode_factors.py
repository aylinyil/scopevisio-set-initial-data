import pytest
import tempfile
import os
import pandas as pd

from scripts.import_postcodes import load_csv, load_region_factors

################################################################################
# --- Unit tests for method load_region_factors from import_region_factors.py
################################################################################
@pytest.fixture
def sample_region_factors_csv():
    """Create a temporary CSV file with sample region factor data."""

    data = """REGION1, REGION_FACTOR
        "Baden-Württemberg", 1.1
        "Bayern", 1.6
        "Berlin", 1.22
        """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(data)
        path = f.name
    yield path
    os.unlink(path)

def test_load_region_factors(sample_region_factors_csv):
    """
    Test loading region factors from a valid sample CSV file.
    """
    result = load_region_factors(sample_region_factors_csv)

    # Make sure a dictionary is returned
    assert isinstance(result, dict)

    # Check that expected keys are present
    assert "Baden-Württemberg" in result
    assert "Bayern" in result
    assert "Berlin" in result

    # Check that values are floats
    for value in result.values():
        assert isinstance(value, float)

    # Check specific values
    assert result["Baden-Württemberg"] == 1.1
    assert result["Bayern"] == 1.6
    assert result["Berlin"] == 1.22

def test_load_region_factors_file_not_found():  
    """Raises FileNotFoundError when file does not exist."""

    with pytest.raises(FileNotFoundError):
        load_region_factors("non_existent_file.csv")

def test_load_region_factors_empty_file():
    """Raises ValueError when CSV is empty."""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        path = f.name

    try:
        with pytest.raises(ValueError):
            load_region_factors(path)
    finally:
        os.unlink(path)

def test_load_region_factors_missing_columns():
    """Raises ValueError if required columns are missing."""

    bad_data = """REGION1
        "Baden-Württemberg"
        """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(bad_data)
        path = f.name

    try:
        with pytest.raises(ValueError, match="Missing required columns"):
            load_region_factors(path)
    finally:
        os.unlink(path)

def test_load_region_factors_invalid_data():
    """Raises ValueError if data types are incorrect."""

    bad_data = """REGION1, REGION_FACTOR
        "Baden-Württemberg", not_a_number
        """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(bad_data)
        path = f.name

    try:
        with pytest.raises(ValueError):
            load_region_factors(path)
    finally:
        os.unlink(path)

################################################################################
# --- Unit tests for method load_csv from import_postcodes.py
################################################################################

@pytest.fixture
def sample_postcodes_csv():
    """Create a valid temporary CSV file with postcodes data."""
    # 7th column (index 6) = postcode, 3rd column (index 2) = region
    data = """
    "DE","DE-BW","Baden-Württemberg","Freiburg","Breisgau-Hochschwarzwald","Müllheim-Badenweiler","79289","Müllheim"
    """

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(data)
        path = f.name
    yield path
    os.unlink(path)

def test_load_csv(sample_postcodes_csv):
    """Test loading postcodes from a valid sample CSV file."""
    result = load_csv(sample_postcodes_csv)

    # Make sure a DataFrame is returned
    assert isinstance(result, pd.DataFrame)

    # Check that expected columns are present
    assert list(result.columns) == ["region", "postcode"]

    # Check number of rows and columns
    assert result.shape == (1, 2)

    assert "Baden-Württemberg" in result["region"].values
    assert "79289" in result["postcode"].values

def test_load_csv_file_not_found():
    """Raises FileNotFoundError when file does not exist."""

    with pytest.raises(FileNotFoundError):
        load_csv("non_existent_file.csv")

def test_load_csv_empty_file():
    """Raises ValueError when CSV is empty."""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        path = f.name

    try:
        with pytest.raises(pd.errors.EmptyDataError):
            load_csv(path)
    finally:
        os.unlink(path)

def test_load_csv_missing_columns():
    """Raises ValueError if required columns are missing."""

    bad_data = '"' 
    'DE","DE-BW"\n"DE","DE-BY"\n'  # Only 2 columns instead of at least 7
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(bad_data)
        path = f.name

    try:
        with pytest.raises(ValueError):
            load_csv(path)
    finally:
        os.unlink(path)

def test_load_csv_invalid_data():
    """Raises ValueError if postcode data is invalid."""

    bad_data = (
        '"DE","DE-BW","Baden-Württemberg","Freiburg","Beisgau-Hochschwarzwald","Hexental","not_a_postcode"\n'
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(bad_data)
        path = f.name

    try:
        with pytest.raises(ValueError):
            load_csv(path)
    finally:
        os.unlink(path)

################################################################################
# --- Unit tests for method insert_postcode_factors from import_postcodes.py
################################################################################

@pytest.fixture
def sample_df():
    """Sample DataFrame for testing insert_postcode_factors."""
    return pd.DataFrame({
        "region": ["Baden-Württemberg", "Bayern"],
        "postcode": ["79289", "80331"]
    })