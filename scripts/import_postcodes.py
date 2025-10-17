import pandas as pd
import logging
import sys
import os

logger = logging.getLogger("Logger")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.propagate = False

# --- Load Region Factors from CSV
def load_region_factors(path):
    """
    Loads region factors from a CSV file.

    Args:
        path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing REGION1 and REGION_FACTOR.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty or missing required columns.
        Exception: For any other unexpected error.
    """
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
        
        logger.info(f"Loading Region Factors from CSV: {path}")
        df = pd.read_csv(path)

        if df.empty:
            raise ValueError(f"File is empty: {path}")
        
        # Column names trim (removes leading/trailing spaces)
        df.columns = df.columns.str.strip()

        required_columns = {"REGION1", "REGION_FACTOR"}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"Missing required columns: {required_columns - set(df.columns)}")
        
        # Remove quotes and spaces from values
        df["REGION1"] = df["REGION1"].astype(str).str.strip().str.strip('"')
        df["REGION_FACTOR"] = df["REGION_FACTOR"].astype(float)
        factors = dict(zip(df["REGION1"], df["REGION_FACTOR"]))
       
        logger.info(f"{len(factors)} regions loaded.")
        return factors
    except Exception as e:
        logger.error(f"Error loading region factors: {e}")
        raise

# --- Read CSV file "postcodes"
def load_csv(path):
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
        
        logger.info(f"Load postcode CSV file: {path}")
        df = pd.read_csv(path, header=None)

        if df.empty:
            raise ValueError(f"File is empty: {path}")
        
        if df.shape[1] < 7:
            raise ValueError("CSV file must contain at least 7 columns")

        # Column index 2 = region; 6 = postcode
        df = df[[2, 6]]
        df.columns = ["region", "postcode"]

        df = df.dropna(subset=["region", "postcode"])
        # Only keep 5-digit postcodes, remove quotes and spaces
        df["postcode"] = (
            df["postcode"]
            .astype(str)
            .str.strip()
            .str.extract(r"(\d{5})")[0]  # extract only 5-digit sequences
        )

        # Drop rows where postcode could not be parsed to a 5-digit code
        invalid_count = df["postcode"].isna().sum()
        if invalid_count:
            logger.warning("Dropping %s rows with invalid postcodes from CSV.", int(invalid_count))
        df = df[df["postcode"].notna()]
        
        df["region"] = df["region"].astype(str).str.strip().str.strip('"')
        df["postcode"] = df["postcode"].astype(str).str.zfill(5)
        logger.info(f"{len(df)} lines loaded.")

        return df
    except Exception as e:
        logger.error(f"Error loading postcode CSV: {e}")
        raise

# --- Insert data into DB
def insert_postcodes(df, region_factors, cur):
    try:
        regions = df["region"].unique()
        logger.info(f"Regions in CSV file: {regions}")

        for region in regions:
            factor = region_factors.get(region)
            if factor is None:
                # Default to neutral factor when mapping is missing
                logger.warning("Region '%s' missing in factors mapping. Defaulting factor to 1.0.", region)
                factor = 1.0
            logger.info(f"\nRegion: {region} → Factor: {factor}")

            # Check if the region already exists in DB
            cur.execute("SELECT id FROM regions WHERE region = %s;", (region,))
            result = cur.fetchone()

            if result:
                region_id = result[0]
                logger.info(f"'{region}' already exists (id={region_id}).")
            else:
                cur.execute(
                    "INSERT INTO regions (region, region_factor) VALUES (%s, %s) RETURNING id;",
                    (region, factor)
                )
                region_id = cur.fetchone()[0]
                logger.info(f"New region '{region}' (id={region_id}) added.")

            # Postcodes for this region
            region_list = df[df["region"] == region]["postcode"].unique().tolist()
            cur.execute("SELECT postcode FROM postcodes;")
            existing_postcodes = {row[0] for row in cur.fetchall()}
            new_postcode = [(region_id, postcode) for postcode in region_list if postcode not in existing_postcodes]

            if new_postcode:
                logger.info(f"Postcode: {new_postcode} ")
                for region_id, postcode in new_postcode:
                    cur.execute(
                        "INSERT INTO postcodes (region_id, postcode) VALUES (%s, %s);",
                        (region_id, postcode)
                    )
                    logger.info(f"{len(new_postcode)} new postcodes added.")
            else:
                logger.info("No new postcodes found.")

        logger.info("Import of postcodes done!")
    except Exception as e:
        logger.error(f"Error inserting postcodes: {e}")
        raise
