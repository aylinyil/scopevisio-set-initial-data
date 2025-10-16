import pandas as pd
import logging
import sys
import os

logger = logging.getLogger("Logger")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

def load_vehicle_factors(path):
    """
    Loads vehicle factors from a CSV file.

    Args:
        path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing VEHICLE_TYPE and VEHICLE_FACTOR.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty or missing required columns.
        Exception: For any other unexpected error.
    """
    try:
        # --- Check if file exists ---
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")

        # --- Attempt to read CSV ---
        df = pd.read_csv(path)

        # --- Check if DataFrame is empty ---
        if df.empty:
            raise ValueError(f"File is empty: {path}")

        # --- Normalize columns and values ---
        df.columns = df.columns.str.strip()

        required_columns = {"VEHICLE_TYPE", "VEHICLE_FACTOR"}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"Missing required columns: {required_columns - set(df.columns)}")

        df["VEHICLE_TYPE"] = df["VEHICLE_TYPE"].astype(str).str.strip().str.strip('"')
        df["VEHICLE_FACTOR"] = df["VEHICLE_FACTOR"].astype(float)

        logger.info(f"{len(df)} vehicle types loaded from '{path}'.")
        return df

    except FileNotFoundError as e:
        logger.error(e)
        raise
    except pd.errors.EmptyDataError:
        logger.error(f"CSV file is empty: {path}")
        raise ValueError(f"CSV file is empty: {path}")
    except ValueError as e:
        logger.error(e)
        raise
    except Exception as e:
        logger.error(f"Error loading vehicle factors: {e}")
        raise

# --- Insert data into DB
def insert_vehicle_factors(df, cur):
    try:
        vehicle_types = df["VEHICLE_TYPE"].unique()
        logger.info(f"Vehicle types in file: {vehicle_types}")

        for vehicle_type in vehicle_types:
            factor = float(df[df["VEHICLE_TYPE"] == vehicle_type]["VEHICLE_FACTOR"].values[0])
            logger.info(f"Vehicle type: {vehicle_type} → Factor: {factor}")

            # Check if the entry already exists in DB
            cur.execute("SELECT id FROM vehicle WHERE vehicle_type = %s;", (vehicle_type,))
            result = cur.fetchone()

            if result:
                vehicle_type_id = result[0]
                logger.info(f"'{vehicle_type}' already exists (id={vehicle_type_id}).")
            else:
                cur.execute(
                    "INSERT INTO vehicle (vehicle_type, vehicle_factor) VALUES (%s, %s) RETURNING id;",
                    (vehicle_type, factor)
                )
                vehicle_type_id = cur.fetchone()[0]
                
                logger.info(f"'{vehicle_type}' added (id={vehicle_type_id}).")

        logger.info("\nImport of vehicle types done!")
    except Exception as e:
        logger.error(f"Error inserting vehicle factors: {e}")
        raise