import pandas as pd
import logging
import sys

logger = logging.getLogger("Logger")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

# --- Load Vehicle Factors from CSV
def load_vehicle_factors(path):
    try:
        df = pd.read_csv(path)
        # Trim column names (removes leading/trailing spaces)
        df.columns = df.columns.str.strip()
        # Delete quotes and spaces from values
        df["VEHICLE_TYPE"] = df["VEHICLE_TYPE"].astype(str).str.strip().str.strip('"')
        df["VEHICLE_FACTOR"] = df["VEHICLE_FACTOR"].astype(float)

        logger.info(f"{len(df)} vehicle types loaded.")
        return df
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