from scripts.import_postcodes import load_region_factors, insert_postcodes, load_csv
from scripts.import_vehicle_factors import load_vehicle_factors, insert_vehicle_factors
from scripts.import_yearly_mileage_factors import load_yearly_milaege_factors, insert_yearly_mileage_factors

import os, psycopg2
import sys
import logging

logger = logging.getLogger("Logger")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)


# --- Connect to Database
def get_connection(host, password, database, user, port):
    try:
        conn = psycopg2.connect(user=user, password=password, host=host, database=database, port=port)
        logger.info("Connection to DB established successfully.")
        return conn
    except Exception as e:
        logger.exception("Failed to connect to DB")
        sys.exit(1)

if __name__ == "__main__":
    try:
        conn = get_connection(os.getenv("DB_HOST"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME"), os.getenv("DB_USER"), os.getenv("DB_PORT"))
        
        cur = conn.cursor()
        logger.info("Created cursor.")

        # --- Paths to CSV files
        POSTCODES_CSV_PATH = os.path.join(os.path.dirname(__file__), "csv-files", "postcodes.csv")
        REGION_FACTOR_CSV_PATH = os.path.join(os.path.dirname(__file__), "csv-files", "region_factor_mapping.csv")
        VEHICLE_FACTOR_CSV_PATH = os.path.join(os.path.dirname(__file__), "csv-files", "vehicle_factor_mapping.csv")
        YEARLY_MILEAGE_FACTOR_CSV_PATH = os.path.join(os.path.dirname(__file__), "csv-files", "yearly_mileage_factor_mapping.csv")

        try:
            # --- Import data for Yearly Mileage Factors
            yearly_mileage_factors = load_yearly_milaege_factors(YEARLY_MILEAGE_FACTOR_CSV_PATH)
            insert_yearly_mileage_factors(yearly_mileage_factors, cur)
            logger.info("Yearly mileage factors imported successfully.")
        except Exception as e:
            logger.exception("Failed to import yearly mileage factors.")

        try:
            # --- Import data for Vehicle Factors
            vehicle_factors = load_vehicle_factors(VEHICLE_FACTOR_CSV_PATH)
            insert_vehicle_factors(vehicle_factors, cur)
            logger.info("Vehicle factors imported successfully.")
        except Exception as e:
            logger.exception("Failed to import vehicle factors.")

        try:
            # --- Import data for Postcodes and Region-Factors
            region_factors = load_region_factors(REGION_FACTOR_CSV_PATH)
            postcodes = load_csv(POSTCODES_CSV_PATH)
            insert_postcodes(postcodes, region_factors, cur)
            logger.info("Postcodes and region factors imported successfully.")
        except Exception as e:
            logger.exception("Failed to import postcodes and region factors.")

        # -- Close cursor and connection
        conn.commit()
        logger.info("All data imported and committed successfully.")
        
    except Exception as e:
        logger.exception("An error occurred during the import process.")
    finally:
        try:
            cur.close()
            conn.close()
            logger.info("Cursor and connection closed.")
        except Exception:
            logger.exception("Failed to close cursor/connection")