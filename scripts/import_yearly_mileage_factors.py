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

# --- Load Yearly Mileage Factors from CSV
def load_yearly_milaege_factors(path):
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")

        df = pd.read_csv(path)
        logger.info(f"Loading yearly mileage factors from CSV file: {path}")

        if df.empty:
            raise ValueError(f"File is empty: {path}")

        df.columns = df.columns.str.strip()
        required_columns = {"YEARLY_MILEAGE_FROM", "YEARLY_MILEAGE_TO", "FACTOR"}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"Missing required columns: {required_columns - set(df.columns)}")

        # Convert columns robustly to int / float
        df["YEARLY_MILEAGE_FROM"] = pd.to_numeric(df["YEARLY_MILEAGE_FROM"], errors="coerce").astype(int)
        df["YEARLY_MILEAGE_TO"] = pd.to_numeric(
            df["YEARLY_MILEAGE_TO"].astype(str).str.strip().str.strip('"').replace('-1', 100_000_000),
            errors="coerce"
        ).astype(int)
        df["FACTOR"] = df["FACTOR"].astype(float)

        logger.info(f"{len(df)} factors for yearly mileages loaded.")
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
        logger.error(f"Error loading yearly mileage factors: {e}")
        raise


# --- Insert data into DB
def insert_yearly_mileage_factors(df, cur):
    try:
        yearly_mileages = df[["YEARLY_MILEAGE_FROM", "YEARLY_MILEAGE_TO"]].drop_duplicates()
        logger.info(f"Yearly mileages in file: {yearly_mileages.values}")

        for _, row in yearly_mileages.iterrows():
            try:
                # Cast to native Python types before DB insertion
                mileage_from = int(row["YEARLY_MILEAGE_FROM"])
                mileage_to = int(row["YEARLY_MILEAGE_TO"])
                factor = float(
                    df[(df["YEARLY_MILEAGE_FROM"] == mileage_from) &
                       (df["YEARLY_MILEAGE_TO"] == mileage_to)]["FACTOR"].values[0]
                )

                logger.info(f"Yearly mileage range: {mileage_from} - {mileage_to} → Factor: {factor}")

                # Check if the entry already exists in DB
                cur.execute(
                    "SELECT id FROM yearly_mileage WHERE yearly_mileage_from = %s AND yearly_mileage_to = %s;",
                    (mileage_from, mileage_to)
                )
                result = cur.fetchone()

                if result:
                    yearly_mileage_id = result[0]
                    logger.info(f"'{mileage_from} - {mileage_to}' already exists (id={yearly_mileage_id}).")
                else:
                    cur.execute(
                        "INSERT INTO yearly_mileage (yearly_mileage_from, yearly_mileage_to, yearly_mileage_factor) "
                        "VALUES (%s, %s, %s) RETURNING id;",
                        (mileage_from, mileage_to, factor)
                    )
                    yearly_mileage_id = cur.fetchone()[0]
                    logger.info(f"'{mileage_from} - {mileage_to}' added (id={yearly_mileage_id}).")

            except Exception as e:
                logger.error(f"Error processing yearly mileage range {mileage_from} - {mileage_to}: {e}")

        logger.info("Import of yearly mileages done!")

    except Exception as e:
        logger.error(f"Error inserting yearly mileage factors: {e}")
        raise
