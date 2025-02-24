import os
import sqlite3
import pandas as pd
import logging

# Ensure the data directory exists
DB_PATH = "data/housing_results.db"
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Set up logging
logging.basicConfig(
    filename="property_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class Database:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

    def _connect(self):
        """Create a connection to the database."""
        return sqlite3.connect(self.db_path)

    def save_data(self, df, table_name, if_exists="replace"):
        """Save a DataFrame to the database."""
        try:
            logging.info(f"Saving {len(df)} records to '{table_name}' table.")
            with self._connect() as conn:
                df.astype(str).to_sql(table_name, conn, if_exists=if_exists, index=False)
            logging.info(f"Successfully saved {len(df)} records to '{table_name}'.")
        except Exception as e:
            logging.error(f"Error saving to '{table_name}': {e}")

    def load_data(self, table_name):
        """Load data from a specified table."""
        try:
            logging.info(f"Loading data from '{table_name}' table.")
            with self._connect() as conn:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            logging.info(f"Successfully loaded {len(df)} records from '{table_name}'.")
            return df
        except Exception as e:
            logging.error(f"Error loading from '{table_name}': {e}")
            return pd.DataFrame()

# Instantiate a global database object
db = Database()
