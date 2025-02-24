import os
import psycopg2
import pandas as pd
import logging
from sqlalchemy import create_engine
import urllib.parse
import streamlit as st

DB_NAME = st.secrets["database"]["DB_NAME"]
DB_USER = st.secrets["database"]["DB_USER"]
DB_PASSWORD = urllib.parse.quote(st.secrets["database"]["DB_PASSWORD"])
DB_HOST = st.secrets["database"]["DB_HOST"]
DB_PORT = st.secrets["database"]["DB_PORT"]

# Set up logging
logging.basicConfig(
    filename="property_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class Database:
    def __init__(self, db_name=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT):
        self.db_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        self.engine = create_engine(self.db_url)
    
    def save_data(self, df, table_name, if_exists="replace"):
        """Save a DataFrame to the database."""
        try:
            logging.info(f"Saving {len(df)} records to '{table_name}' table.")
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logging.info(f"Successfully saved {len(df)} records to '{table_name}'.")
        except Exception as e:
            logging.error(f"Error saving to '{table_name}': {e}")
    
    def load_data(self, table_name):
        """Load data from a specified table."""
        try:
            logging.info(f"Loading data from '{table_name}' table.")
            df = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)
            logging.info(f"Successfully loaded {len(df)} records from '{table_name}'.")
            return df
        except Exception as e:
            logging.error(f"Error loading from '{table_name}': {e}")
            return pd.DataFrame()

# Instantiate a global database object
db = Database()
