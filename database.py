import pandas as pd
import logging
import streamlit as st
from supabase import create_client, Client

SUPABASE_URL = st.secrets["database"]["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["database"]["SUPABASE_KEY"]

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Set up logging
logging.basicConfig(
    filename="property_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class SupabaseDatabase:
    def __init__(self, client):
        self.client = client

    def save_data(self, df, table_name):
        """Delete existing records and save a DataFrame to a Supabase table."""
        try:
            logging.info(f"Clearing all records from '{table_name}' table.")
            delete_response = self.client.table(table_name).delete().neq("id", None).execute()  # Delete all rows
            logging.info(f"Deleted {len(delete_response.data) if delete_response.data else 0} records from '{table_name}'.")
    
            # Reset primary key sequence
            self.client.postgrest.rpc("run_sql", {"sql": "ALTER SEQUENCE property_listing_numbers_id_seq RESTART WITH 1;"})
            logging.info(f"Reset the primary key sequence for '{table_name}'.")
        
    
            logging.info(f"Saving {len(df)} new records to '{table_name}' table.")
            data = df.to_dict(orient="records")  # Convert DataFrame to list of dictionaries
            insert_response = self.client.table(table_name).insert(data).execute()
            
            if insert_response.data:
                logging.info(f"Successfully saved {len(insert_response.data)} new records to '{table_name}'.")
            else:
                logging.warning(f"No records inserted into '{table_name}'. Response: {insert_response}")
    
        except Exception as e:
            logging.error(f"Error in save_data for '{table_name}': {e}")

    def load_data(self, table_name):
        """Load data from a specified Supabase table."""
        try:
            logging.info(f"Loading data from '{table_name}' table.")
            response = self.client.table(table_name).select("*").execute()
            
            if response.data:
                df = pd.DataFrame(response.data)
                logging.info(f"Successfully loaded {len(df)} records from '{table_name}'.")
                return df
            else:
                logging.warning(f"No data found in '{table_name}'.")
                return pd.DataFrame()
        
        except Exception as e:
            logging.error(f"Error loading from '{table_name}': {e}")
            return pd.DataFrame()

# Instantiate the Supabase database object
db = SupabaseDatabase(supabase)


