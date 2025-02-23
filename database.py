import pandas as pd
import datetime
import sqlite3
import logging

# Set up logging
logging.basicConfig(
    filename='property_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def save_list_properties_db(property_number, update_time):
    try:
        # Log the function call
        logging.info(f"Starting save_list_properties_db with {len(property_number)} property numbers.")

        # Convert to DataFrame
        df = pd.DataFrame(property_number, columns=['property_number'])
        df['update_time'] = update_time

        # Convert all values to string
        df = df.astype(str)

        # Save to SQLite
        conn = sqlite3.connect('data/results.db')
        df.to_sql('property_numbers_list', conn, if_exists='replace', index=False)
        conn.close()

        # Log success
        logging.info(f"Successfully saved {len(property_number)} property numbers to database.")
    
    except Exception as e:
        # Log any errors that occur
        logging.error(f"Error in save_list_properties_db: {e}")

def load_list_properties_db():
    try:
        # Log the function call
        logging.info("Starting load_list_properties_db.")

        # Connect to the SQLite database
        conn = sqlite3.connect('data/results.db')

        # Query the database table
        query = "SELECT * FROM property_numbers_list"
        df = pd.read_sql_query(query, conn)

        # Close the connection
        conn.close()

        # Log success
        logging.info(f"Successfully loaded {len(df)} records from the database.")

        return df
    
    except Exception as e:
        # Log any errors that occur
        logging.error(f"Error in load_list_properties_db: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error

def save_data_listing_db(df):
    try:
        # Log the function call
        logging.info(f"Starting save_data_listing_db with {len(df)} records.")

        # Convert all values to string
        df = df.astype(str)

        # Save to SQLite
        conn = sqlite3.connect('data/results.db')
        df.to_sql('listings', conn, if_exists='replace', index=False)
        conn.close()

        # Log success
        logging.info(f"Successfully saved {len(df)} records to the 'listings' table.")
    
    except Exception as e:
        # Log any errors that occur
        logging.error(f"Error in save_data_listing_db: {e}")

def load_data_listing_db():
    try:
        # Log the function call
        logging.info("Starting load_data_listing_db.")

        # Connect to the SQLite database
        conn = sqlite3.connect('data/results.db')

        # Query the database table
        query = "SELECT * FROM listings"
        df = pd.read_sql_query(query, conn)

        # Close the connection
        conn.close()

        # Log success
        logging.info(f"Successfully loaded {len(df)} records from the 'listings' table.")

        return df
    
    except Exception as e:
        # Log any errors that occur
        logging.error(f"Error in load_data_listing_db: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error

def save_data_history_db(df):
    try:
        # Log the function call
        logging.info(f"Starting save_data_history_db with {len(df)} records.")

        # Convert all values to string
        df = df.astype(str)

        # Save to SQLite
        conn = sqlite3.connect('data/results.db')
        df.to_sql('listings_history', conn, if_exists='replace', index=False)
        conn.close()

        # Log success
        logging.info(f"Successfully saved {len(df)} records to the 'listings_history' table.")
    
    except Exception as e:
        # Log any errors that occur
        logging.error(f"Error in save_data_history_db: {e}")

def load_data_history_db():
    try:
        # Log the function call
        logging.info("Starting load_data_history_db.")

        # Connect to the SQLite database
        conn = sqlite3.connect('data/results.db')

        # Query the database table
        query = "SELECT * FROM listings_history"
        df = pd.read_sql_query(query, conn)

        # Close the connection
        conn.close()

        # Log success
        logging.info(f"Successfully loaded {len(df)} records from the 'listings_history' table.")

        return df
    
    except Exception as e:
        # Log any errors that occur
        logging.error(f"Error in load_data_history_db: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error
