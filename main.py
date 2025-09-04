import re
import sqlite3
import pandas as pd
import logging
from api_requests_client import fetch_social_handle, send_enriched_data

# Logging config
def setup_logging(log_file="customer_order_enrich.log"):
    logging.basicConfig(
        filename=log_file,
        filemode='w',  
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    # Write log to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

def parse_date(date_str):
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return pd.to_datetime(date_str, format=fmt)
        except:
            continue
    return pd.NaT  # Not a valid date

def get_customer_orders():
    conn = sqlite3.connect("database/client_data.db")
    # Join orders and users table using the customer id and return all customer orders
    query = '''
        SELECT o.order_id, o.customer_id, o.order_date, o.order_total, c.name, c.email, c.city
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id'''
    df = pd.read_sql_query(query, conn, params=())
    conn.close()
    return df

def filter_by_city(city: str, df):
    # Standardise city formats in the dataset
    df['city'] = df['city'].str.strip().str.lower()
    # Filter based on selected city making sure city input is also converted to lowercase
    df = df[df['city'] == city.strip().lower()]
    return df

def filter_by_start_date(start_date: str, df):
    # standardise date format and filter where start_date is YYYY-MM-DD
    date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if not date_regex.match(start_date):
        logging.warning(f"Start date does not match the required format of YYYY-MM-DD")
    start_date = pd.to_datetime(start_date)
    df['order_date'] = df['order_date'].apply(parse_date)
    df = df[(df['order_date'] >= start_date)]
    return df

def enrich_customer_orders():
    setup_logging()
    start_date = '2024-09-01'
    city = 'manchester'
    logging.info(f"Starting enrichment of order data for customers living in {city} since date:{start_date}")
    # Fetch all customer orders from the db as DB is fairly small and the data can be cleaned and filtered after
    df = get_customer_orders()
    if df.empty:
        logging.warning(f"No data returned form db for date range and city provided")
    logging.info(f"Data successfully returned from db")
    df = filter_by_city(city, df)
    # filter by date where date is YYYY-MM-DD
    df = filter_by_start_date(start_date, df) 

    # Group the data by customer_id
    agg_df = df.groupby("customer_id").agg(
        name = ("name", "first"),
        email = ("email", "first"),
        total_spend = ("order_total", "sum")
    ).reset_index()

    # Create empty columns to store results
    agg_df['social_handle'] = None
    agg_df['success'] = False
    agg_df['reason'] = None

    for idx, row in agg_df.iterrows():
        logging.info(f"Fetching social handle for row {idx} | email: {row['email']}")
        # attempt to fetch social handle for each customer
        result = fetch_social_handle(row['email'])
        # Set each column separately
        social_handle = None
        if result['data'] is not None:
            social_handle = result['data'].get('social_handle', None)
        agg_df.at[idx, 'social_handle'] = social_handle
        agg_df.at[idx, 'success'] = result['data'] is not None  # Set True if data returned
        agg_df.at[idx, 'reason'] = result['status']
        logging.info(f"Fetch status: {result['status']}")

        # Send POST with row data
        post_data = {
            "customer_id": row['customer_id'],
            "name": row['name'] or "",
            "email": row['email'] or "",
            "total_spend": row['total_spend'] or 0,
            "social_handle": social_handle or ""
        }
        logging.info(f"Sending data to submissions endpoint")
        result = send_enriched_data(post_data)
        logging.info(f"Post Status: {result}")

    # Output final dataframe as CSV
    logging.info(f"Writing output to CSV")
    agg_df.to_csv(f"customer_orders_since_{start_date}.csv", index=False)
    logging.info(f"Process complete.")


if __name__ == "__main__":
    enrich_customer_orders()

  




