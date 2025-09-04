import time
import random
import sqlite3
import pandas as pd
import requests
import logging

# Logging config
def setup_logging(log_file="customer_order_enrich.log"):
    logging.basicConfig(
        filename=log_file,
        filemode='w',          # overwrite each run
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    # Also write log to console
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


def fetch_enriched_customer_data(email, max_retries=5):
    """Fetch API data for a given email with retries and error handling."""
    if not email:
        return {"data": None, "status": "No email provided"}

    url = "http://localhost:5000/enrichment"
    headers = {
        "X-API-KEY": "SECRET_KEY_123"
    }
    params = {
        "email": email
    }
    params = {"email": email}
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers, params=params)
            # Raise for status for non-2xx responses
            response.raise_for_status()
            # Check is no data returned from enriched data endpoint
            if response.status_code == 204 or not response.json():  # 204 = No Content
                return {"data": None, "status": f"No profile found for {email}"}

            return {"data": response.json(), "status": "Successfully found social handle for user"}

        except requests.HTTPError as http_err:
            status = response.status_code
            if status == 429:  # Rate limit
                wait_time = 2 ** retries + random.random()  # exponential backoff + jitter
                logging.info(f"Rate limited for {email}. Waiting {wait_time:.1f} seconds before retrying...")
                time.sleep(wait_time)
                retries += 1
            elif 500 <= status < 600:  # Server errors
                wait_time = 2 ** retries + random.random()
                logging.info(f"Server error ({status}) for {email}. Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                retries += 1
            elif status == 404:  # Missing profile
                return {"data": None, "status": "Profile not found (404)"}
            else:
                return {"data": None, "status": f"HTTP error {status}: {http_err}"}

        except requests.RequestException as e:
            # Handle network errors
            wait_time = 2 ** retries + random.random()
            logging.info(f"Network error for {email}: {e}. Retrying in {wait_time:.1f} seconds...")
            time.sleep(wait_time)
            retries += 1

    return {"data": None, "status": f"Max retries ({max_retries}) exceeded"}

def send_enriched_customer_data(data, max_retries=5):
    # Send encriched customer data to submissions API with retries and error handling.
    url = "http://localhost:5000/submission"
    headers = {
        "X-API-KEY": "SECRET_KEY_123",
        "Content-Type": "application/json"
    }
    retries = 0

    while retries < max_retries:
        try:
            response = requests.post(url, headers=headers, json=data)
            # Raise for status for non-2xx responses
            response.raise_for_status()
            # Check is no data returned from enriched data endpoint
            return {"data": response.json(), "status": "Successfully posted user data"}

        except requests.HTTPError as http_err:
            status = response.status_code
            if status == 429:  # Rate limit
                wait_time = 2 ** retries + random.random()  # exponential backoff + jitter
                logging.info(f"Rate limited waiting {wait_time:.1f} seconds before retrying...")
                time.sleep(wait_time)
                retries += 1
            elif 500 <= status < 600:  # Server errors
                wait_time = 2 ** retries + random.random()
                logging.info(f"Server error ({status}) while sending CustomerID: {data['customer_id']}. Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                retries += 1
            elif status == 422:  # Validation Error
                return {"data": response.json(), "status": "Validation Error (422)"}
            else:
                return {"data": None, "status": f"HTTP error {status}: {http_err}"}

        except requests.RequestException as e:
            # Handle network errors
            wait_time = 2 ** retries + random.random()
            logging.info("Network error while sending CustomerID: {data['customer_id']} with Error: {e}. Retrying in {wait_time:.1f} seconds...")
            time.sleep(wait_time)
            retries += 1

    return {"data": None, "status": f"Max retries ({max_retries}) exceeded while sending CustomerID: {data['customer_id']}"}

def filter_by_city(city, df):
    # standardise city formats in the dataset and filter based on selected city
    df['city'] = df['city'].str.strip().str.lower()
    df = df[df['city'] == city]
    return df

def filter_by_start_date(start_date, df):
    # standardise date format and filter where start_date is YYYY-MM-DD
    start_date = pd.to_datetime('2024-09-01')
    df['order_date'] = df['order_date'].apply(parse_date)
    df = df[(df['order_date'] >= start_date)]
    return df

if __name__ == "__main__":
    setup_logging()
    start_date = '2024-09-01'
    city = 'manchester'
    logging.info(f"Starting enrichment of order data for customers living in {city} since date:{start_date}")
    # Fetch all customer orders from the db as DB is fairly small and the data can be cleaned and filtered after
    df = get_customer_orders()
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
        result = fetch_enriched_customer_data(row['email'])
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
        result = send_enriched_customer_data(post_data)
        logging.info(f"Post Status: {result}")

    # Output final dataframe as CSV
    logging.info(f"Writing output to CSV")
    agg_df.to_csv(f"customer_orders_since_{start_date}.csv", index=False)

    logging.info(f"Process complete")




