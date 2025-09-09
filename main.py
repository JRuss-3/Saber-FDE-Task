import sqlite3
from typing import Optional
import pandas as pd
import logging
from api_requests_client import fetch_social_handle, send_enriched_data
from datetime import datetime

# Improvement using Pydantic to validate data types
# from pydantic import BaseModel, ValidationError

# class CustomerOrdersRow(BaseModel):
#     "customer_id": str,
#     "name": str,
#     "email": str,
#     "total_spend": float,
#     "social_handle": str

# validated_rows = []
# for record in df.to_dict(orient="records"):
#     try:
#         validated = CustomerOrdersRow(**record)   # Pydantic validation
#         validated_rows.append(validated.dict())  # safe dict
#     except ValidationError as e:
#         print("Validation error:", e)

# Example DataFrame
# df = pd.DataFrame([
#     {"id": 1, "name": "Alice", "age": 30},
#     {"id": 2, "name": "Bob", "age": 25},
# ])
# Convert DataFrame rows into validated models
# users = [User(**row) for row in df.to_dict(orient="records")]


# Logging config
def setup_logging(log_file_prefix=f"customer_order_enrich"): #should probably add in the date the code is run in the name of the log file
    current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    log_file = f"{log_file_prefix}_{current_time}.log"
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

def parse_date(date_str: str) -> Optional[pd.Timestamp]:
    #"%d/%m/%Y" → matches things like 06/05/2025 (common in Europe). "%Y-%m-%d" → matches things like 2025-05-06 (ISO format, common in databases).
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"): # Defining these formats means pandas doesnt just guess (which can be Slow) and is less Ambiguous
        try:
            return pd.to_datetime(date_str, format=fmt)
        except:
            continue
    return None  # Not a valid date -> if neither format works, you safely return NaT instead of crashing.

def get_customer_orders():
    conn = sqlite3.connect("database/client_data.db")
    # Join orders and users table using the customer id and return all customer orders
    query = '''
        SELECT o.order_id, o.customer_id, o.order_date, o.order_total, c.name, c.email, c.city
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id''' #No need to use WHERE TRIM(LOWER(city)) = 'manchester';
    df = pd.read_sql_query(query, conn, params=())
    conn.close()
    return df

def filter_by_city(city: str, df: pd.DataFrame) -> pd.DataFrame:
    df['city'] = df['city'].str.strip().str.lower() # Standardise city formats in the dataset
    df = df[df['city'] == city.strip().lower()] # Filter based on selected city making sure city input is also converted to lowercase
    return df

def filter_by_start_date(date: str, df: pd.DataFrame) -> pd.DataFrame:
    start_date = parse_date(date)
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
    # filter by date where date is YYYY-MM-DD or DD/MM/YYYY
    df = filter_by_start_date(start_date, df)
    df['order_total'] = df['order_total'].round(2) # round the sums to 2dp to clean up totals col
    
    # Group the data by customer_id
    agg_df = df.groupby("customer_id").agg(
        name = ("name", "first"),
        email = ("email", "first"),
        total_spend = ("order_total", "sum")
    ).reset_index()
    
    # IMPROVEMENT -> this should probably be done before performing the sum of data e.g. df['order_total'] = df['order_total'].round(2)
    # agg_df['total_spend'] = agg_df['total_spend'].round(2)

    # Create empty columns to store results
    agg_df['social_handle'] = None
    agg_df['success'] = False
    agg_df['reason'] = None

    for idx, row in agg_df.iterrows():
        logging.info(f"Fetching social handle for row {idx} | email: {row['email']}")
        # attempt to fetch social handle for each customer
        result = fetch_social_handle(row['email'])
        # Set each column separately
        if result['data'] is not None:
            agg_df.at[idx, 'social_handle'] = result['data'].get('social_handle', None)
            agg_df.at[idx, 'success'] = True # Set True if data returned
        agg_df.at[idx, 'reason'] = result['status']
        logging.info(f"Fetch status: {result['status']}")

        # Send POST with row data
        post_data = {
            "customer_id": row['customer_id'],
            "name": row['name'] or "",
            "email": row['email'] or "",
            "total_spend": row['total_spend'] or 0,
            "social_handle": row['social_handle'] or ""
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

  




