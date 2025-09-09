

import logging
import random
import time
import requests

#Fetch API data for a given email with retries and error handling
def fetch_social_handle(email, max_retries=5):
    if not email:
        return {"data": None, "status": "No email provided"}

    url = "http://localhost:5000/enrichment"
    headers = {
        "X-API-KEY": "SECRET_KEY_123"
    }
    params = {"email": email}
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers, params=params)
            # Raise for status for non-2xx responses
            response.raise_for_status()
            # Check is no data returned from endpoint
            if response.status_code == 204 or not response.json():
                return {"data": None, "status": f"No data returned for {email}"}

            return {"data": response.json(), "status": "Successfully found social handle for user"}

        except requests.HTTPError as http_err:
            status = response.status_code
            if status == 429:  # Rate limit
                # should probably add in a check for the retry after param wait_time = response.headers.get("Retry-After") and use that to set the time delay is exists
                wait_time = 2 ** retries + random.random()  # exponential backoff + jitter designed to stop all retries happening at the same time
                logging.warning(f"Rate limited for {email}. Waiting {wait_time:.1f} seconds before retrying...")
                time.sleep(wait_time)
                retries += 1
            elif 500 <= status < 600:  # Server errors
                wait_time = 2 ** retries + random.random()
                logging.warning(f"Server error ({status}) for {email}. Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                retries += 1
            elif status == 404:  # Missing profile
                return {"data": None, "status": "Profile not found (404)"}
            else:
                logging.error(f"HTTP error {status}: {http_err}")
                return {"data": None, "status": f"HTTP error {status}: {http_err}"}

        except requests.RequestException as e:
            # Handle network errors
            wait_time = 2 ** retries + random.random()
            logging.warning(f"Network error for {email}: {e}. Retrying in {wait_time:.1f} seconds...")
            time.sleep(wait_time)
            retries += 1
    logging.error(f"Error fetching social handle for customer. Max retries ({max_retries}) exceeded")
    return {"data": None, "status": f"Max retries ({max_retries}) exceeded"}

def send_enriched_data(data, max_retries=5):
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
            return response.json()

        except requests.HTTPError as http_err:
            status = response.status_code
            if status == 429:  # Rate limit
                wait_time = 2 ** retries + random.random()  # exponential backoff + jitter
                logging.warning(f"Rate limited waiting {wait_time:.1f} seconds before retrying...")
                time.sleep(wait_time)
                retries += 1
            elif 500 <= status < 600:  # Server errors
                wait_time = 2 ** retries + random.random()
                logging.warning(f"Server error ({status}) while sending CustomerID: {data['customer_id']}. Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                retries += 1
            elif status == 422:  # Validation Error
                logging.error(f"Data validation error: {response.json()}")
                return response.json()
            else:
                logging.error(f"HTTP error {status}: {http_err}")
                return f"HTTP error {status}: {http_err}"

        except requests.RequestException as e:
            # Handle network errors
            wait_time = 2 ** retries + random.random()
            # not converted the logging response into an f string 
            logging.warning(f"Network error while sending CustomerID: {data['customer_id']} with Error: {e}. Retrying in {wait_time:.1f} seconds...")
            time.sleep(wait_time)
            retries += 1
    logging.error(f"Max retries ({max_retries}) exceeded while sending CustomerID: {data['customer_id']}")
    return f"Max retries ({max_retries}) exceeded while sending CustomerID: {data['customer_id']}"