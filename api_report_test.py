import os
import requests
import json
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# Constants
PAGE_SIZE = 200
PROJECT_ID = "rosicrucians"
DATASET_ID = "keap"
TOKEN_TABLE = "oauth_tokens"
PAGE_TOKEN_FILE = "next_page_token"

SEARCH_ID = 1664
TABLE_NAME = "sales_keap"

def get_last_saved_token():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT access_token
        FROM `{PROJECT_ID}.{DATASET_ID}.{TOKEN_TABLE}`
        ORDER BY timestamp DESC
        LIMIT 1
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row.access_token
    return None

def save_next_page_token(token, search_id):
    with open(f"{PAGE_TOKEN_FILE}_{search_id}.txt", "w") as file:
        file.write(token if token else "")

def load_next_page_token(search_id):
    try:
        with open(f"{PAGE_TOKEN_FILE}_{search_id}.txt", "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        return None

def fetch_data_from_api(search_id, table_name):
    all_data = []
    next_page_token = load_next_page_token(search_id)
    batch_size = 1000
    page_count = 0
    print(f"Fetching for search_id={search_id} (table: {table_name})")

    while True:
        access_token = get_last_saved_token()
        if not access_token:
            raise Exception("No access token found in the database.")

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        base_url = f"https://api.infusionsoft.com/crm/rest/v2/reporting/reports/{search_id}:run?page_size={PAGE_SIZE}"
        if next_page_token:
            base_url += f"&page_token={next_page_token}"

        response = requests.post(base_url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"API request failed for search_id={search_id} with status code {response.status_code}: {response.text}")

        json_data = response.json()

        if "results" in json_data:
            for result in json_data["results"]:
                row = {col["field_name"]: col["value"] for col in result.get("columns", [])}
                all_data.append(row)

                if len(all_data) >= batch_size:
                    df = pd.DataFrame(all_data)
                    load_data_to_bigquery(df, table_name)
                    save_next_page_token(next_page_token, search_id)
                    all_data = []

        page_count += 1
        print(f"Progress: Page {page_count} processed for search_id {search_id}.")

        next_page_token = json_data.get("page_token")
        if not next_page_token:
            break

    if all_data:
        save_next_page_token(next_page_token, search_id)
        df = pd.DataFrame(all_data)
        load_data_to_bigquery(df, table_name)
        print(f"Final batch of {len(df)} rows inserted for search_id {search_id}.")

def load_data_to_bigquery(df, table_name):
    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}.")

def main():
    try:
        fetch_data_from_api(search_id=SEARCH_ID, table_name=TABLE_NAME)
        token_file = f"{PAGE_TOKEN_FILE}_{SEARCH_ID}.txt"
        if os.path.exists(token_file):
            os.remove(token_file)
        print(f"Completed: {TABLE_NAME}")
    except Exception as e:
        print(f"Error with {TABLE_NAME}: {e}")
        token = load_next_page_token(SEARCH_ID)
        if token:
            print(f"Saved page token: {token}")
        else:
            print("No saved page token.")

if __name__ == "__main__":
    main()