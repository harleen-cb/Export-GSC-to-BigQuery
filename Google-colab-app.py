#Import necessary libraries

import argparse
import httplib2
import requests
import os
import pandas as pd

from collections import defaultdict
from dateutil import relativedelta
from googleapiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from oauth2client.client import OAuth2WebServerFlow

from googleapiclient.discovery import build
from oauth2client.file import Storage

from google.cloud import bigquery
from google.oauth2 import service_account


# Authenticate this app to read GSC data

creds = '/content/client_secrets.json'

def authorize_creds(creds):
    print('Authorizing Creds')
    # Variable parameter that controls the set of resources that the access token permits.
    SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']

    # Path to client_secrets.json file
    creds = 'client_secrets.json'
    CLIENT_SECRETS_PATH = creds

    # Create a parser to be able to open browser for Authorization
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    flags = parser.parse_args([])

    # Creates an authorization flow from a clientsecrets file.
    # Will raise InvalidClientSecretsError for unknown types of Flows.
    flow = client.flow_from_clientsecrets(
        CLIENT_SECRETS_PATH, scope = SCOPES,
        message = tools.message_if_missing(CLIENT_SECRETS_PATH))

    # Prepare credentials and authorize HTTP
    # If they exist, get them from the storage object
    # credentials will get written back to the 'authorizedcreds.dat' file.
    storage = file.Storage('authorizedcreds.dat')
    credentials = storage.get()

    # If authenticated credentials don't exist, open Browser to authenticate
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)      # Add the valid creds to a variable

    # Take the credentials and authorize them using httplib2
    http = httplib2.Http()                                      # Creates an HTTP client object to make the http request
    http = credentials.authorize(http=http)                     # Sign each request from the HTTP client with the OAuth 2.0 access token
    webmasters_service = build('searchconsole', 'v1', http=http)   # Construct a Resource to interact with the API using the Authorized HTTP Client.

    print('Auth Successful')
    return webmasters_service

# Create Function to execute your API Request
def execute_request(service, property_uri, request):
    return service.searchanalytics().query(siteUrl=property_uri, body=request).execute()

authorize_creds(creds)



# Extract GSC data

credentials_path = 'authorizedcreds.dat'

if os.path.exists(credentials_path):
    # Load credentials from file
    storage = Storage(credentials_path)
    credentials = storage.get()
else:
    # Run the OAuth flow with automated code retrieval
    credentials = run_flow(flow, Storage(credentials_path))

# Build the service
service = build('searchconsole', 'v1', credentials=credentials)

# Select property, start date and end date
# https://developers.google.com/webmaster-tools/v1/searchanalytics/query
property_url = 'sc-domain:cueblocks.com'
startDate = '2023-11-01'
endDate = '2024-05-31'
dimensions = ['date','page', 'query', ]

# Create an empty list to store the rows retrieved from the response
data = []

# Initialize the variable 'startRow' to track the starting row of each request
startRow = 0

while startRow == 0 or startRow % 25000 == 0:
    # Build the request body with the specified variables
    request = {
        'startDate': startDate,
        'endDate': endDate,
        'dimensions': dimensions,
        'rowLimit': 25000,
        'startRow': startRow
    }

    # Store the response from the Google Search Console API
    response = service.searchanalytics().query(siteUrl=property_url, body=request).execute()

    # Get and update the rows
    rows = response.get('rows', [])
    startRow = startRow + len(rows)

    # Extend the data list with the rows
    data.extend(rows)

# Create a DataFrame from the data list
df = pd.DataFrame([
    {
        'date': row['keys'][0],
        'page': row['keys'][1],
        'query': row['keys'][2],
        #'country': row['keys'][2],
        #'devices': row['keys'][3],
        'clicks': row['clicks'],
        'impressions': row['impressions'],
        'ctr': row['ctr'],
        'position': row['position']
    } for row in data
])

# Save the DataFrame as a CSV file
df.to_csv('data.csv')



# Upload data to BigQuery


from google.cloud.exceptions import NotFound

project_id = 'AbbaDabbaChabba'  # Your Google Cloud Project ID
dataset_id = 'GSC_Nov23_May24'  # BigQuery Dataset name where the data will be stored
table_id = 'GSC_export'  # BigQuery Table name where the data will be stored

# Path to your service account key file
key_path = '/content/my-key.json'

# Create credentials
credentials = service_account.Credentials.from_service_account_file(key_path)

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """Uploads the DataFrame to Google BigQuery."""
    # The DataFrame's column names are formatted for BigQuery compatibility
    #df.columns = [col.replace('ga:', 'gs_') for col in df.columns]

    bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    schema = []

    # Generating schema based on DataFrame columns
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            bq_type = 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            bq_type = 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            bq_type = 'BOOLEAN'
        else:
            bq_type = 'STRING'
        schema.append(bigquery.SchemaField(col, bq_type))

    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        # Creating a new table if it does not exist
        table = bigquery.Table(table_ref, schema=schema)
        bigquery_client.create_table(table)
        print(f"Created table {table_id}")

    # Loading data into BigQuery and confirming completion
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    load_job = bigquery_client.load_table_from_dataframe(df, table_ref)
    load_job.result()
    print(f"Data uploaded to {full_table_id}")

upload_to_bigquery(df, project_id, dataset_id, table_id)
