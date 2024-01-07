CREATE OR REPLACE PROCEDURE SAMPLE_DATA.TPCH_SF1.READ_SHEET(
        spreadsheet_id string,
        sheet_range string,
        table_name string,
        database_ string,
        schema_ string
    )
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.8
    HANDLER = 'read_from_gsheet'
    EXTERNAL_ACCESS_INTEGRATIONS = (gsheet_api_access_integration)
    PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas', 'numpy')
    SECRETS = ('credential' = sample_data.tpch_sf1.gsheet_secret)
    AS $$
import _snowflake
import requests
import json
import snowflake.snowpark as snowpark
import pandas as pd
import numpy as np
from datetime import datetime

class GoogleSheetFetchError(Exception):
    pass
   
def read_from_gsheet(session, spreadsheet_id, sheet_range, table_name, database_, schema_):

    # URL for the Google Sheets API
    base_url = f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}'

    access_token = _snowflake.get_oauth_access_token('credential')
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }
 
    # Prepare the request URL
    url = f'{base_url}/values/{sheet_range}?'
    
    # Make a POST request to update the sheet
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        values = data.get('values', [])
        # Apply normalization to column names
        column_names = values[0]
        snowpark_df = pd.DataFrame(values[1:], columns=column_names)
        #fill empty column
        snowpark_df = snowpark_df.replace('', np.nan)
        session.write_pandas(df=snowpark_df, table_name=table_name, database=database_, schema= schema_, auto_create_table= True, overwrite= True)
 
        print('Data written to Google Sheet successfully.')
    else:
        raise GoogleSheetFetchError(f'Google Sheets error: {response.status_code} - {response.text}')
        # print(f'Error reading data from Google Sheet: {response.status_code} - {response.text}')
    return response.status_code
$$;


CALL SAMPLE_DATA.TPCH_SF1.WRITE_TO_GSHEET('SAMPLE_DATA.TPCH_SF1.CUSTOMER', '1nhESCkbzfd8b6-iYs3mTtxT8xdomNqHzxcFR_RhWCiQ', 'sheet1!A-Z');
