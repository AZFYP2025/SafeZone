import pandas as pd
import gspread
from google.oauth2 import service_account
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Google Sheets API Setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SHEET_ID = os.getenv('SHEET_ID', "1CNo8eLCASEfd7ktOgiUrzT8KBkAWhW5sPON1BITBKvM")
credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE', "google-credentials.json")

# Load the data from the public URL
URL_DATA = 'https://storage.data.gov.my/publicsafety/crime_district.parquet'
df = pd.read_parquet(URL_DATA)

# Check for required columns
required_columns = ['district', 'category', 'date', 'crimes']
if not all(column in df.columns for column in required_columns):
    raise ValueError(f"DataFrame is missing one or more required columns: {required_columns}")

# Convert 'date' to datetime if it exists
df['date'] = pd.to_datetime(df['date'])

# Filter the DataFrame to include only the required columns
df_filtered = df[required_columns].copy()

# Combine districts as specified
df_filtered['district'] = df_filtered['district'].replace({
    'Johor Bahru Selatan': 'Johor Bahru',
    'Johor Bahru Utara': 'Johor Bahru',
    'Seberang Perai Selatan': 'Seberang Perai',
    'Seberang Perai Tengah': 'Seberang Perai',
    'Seberang Perai Utara': 'Seberang Perai',
    'Klang Selatan': 'Klang',
    'Klang Utara': 'Klang'
})

# Group by district, category, and date, and sum the crimes
df_combined = df_filtered.groupby(['district', 'category', 'date'], as_index=False)['crimes'].sum()

# Add a placeholder 'state' column (if needed)
df_combined['state'] = ''

# Reorder columns to match your Google Sheet format
df_combined = df_combined[['state', 'district', 'category', 'date', 'crimes']]

# Format date for Google Sheets
df_combined['date'] = df_combined['date'].dt.strftime('%Y-%m-%d')

# Upload to Google Sheets
def upload_to_google_sheets(dataframe, sheet_id, credentials_file, worksheet_name="SafeZoneGOV"):
    try:
        # Correct usage of from_service_account_file
        creds = service_account.Credentials.from_service_account_file(
            credentials_file, scopes=SCOPES  # Use scopes as a keyword argument
        )
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
        sheet.clear()

        data_to_upload = dataframe.values.tolist()
        header = dataframe.columns.tolist()
        data_to_upload.insert(0, header)

        num_rows = len(data_to_upload)
        num_cols = len(data_to_upload[0]) if num_rows > 0 else 0
        range_name = f"A1:{chr(64 + num_cols)}{num_rows}"

        sheet.update(range_name, data_to_upload)
        logging.info("Data uploaded to Google Sheets successfully!")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Upload the preprocessed data to Google Sheets
upload_to_google_sheets(df_combined, SHEET_ID, credentials_file)
