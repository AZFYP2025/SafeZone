import pandas as pd
import gspread
from google.oauth2 import service_account
import os
import logging
import json
import tempfile

# Setup logging
logging.basicConfig(level=logging.INFO)

# Google Sheets API Setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SHEET_ID = "1CNo8eLCASEfd7ktOgiUrzT8KBkAWhW5sPON1BITBKvM"  # Hardcoded Google Sheet ID

# Load the credentials from the repository secret
GOOGLE_SHEETS_CREDENTIALS = os.getenv('GOOGLE_SHEETS_CREDENTIALS')

if not GOOGLE_SHEETS_CREDENTIALS:
    logging.error("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")
    exit(1)

# Write the credentials to a temporary file
def create_credentials_file(credentials_json):
    try:
        # Parse the JSON to ensure it's valid
        credentials = json.loads(credentials_json)
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(credentials, temp_file)
            return temp_file.name
    except json.JSONDecodeError:
        logging.error("Invalid JSON in GOOGLE_SHEETS_CREDENTIALS.")
        exit(1)

# Create the credentials file
credentials_file = create_credentials_file(GOOGLE_SHEETS_CREDENTIALS)

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
    'Klang Utara': 'Klang',
    'Cameron Highland': 'Cameron Highlands'
})

# Group by district, category, and date, and sum the crimes
df_combined = df_filtered.groupby(['district', 'category', 'date'], as_index=False)['crimes'].sum()

# Reorder columns to match your Google Sheet format
df_combined = df_combined[['district', 'category', 'date', 'crimes']]

# Format date for Google Sheets
df_combined['date'] = df_combined['date'].dt.strftime('%Y-%m-%d')

# Upload to Google Sheets
def upload_to_google_sheets(dataframe, sheet_id, credentials_file, worksheet_name="SafeZoneGOV"):
    try:
        # Load credentials and authorize the client
        creds = service_account.Credentials.from_service_account_file(
            credentials_file, scopes=SCOPES
        )
        client = gspread.authorize(creds)

        # Open the Google Sheet by ID and select the worksheet
        sheet = client.open_by_key(sheet_id).worksheet(worksheet_name)

        # Clear existing data in the sheet (optional)
        sheet.clear()

        # Convert the DataFrame to a list of lists
        data_to_upload = dataframe.values.tolist()

        # Add the header row
        header = dataframe.columns.tolist()
        data_to_upload.insert(0, header)

        # Calculate the range dynamically based on the size of the data
        num_rows = len(data_to_upload)
        num_cols = len(data_to_upload[0]) if num_rows > 0 else 0
        range_name = f"A1:{chr(64 + num_cols)}{num_rows}"  # Example: "A1:E10"

        # Upload the data to the Google Sheet
        sheet.update(range_name, data_to_upload)

        logging.info("Data uploaded to Google Sheets successfully!")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Upload the preprocessed data to Google Sheets
upload_to_google_sheets(df_combined, SHEET_ID, credentials_file, worksheet_name="SafeZoneGOV")

# Clean up the temporary credentials file
os.remove(credentials_file)
