import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Google Sheets API Setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SHEET_ID = "1CNo8eLCASEfd7ktOgiUrzT8KBkAWhW5sPON1BITBKvM"

# Load the data from the public URL
URL_DATA = 'https://storage.data.gov.my/publicsafety/crime_district.parquet'
df = pd.read_parquet(URL_DATA)

# Convert 'date' to datetime if it exists
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'])

# Filter the DataFrame to include only the required columns
df_filtered = df[['district', 'category', 'date', 'crimes']].copy()  # Use .copy() to avoid the warning

# Combine districts as specified
df_filtered['district'] = df_filtered['district'].replace({
    'Johor Bahru Utara Selatan': 'Johor Bahru',
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
df_combined['state'] = ''  # You can fill this with the appropriate state if available

# Reorder columns to match your Google Sheet format
df_combined = df_combined[['state', 'district', 'category', 'date', 'crimes']]

# Upload to Google Sheets
def upload_to_google_sheets(dataframe, sheet_id, credentials_file):
    # Define the scope
    scope = SCOPES

    # Add your credentials JSON file
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)

    # Authorize the client
    client = gspread.authorize(creds)

    # Open the Google Sheet by ID and select the worksheet
    sheet = client.open_by_key(sheet_id).worksheet("SafeZone")  # Specify the sheet name

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

    print("Data uploaded to Google Sheets successfully!")

# Replace with your Google Sheet ID and credentials file path
CREDENTIALS_FILE = "google-credentials.json"  # Path to your Google API credentials JSON file

# Upload the preprocessed data to Google Sheets
upload_to_google_sheets(df_combined, SHEET_ID, CREDENTIALS_FILE)
