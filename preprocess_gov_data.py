import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# Load Data
URL_DATA = 'https://storage.data.gov.my/publicsafety/crime_district.parquet'
df = pd.read_parquet(URL_DATA)

# Convert 'date' column to datetime format if it exists
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'])

# Create a copy to avoid SettingWithCopyWarning
df_filtered = df[['district', 'category', 'date', 'crimes']].copy()

# Normalize district names (merging districts)
df_filtered.loc[:, 'district'] = df_filtered['district'].replace({
    'Johor Bahru Selatan': 'Johor Bahru',
    'Johor Bahru Utara': 'Johor Bahru',
    'Klang Selatan': 'Johor Bahru',
    'Klang Utara': 'Johor Bahru',
    'Seberang Perai Selatan': 'Seberang Perai',
    'Seberang Perai Tengah': 'Seberang Perai',
    'Seberang Perai Utara': 'Seberang Perai'
})

# Group by district, category, and date, summing up the crime numbers
df_grouped = df_filtered.groupby(['district', 'category', 'date'], as_index=False)['crimes'].sum()

# Google Sheets API Setup
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "SafeZone"  # Google Sheet name
WORKSHEET_NAME = "SafeZoneGOV"  # Specific sheet inside SafeZone
SERVICE_ACCOUNT_FILE = "google-credentials.json"  # Replace with actual credentials file

# Authenticate and Connect to Google Sheets
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# Function to Upload Data to Google Sheets (SafeZone Sheet)
def upload_to_google_sheets(df):
    try:
        sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)  # Open "SafeZone" sheet
        data = [df.columns.tolist()] + df.values.tolist()
        sheet.clear()  # Clear existing data
        sheet.update(data)
        print(f"Data successfully uploaded to '{WORKSHEET_NAME}' in '{SHEET_NAME}'.")
    except Exception as e:
        print(f"Error uploading to Google Sheets: {e}")

# Upload processed crime data
upload_to_google_sheets(df_grouped)
