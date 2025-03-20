import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Download stopwords for NLP
nltk.download("stopwords")

# 🔥 Initialize Firebase
cred = credentials.Certificate("firebase-credentials.json")
firebase_admin.initialize_app(cred, {"databaseURL": "https://console.firebase.google.com/u/0/project/safezone-660a9/database/safezone-660a9-default-rtdb/data"})

# 📊 Google Sheets API Setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SHEET_ID = "your_google_sheet_id"
RANGE_NAME = "Sheet1!A:D"  # Adjust range as needed

def fetch_google_sheets():
    creds = service_account.Credentials.from_service_account_file("google-sheets-credentials.json", scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])
    return pd.DataFrame(values[1:], columns=values[0])  # Convert to DataFrame

# 🔎 NLP Preprocessing
def clean_text(text):
    text = text.lower()
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Remove special characters
    words = text.split()
    words = [word for word in words if word not in stopwords.words("english")]
    return " ".join(words)

def categorize_crime(text):
    crime_dict = {
        "assault": ["murder", "assault", "robbery", "rape"],
        "property": ["theft", "burglary", "fraud", "vandalism"]
    }
    text = text.lower()
    for category, keywords in crime_dict.items():
        if any(word in text for word in keywords):
            return category.capitalize() + " Crime"
    return "Unknown Crime"

# 🔥 Process Data and Upload to Firebase
def process_and_upload():
    print("Fetching data from Google Sheets...")
    df = fetch_google_sheets()

    if df.empty:
        print("No data found.")
        return

    df["Cleaned Text"] = df["Tweet Text"].apply(clean_text)
    df["Crime Category"] = df["Cleaned Text"].apply(categorize_crime)

    ref = db.reference("crime_data")

    for _, row in df.iterrows():
        ref.push({
            "timestamp": row["Timestamp"],
            "crime_category": row["Crime Category"],
            "crime_type": row["Main Topic"],
            "description": row["Cleaned Text"]
        })

    print("✅ Data uploaded to Firebase!")

if __name__ == "__main__":
    process_and_upload()
