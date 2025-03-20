import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import re
import stanza  # NLP for Malay
from googletrans import Translator
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from googleapiclient.discovery import build
from google.oauth2 import service_account

# 🔥 Initialize Firebase
cred = credentials.Certificate("firebase-credentials.json")
firebase_admin.initialize_app(cred, {"databaseURL": "https://safezone-660a9.firebaseio.com/"})

# 📊 Google Sheets API Setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SHEET_ID = "1CNo8eLCASEfd7ktOgiUrzT8KBkAWhW5sPON1BITBKvM"
RANGE_NAME = "Sheet1!A:D"

# 🔎 Initialize NLP tools
stanza.download("ms")  # Download Malay NLP model
nlp = stanza.Pipeline("ms")
translator = Translator()
stemmer = StemmerFactory().create_stemmer()

# 📌 Fetch Data from Google Sheets
def fetch_google_sheets():
    creds = service_account.Credentials.from_service_account_file("google-sheets-credentials.json", scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])
    return pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()

# 🔎 NLP Preprocessing
def preprocess_text(text):
    if not isinstance(text, str):
        return ""

    text = text.lower()
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Remove special characters

    # Convert Malay to English using Google Translate
    translated_text = translator.translate(text, src="ms", dest="en").text

    # Stanza NLP processing (Malay)
    doc = nlp(text)
    words = [word.text for sentence in doc.sentences for word in sentence.words]

    # Stemming (Malay)
    words = [stemmer.stem(word) for word in words]

    return " ".join(words)

# 📌 Crime Categorization
def categorize_crime(text):
    crime_dict = {
    "violent": [
        "murder", "homicide", "kill", "killing", "rape", "violence", "abduction","kidnap", "kidnapping"
    ],
    "property": [
        "burglary", "break-in", "trespass", "robbery", "armed robbery"
    ]
}
    
    text = text.lower()
    for category, keywords in crime_dict.items():
        if any(word in text for word in keywords):
            return category
    return "Unknown Crime"

# 🔥 Process Data and Upload to Firebase
def process_and_upload():
    print("Fetching data from Google Sheets...")
    df = fetch_google_sheets()

    if df.empty:
        print("No data found.")
        return

    df["Cleaned Text"] = df["Tweet Text"].apply(preprocess_text)
    df["Crime Category"] = df["Cleaned Text"].apply(categorize_crime)

    ref = db.reference("crime_data")

    for _, row in df.iterrows():
        ref.push({
            "timestamp": row["Timestamp"],
            "category": row["Crime Category"],
            "type": row["Main Topic"],
            "original_text": row["Tweet Text"],  
            "cleaned_text": row["Cleaned Text"]  
        })

    print("✅ Data uploaded to Firebase!")

# Run script
if __name__ == "__main__":
    process_and_upload()
