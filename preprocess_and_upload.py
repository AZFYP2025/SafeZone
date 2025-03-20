import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import re
import stanza  # NLP for Malay
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
stemmer = StemmerFactory().create_stemmer()

# 📌 Malaysian States and Districts
malaysian_states = [
    "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan", "Pahang",
    "Perak", "Perlis", "Pulau Pinang", "Sabah", "Sarawak", "Selangor",
    "Terengganu", "W.P. Kuala Lumpur", "W.P. Labuan", "W.P. Putrajaya"
]

malaysian_districts = [
    "Johor Bahru", "Batu Pahat", "Muar", "Kluang", "Kota Tinggi", "Segamat",  # Johor
    "Alor Setar", "Sungai Petani", "Kulim", "Baling", "Langkawi",  # Kedah
    "Kota Bharu", "Pasir Mas", "Gua Musang", "Bachok", "Tumpat",  # Kelantan
    "Melaka Tengah", "Alor Gajah", "Jasin",  # Melaka
    "Seremban", "Port Dickson", "Jempol", "Rembau",  # Negeri Sembilan
    "Kuantan", "Temerloh", "Bentong", "Jerantut", "Pekan",  # Pahang
    "Ipoh", "Taiping", "Manjung", "Kuala Kangsar", "Batang Padang",  # Perak
    "Kangar", "Arau", "Padang Besar",  # Perlis
    "George Town", "Seberang Perai", "Balik Pulau",  # Pulau Pinang
    "Kota Kinabalu", "Sandakan", "Tawau", "Lahad Datu", "Keningau",  # Sabah
    "Kuching", "Miri", "Sibu", "Bintulu", "Limbang",  # Sarawak
    "Shah Alam", "Petaling Jaya", "Klang", "Gombak", "Hulu Langat", "Sepang",  # Selangor
    "Kuala Terengganu", "Dungun", "Marang", "Kemaman", "Besut",  # Terengganu
    "Kuala Lumpur", "Labuan", "Putrajaya"  # WP
]

# 📌 Extract State and District from Text
def extract_location(text):
    text = text.lower()

    detected_state = "Unknown State"
    detected_district = "Unknown District"

    # Check for states in the text
    for state in malaysian_states:
        if state.lower() in text:
            detected_state = state
            break  # Stop if found

    # Check for districts in the text
    for district in malaysian_districts:
        if district.lower() in text:
            detected_district = district
            break  # Stop if found

    return detected_state, detected_district

# 📌 NLP Preprocessing
def preprocess_text(text):
    if not isinstance(text, str):
        return ""

    text = text.lower()
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Remove special characters

    # Stanza NLP processing (Malay)
    doc = nlp(text)
    words = [word.text for sentence in doc.sentences for word in sentence.words]

    # Stemming (Malay)
    words = [stemmer.stem(word) for word in words]

    return " ".join(words)

# 📌 Extract Crime Type and Category
def categorize_crime(text):
    crime_dict = {
        "assault": ["membunuh", "bunuh", "pembunuhan", "rogol", "merogol", "serangan", "pukul"],
        "property": ["curi", "mencuri", "rompak", "merompak", "rompakan"]
    }

    crime_types = {
        "murder": ["membunuh", "bunuh", "pembunuhan"],
        "theft": ["curi", "mencuri"],
        "robbery": ["rompak", "merompak", "rompakan"],
        "rape": ["rogol", "merogol"],
        "assault": ["serangan", "pukul"]
    }

    text = text.lower()

    category = "unknown"
    crime_type = "unknown"

    for cat, keywords in crime_dict.items():
        if any(word in text for word in keywords):
            category = cat
            break

    for ctype, keywords in crime_types.items():
        if any(word in text for word in keywords):
            crime_type = ctype
            break

    return category, crime_type

# 🔥 Process Data and Upload to Firebase
def process_and_upload():
    print("Fetching data from Google Sheets...")
    df = fetch_google_sheets()

    if df.empty:
        print("No data found.")
        return

    df["Cleaned Text"] = df["Tweet Text"].apply(preprocess_text)
    df[["Crime Category", "Crime Type"]] = df["Cleaned Text"].apply(lambda x: pd.Series(categorize_crime(x)))
    df[["State", "District"]] = df["Tweet Text"].apply(lambda x: pd.Series(extract_location(x)))

    ref = db.reference("crime_data")

    for _, row in df.iterrows():
        ref.push({
            "state": row["State"],
            "district": row["District"],
            "category": row["Crime Category"],
            "type": row["Crime Type"],
            "date": row["Timestamp"]
        })

    print("✅ Data uploaded to Firebase!")

# Run script
if __name__ == "__main__":
    process_and_upload()
