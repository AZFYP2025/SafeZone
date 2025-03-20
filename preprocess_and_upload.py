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
    "Terengganu", "W.P. Kuala Lumpur"
]

malaysian_districts = [
    "Batu Pahat", "Iskandar Puteri", "Johor Bahru Selatan", "Johor Bahru Utara", "Kluang",
    "Kota Tinggi", "Kulaijaya", "Ledang", "Mersing", "Muar", "Nusajaya", "Pontian", "Segamat", "Seri Alam",  # Johor
    "Baling", "Bandar Bharu", "Kota Setar", "Kuala Muda", "Kubang Pasu", "Kulim",
    "Langkawi", "Padang Terap", "Pendang", "Sik", "Yan",  # Kedah
    "Bachok", "Gua Musang", "Jeli", "Kota Bharu", "Kuala Krai", "Machang", "Pasir Mas",
    "Pasir Puteh", "Tanah Merah", "Tumpat",  # Kelantan
    "Alor Gajah", "Jasin", "Melaka Tengah",  # Melaka
    "Jelebu", "Jempol", "Kuala Pilah", "Nilai", "Port Dickson", "Rembau", "Seremban", "Tampin",  # Negeri Sembilan
    "Bentong", "Bera", "Cameron Highland", "Jerantut", "Kuala Lipis", "Kuantan",
    "Maran", "Pekan", "Raub", "Rompin", "Temerloh",  # Pahang
    "Batu Gajah", "Gerik", "Hilir Perak", "Ipoh", "Kampar", "Kerian", "Kuala Kangsar",
    "Manjung", "Pengkalan Hulu", "Perak Tengah", "Selama", "Sungai Siput", "Taiping",
    "Tanjong Malim", "Tapah",  # Perak
    "Arau", "Kangar", "Padang Besar",  # Perlis
    "Barat Daya", "Seberang Perai Selatan", "Seberang Perai Tengah", "Seberang Perai Utara", "Timur Laut",  # Pulau Pinang
    "Beaufort", "Beluran", "Keningau", "Kota Belud", "Kota Kinabalu", "Kinabatangan",
    "Kota Marudu", "Kudat", "Kunak", "Lahad Datu", "Papar", "Penampang", "Ranau",
    "Sandakan", "Semporna", "Sipitang", "Tawau", "Tenom", "Tuaran",  # Sabah
    "Bau", "Belaga", "Betong", "Bintulu", "Dalat", "Julau", "Kanowit", "Kapit",
    "Kota Samarahan", "Kuching", "Lawas", "Limbang", "Lubok Antu", "Lundu", "Marudi",
    "Matu Daro", "Meradong", "Miri", "Mukah", "Padawan", "Saratok", "Sarikei",
    "Serian", "Sibu", "Simunjan", "Song", "Sri Aman", "Tatau",  # Sarawak
    "Ampang Jaya", "Gombak", "Hulu Selangor", "Kajang", "Klang Selatan", "Klang Utara",
    "Kuala Langat", "Kuala Selangor", "Petaling Jaya", "Sabak Bernam", "Sepang",
    "Serdang", "Sg. Buloh", "Shah Alam", "Subang Jaya",  # Selangor
    "Besut", "Dungun", "Hulu Terengganu", "Kemaman", "Kuala Terengganu", "Marang", "Setiu",  # Terengganu
    "Brickfields", "Cheras", "Dang Wangi", "Sentul", "Wangsa Maju", "W.P. Putrajaya"  # W.P. Kuala Lumpur
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
