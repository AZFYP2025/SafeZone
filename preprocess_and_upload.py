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

malaysian_states = [
    "johor", "kedah", "kelantan", "melaka", "negeri sembilan", "pahang",
    "perak", "perlis", "pulau pinang", "sabah", "sarawak", "selangor",
    "terengganu", "w.p. kuala lumpur"
]

malaysian_districts = [
    "batu pahat", "iskandar puteri", "johor bahru", "johor bahru", "kluang",
    "kota tinggi", "kulaijaya", "ledang", "mersing", "muar", "nusajaya", "pontian", "segamat", "seri alam",  # Johor
    "baling", "bandar bharu", "kota setar", "kuala muda", "kubang pasu", "kulim",
    "langkawi", "padang terap", "pendang", "sik", "yan",  # Kedah
    "bachok", "gua musang", "jeli", "kota bharu", "kuala krai", "machang", "pasir mas",
    "pasir puteh", "tanah merah", "tumpat",  # Kelantan
    "alor gajah", "jasin", "melaka tengah",  # Melaka
    "jelebu", "jempol", "kuala pilah", "nilai", "port dickson", "rembau", "seremban", "tampin",  # Negeri Sembilan
    "bentong", "bera", "cameron highland", "jerantut", "kuala lipis", "kuantan",
    "maran", "pekan", "raub", "rompin", "temerloh",  # Pahang
    "batu gajah", "gerik", "hilir perak", "ipoh", "kampar", "kerian", "kuala kangsar",
    "manjung", "pengkalan hulu", "perak tengah", "selama", "sungai siput", "taiping",
    "tanjong malim", "tapah",  # Perak
    "arau", "kangar", "padang besar",  # Perlis
    "barat daya", "seberang perai", "timur laut",  # Pulau Pinang
    "beaufort", "beluran", "keningau", "kota belud", "kota kinabalu", "kinabatangan",
    "kota marudu", "kudat", "kunak", "lahad datu", "papar", "penampang", "ranau",
    "sandakan", "semporna", "sipitang", "tawau", "tenom", "tuaran",  # Sabah
    "bau", "belaga", "betong", "bintulu", "dalat", "julau", "kanowit", "kapit",
    "kota samarahan", "kuching", "lawas", "limbang", "lubok antu", "lundu", "marudi",
    "matu daro", "meradong", "miri", "mukah", "padawan", "saratok", "sarikei",
    "serian", "sibu", "simunjan", "song", "sri aman", "tatau",  # Sarawak
    "ampang jaya", "gombak", "hulu selangor", "kajang", "klang",
    "kuala langat", "kuala selangor", "petaling jaya", "sabak bernam", "sepang",
    "serdang", "sg. buloh", "shah alam", "subang jaya",  # Selangor
    "besut", "dungun", "hulu terengganu", "kemaman", "kuala terengganu", "marang", "setiu",  # Terengganu
    "brickfields", "cheras", "dang wangi", "sentul", "wangsa maju", "w.p. putrajaya"  # W.P. Kuala Lumpur
]

special_cases = {
    "kl": "w.p. kuala lumpur",
    "kuala lumpur": "w.p. kuala lumpur",
    "putrajaya": "w.p. putrajaya",
    "jb": "johor bahru",
    "pj": "petaling jaya",
    "sg buloh": "sg. buloh",
    "sungai buloh": "sg.buloh",
    "n9": "negeri sembilan"
}

def normalize_location(input_location):
    input_location = input_location.lower().strip()  # Convert to lowercase and remove spaces
    
    if input_location in special_cases:
        return special_cases[input_location].title()

    if input_location in malaysian_states:
        return input_location.title()  # Format properly
    
    if input_location in malaysian_districts:
        return input_location.title()  # Format properly
    
    # Capitalize first letter of each word if not found
    return input_location.title()


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
