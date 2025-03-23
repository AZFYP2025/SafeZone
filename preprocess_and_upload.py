import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import hashlib
import stanza  # NLP for Malay
from googleapiclient.discovery import build
from google.oauth2 import service_account
import logging
import re
from googleapiclient.errors import HttpError  # Import HttpError
import time  # Import time for retry delay

# Malaysian states, districts, and special cases
MALAYSIAN_STATES = [
    "johor", "kedah", "kelantan", "melaka", "negeri sembilan", "pahang",
    "perak", "perlis", "pulau pinang", "sabah", "sarawak", "selangor",
    "terengganu", "w.p. kuala lumpur"
]

MALAYSIAN_DISTRICTS = [
    "batu pahat", "iskandar puteri", "johor bahru selatan", "johor bahru utara", "kluang",
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
    "barat daya", "seberang perai selatan", "seberang perai tengah", "seberang perai utara", "timur laut",  # Pulau Pinang
    "beaufort", "beluran", "keningau", "kota belud", "kota kinabalu", "kinabatangan",
    "kota marudu", "kudat", "kunak", "lahad datu", "papar", "penampang", "ranau",
    "sandakan", "semporna", "sipitang", "tawau", "tenom", "tuaran",  # Sabah
    "bau", "belaga", "betong", "bintulu", "dalat", "julau", "kanowit", "kapit",
    "kota samarahan", "kuching", "lawas", "limbang", "lubok antu", "lundu", "marudi",
    "matu daro", "meradong", "miri", "mukah", "padawan", "saratok", "sarikei",
    "serian", "sibu", "simunjan", "song", "sri aman", "tatau",  # Sarawak
    "ampang jaya", "gombak", "hulu selangor", "kajang", "klang selatan", "klang utara",
    "kuala langat", "kuala selangor", "petaling jaya", "sabak bernam", "sepang",
    "serdang", "sg. buloh", "shah alam", "subang jaya",  # Selangor
    "besut", "dungun", "hulu terengganu", "kemaman", "kuala terengganu", "marang", "setiu",  # Terengganu
    "brickfields", "cheras", "dang wangi", "sentul", "wangsa maju", "w.p. putrajaya"  # W.P. Kuala Lumpur
]

# Define normalization mappings
NORMALIZATION_MAPPINGS = {
    "putrajaya": "w.p. putrajaya",
    "sg buloh": "sg. buloh",
    "sungai buloh": "sg. buloh",
    "kl": "w.p. kuala lumpur",
    "kuala lumpur": "w.p. kuala lumpur"
}

DISTRICT_TO_STATE = {
    # Johor
    "batu pahat": "johor",
    "iskandar puteri": "johor",
    "johor bahru selatan": "johor",
    "johor bahru utara": "johor",
    "kluang": "johor",
    "kota tinggi": "johor",
    "kulaijaya": "johor",
    "ledang": "johor",
    "mersing": "johor",
    "muar": "johor",
    "nusajaya": "johor",
    "pontian": "johor",
    "segamat": "johor",
    "seri alam": "johor",

    # Kedah
    "baling": "kedah",
    "bandar bharu": "kedah",
    "kota setar": "kedah",
    "kuala muda": "kedah",
    "kubang pasu": "kedah",
    "kulim": "kedah",
    "langkawi": "kedah",
    "padang terap": "kedah",
    "pendang": "kedah",
    "sik": "kedah",
    "yan": "kedah",

    # Kelantan
    "bachok": "kelantan",
    "gua musang": "kelantan",
    "jeli": "kelantan",
    "kota bharu": "kelantan",
    "kuala krai": "kelantan",
    "machang": "kelantan",
    "pasir mas": "kelantan",
    "pasir puteh": "kelantan",
    "tanah merah": "kelantan",
    "tumpat": "kelantan",

    # Melaka
    "alor gajah": "melaka",
    "jasin": "melaka",
    "melaka tengah": "melaka",

    # Negeri Sembilan
    "jelebu": "negeri sembilan",
    "jempol": "negeri sembilan",
    "kuala pilah": "negeri sembilan",
    "nilai": "negeri sembilan",
    "port dickson": "negeri sembilan",
    "rembau": "negeri sembilan",
    "seremban": "negeri sembilan",
    "tampin": "negeri sembilan",

    # Pahang
    "bentong": "pahang",
    "bera": "pahang",
    "cameron highland": "pahang",
    "jerantut": "pahang",
    "kuala lipis": "pahang",
    "kuantan": "pahang",
    "maran": "pahang",
    "pekan": "pahang",
    "raub": "pahang",
    "rompin": "pahang",
    "temerloh": "pahang",

    # Perak
    "batu gajah": "perak",
    "gerik": "perak",
    "hilir perak": "perak",
    "ipoh": "perak",
    "kampar": "perak",
    "kerian": "perak",
    "kuala kangsar": "perak",
    "manjung": "perak",
    "pengkalan hulu": "perak",
    "perak tengah": "perak",
    "selama": "perak",
    "sungai siput": "perak",
    "taiping": "perak",
    "tanjong malim": "perak",
    "tapah": "perak",

    # Perlis
    "arau": "perlis",
    "kangar": "perlis",
    "padang besar": "perlis",

    # Pulau Pinang
    "barat daya": "pulau pinang",
    "seberang perai selatan": "pulau pinang",
    "seberang perai tengah": "pulau pinang",
    "seberang perai utara": "pulau pinang",
    "timur laut": "pulau pinang",

    # Sabah
    "beaufort": "sabah",
    "beluran": "sabah",
    "keningau": "sabah",
    "kota belud": "sabah",
    "kota kinabalu": "sabah",
    "kinabatangan": "sabah",
    "kota marudu": "sabah",
    "kudat": "sabah",
    "kunak": "sabah",
    "lahad datu": "sabah",
    "papar": "sabah",
    "penampang": "sabah",
    "ranau": "sabah",
    "sandakan": "sabah",
    "semporna": "sabah",
    "sipitang": "sabah",
    "tawau": "sabah",
    "tenom": "sabah",
    "tuaran": "sabah",

    # Sarawak
    "bau": "sarawak",
    "belaga": "sarawak",
    "betong": "sarawak",
    "bintulu": "sarawak",
    "dalat": "sarawak",
    "julau": "sarawak",
    "kanowit": "sarawak",
    "kapit": "sarawak",
    "kota samarahan": "sarawak",
    "kuching": "sarawak",
    "lawas": "sarawak",
    "limbang": "sarawak",
    "lubok antu": "sarawak",
    "lundu": "sarawak",
    "marudi": "sarawak",
    "matu daro": "sarawak",
    "meradong": "sarawak",
    "miri": "sarawak",
    "mukah": "sarawak",
    "padawan": "sarawak",
    "saratok": "sarawak",
    "sarikei": "sarawak",
    "serian": "sarawak",
    "sibu": "sarawak",
    "simunjan": "sarawak",
    "song": "sarawak",
    "sri aman": "sarawak",
    "tatau": "sarawak",

    # Selangor
    "ampang jaya": "selangor",
    "gombak": "selangor",
    "hulu selangor": "selangor",
    "kajang": "selangor",
    "klang selatan": "selangor",
    "klang utara": "selangor",
    "kuala langat": "selangor",
    "kuala selangor": "selangor",
    "petaling jaya": "selangor",
    "sabak bernam": "selangor",
    "sepang": "selangor",
    "serdang": "selangor",
    "sg. buloh": "selangor",
    "shah alam": "selangor",
    "subang jaya": "selangor",

    # Terengganu
    "besut": "terengganu",
    "dungun": "terengganu",
    "hulu terengganu": "terengganu",
    "kemaman": "terengganu",
    "kuala terengganu": "terengganu",
    "marang": "terengganu",
    "setiu": "terengganu",

    # W.P. Kuala Lumpur
    "brickfields": "w.p. kuala lumpur",
    "cheras": "w.p. kuala lumpur",
    "dang wangi": "w.p. kuala lumpur",
    "sentul": "w.p. kuala lumpur",
    "wangsa maju": "w.p. kuala lumpur",
    "w.p. putrajaya": "w.p. kuala lumpur"
}

# Combine all locations into a single list for easier lookup
MALAYSIAN_LOCATIONS = MALAYSIAN_STATES + MALAYSIAN_DISTRICTS

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Firebase
def initialize_firebase():
    try:
        cred = credentials.Certificate("firebase-credentials.json")
        firebase_admin.initialize_app(cred, {"databaseURL": "https://safezone-660a9-default-rtdb.asia-southeast1.firebasedatabase.app/"})
        logging.info("Firebase initialized successfully.")
    except Exception as e:
        logging.error(f"Error initializing Firebase: {e}")
        raise

# Google Sheets API Setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SHEET_ID = "1CNo8eLCASEfd7ktOgiUrzT8KBkAWhW5sPON1BITBKvM"
RANGE_NAME = "SafeZone!A:H"

# Initialize NLP tools
def initialize_nlp():
    try:
        stanza.download("id")  # Use 'id' for Malay
        nlp = stanza.Pipeline("id")
        logging.info("NLP pipeline initialized successfully.")
        return nlp
    except Exception as e:
        logging.error(f"Error initializing NLP pipeline: {e}")
        raise

# Fetch data from Google Sheets
def fetch_google_sheets():
    creds = service_account.Credentials.from_service_account_file("google-credentials.json", scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    retries = 3  # Number of retry attempts
    for attempt in range(retries):
        try:
            result = sheet.values().get(spreadsheetId=SHEET_ID, range=RANGE_NAME).execute()
            values = result.get("values", [])

            if not values:
                logging.warning("No data found in Google Sheets.")
                return pd.DataFrame()

            # Convert to DataFrame and select only the required columns
            df = pd.DataFrame(values[1:], columns=values[0])
            df = df[["Date (GMT)", "Main Topic", "Tweet Text"]]  # Select only the required columns

            # Modify Date Format (Remove Time)
            df["Date (GMT)"] = pd.to_datetime(df["Date (GMT)"]).dt.date

            # Log unique values in the "Main Topic" column before mapping
            logging.info(f"Unique 'Main Topic' values before mapping: {df['Main Topic'].unique()}")

            # Lowercase the "Main Topic" column before mapping
            df["Main Topic"] = df["Main Topic"].str.lower()

            # Malay Crime Terms Mapping
            crime_mapping = {
                "curi": "stealing", "pencuri": "stealing", "pencurian": "stealing",
                "rogol": "rape", "perogol": "rape", "merogol": "rape",
                "rompak": "robbery", "merompak": "robbery", "rompakan": "robbery"
            }
            df["Main Topic"] = df["Main Topic"].replace(crime_mapping)

            # Log unique values in the "Main Topic" column after mapping
            logging.info(f"Unique 'Main Topic' values after mapping: {df['Main Topic'].unique()}")

            logging.info(f"Fetched {len(df)} rows from Google Sheets.")
            logging.info(f"Columns in DataFrame: {df.columns.tolist()}")  # Log column names
            logging.info(f"First row of data: {df.iloc[0].to_dict()}")  # Log first row of data
            return df

        except HttpError as e:
            logging.error(f"Error fetching data from Google Sheets (attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(5)  # Wait before retrying
            else:
                logging.error("All retry attempts failed. Returning empty DataFrame.")
                return pd.DataFrame()  # Return empty DataFrame if all retries fail
        except Exception as e:
            logging.error(f"Unexpected error fetching data from Google Sheets (attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(5)  # Wait before retrying
            else:
                logging.error("All retry attempts failed. Returning empty DataFrame.")
                return pd.DataFrame()  # Return empty DataFrame if all retries fail
                
# Preprocess text (clean and normalize)
def preprocess_text(text):
    try:
        # Remove special characters, URLs, and extra spaces
        text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)  # Remove URLs
        text = re.sub(r"\W", " ", text)  # Remove special characters
        text = re.sub(r"\s+", " ", text).strip()  # Remove extra spaces
        return text
    except Exception as e:
        logging.error(f"Error preprocessing text: {e}")
        return text

# Generate Unique Row ID
def generate_row_id(row):
    try:
        unique_string = f"{row['Date (GMT)']}-{row['Tweet Text']}".encode('utf-8')
        return hashlib.md5(unique_string).hexdigest()
    except Exception as e:
        logging.error(f"Error generating row ID: {e}")
        return None

# Extract State and District from Text using NLP
def extract_location(text, nlp):
    try:
        text_lower = text.lower().strip()

        # 1️⃣ Handle Special Cases for State
        if text_lower in SPECIAL_STATE_CASES:
            return SPECIAL_STATE_CASES[text_lower], "Unknown"  # State is corrected, district unknown

        # 2️⃣ Handle Special Cases for District
        if text_lower in SPECIAL_DISTRICT_CASES:
            district = SPECIAL_DISTRICT_CASES[text_lower]
            # Ensure the correct state is assigned
            if district.lower() in DISTRICT_TO_STATE:
                return DISTRICT_TO_STATE[district.lower()], district
            return "Unknown", district  # District found, but state unknown

        # 3️⃣ Use regex to detect location mentions
        match = re.search(r"(di|kat|di dalam|di kawasan)\s+([\w\s]+)", text_lower)
        if match:
            possible_location = match.group(2).strip()
            for loc in MALAYSIAN_LOCATIONS:
                if loc.lower() in possible_location.lower():
                    if loc.lower() in DISTRICT_TO_STATE:
                        state = DISTRICT_TO_STATE[loc.lower()]
                        return state, loc  # Return correct state & district
                    else:
                        return loc, "Unknown"  # If not a district, treat as state

        # 4️⃣ Use Stanza NLP for Named Entity Recognition (NER)
        doc = nlp(text)
        locations = [ent.text for ent in doc.ents if ent.type == "GPE"]

        if len(locations) >= 2:
            return locations[0], locations[1]  # Return state & district
        elif len(locations) == 1:
            if locations[0].lower() in DISTRICT_TO_STATE:
                state = DISTRICT_TO_STATE[locations[0].lower()]
                return state, locations[0]  # Return correct state & district
            else:
                return locations[0], "Unknown"

        logging.warning(f"No location found in text: {text}")
        return "Unknown", "Unknown"

    except Exception as e:
        logging.error(f"Error extracting location: {e}")
        return "Unknown", "Unknown"

        
# Map Malay crime terms to Type and Category
def map_malay_to_type_and_category(topic):
    topic = topic.lower().strip()  # Ensure case insensitivity and remove extra spaces
    if topic in ["stealing"]:
        return "property", "theft"
    elif topic in ["rape"]:
        return "assault", "rape"
    elif topic in ["robbery"]:
        return "property", "robbery"
    else:
        return "Other", "Unknown"  # Default for unknown terms

# Process and Upload Data
def normalize_text(text):
    """
    Normalize the text by replacing special cases with their standardized forms.
    """
    text_lower = text.lower()
    for key, value in NORMALIZATION_MAPPINGS.items():
        if key in text_lower:
            text_lower = text_lower.replace(key, value)
    return text_lower

def extract_location(text, nlp):
    try:
        # Normalize the text first
        normalized_text = normalize_text(text)

        # 1️⃣ Check if the normalized text contains a special case
        for key, value in NORMALIZATION_MAPPINGS.items():
            if value in normalized_text:
                # If the normalized text matches a special case, use DISTRICT_TO_STATE to find the state
                state = DISTRICT_TO_STATE.get(value, "Unknown")
                return state, value  # Return state and district

        # 2️⃣ Use regex to detect location mentions
        match = re.search(r"(di|kat|di dalam|di kawasan)\s+([\w\s]+)", normalized_text)
        if match:
            possible_location = match.group(2).strip()
            # Check if the location is in DISTRICT_TO_STATE
            for loc in DISTRICT_TO_STATE:
                if loc.lower() in possible_location.lower():
                    state = DISTRICT_TO_STATE[loc.lower()]
                    return state, loc  # Return correct state & district

        # 3️⃣ Use NLP for Named Entity Recognition (NER)
        doc = nlp(text)
        locations = [ent.text for ent in doc.ents if ent.type == "GPE"]

        if len(locations) >= 2:
            return locations[0], locations[1]  # Return state & district
        elif len(locations) == 1:
            # Check if the location is in DISTRICT_TO_STATE
            location_lower = locations[0].lower()
            if location_lower in DISTRICT_TO_STATE:
                state = DISTRICT_TO_STATE[location_lower]
                return state, locations[0]  # Return correct state & district
            else:
                return locations[0], "Unknown"  # Treat as state, district unknown

        logging.warning(f"No location found in text: {text}")
        return "Unknown", "Unknown"

    except Exception as e:
        logging.error(f"Error extracting location: {e}")
        return "Unknown", "Unknown"
        
# Main execution
if __name__ == "__main__":
    try:
        initialize_firebase()
        nlp = initialize_nlp()
        process_and_upload()
    except Exception as e:
        logging.error(f"Script failed: {e}")
