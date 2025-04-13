import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import hashlib
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
    "bentong", "bera", "cameron highlands", "jerantut", "kuala lipis", "kuantan",
    "maran", "pekan", "raub", "rompin", "temerloh",  # Pahang
    "batu gajah", "gerik", "hilir perak", "ipoh", "kampar", "kerian", "kuala kangsar",
    "manjung", "pengkalan hulu", "perak tengah", "selama", "sungai siput", "taiping",
    "tanjong malim", "tapah",  # Perak
    "arau", "kangar", "padang besar",  # Perlis
    "barat daya","seberang perai", "timur laut",  # Pulau Pinang
    "beaufort", "beluran", "keningau", "kota belud", "kota kinabalu", "kinabatangan",
    "kota marudu", "kudat", "kunak", "lahad datu", "papar", "penampang", "ranau",
    "sandakan", "semporna", "sipitang", "tawau", "tenom", "tuaran",  # Sabah
    "bau", "belaga", "betong", "bintulu", "dalat", "julau", "kanowit", "kapit",
    "kota samarahan", "kuching", "lawas", "limbang", "lubok antu", "lundu", "marudi",
    "matu daro", "meradong", "miri", "mukah", "padawan", "saratok", "sarikei",
    "serian", "sibu", "simunjan", "song", "sri aman", "tatau",  # Sarawak
    "ampang jaya", "gombak", "hulu selangor", "kajang", "klang", "kuala langat", 
    "kuala selangor", "petaling jaya", "sabak bernam", "sepang",
    "serdang", "sg. buloh", "shah alam", "subang jaya",  # Selangor
    "besut", "dungun", "hulu terengganu", "kemaman", "kuala terengganu", "marang", "setiu",  # Terengganu
    "brickfields", "cheras", "dang wangi", "sentul", "wangsa maju", "w.p. putrajaya"  # W.P. Kuala Lumpur
]

# Dictionary for abbreviations
ABBREVIATIONS = {
    "putrajaya": "w.p. putrajaya",
    "sg buloh": "sg. buloh",
    "sungai buloh": "sg. buloh",
    "kl": "w.p. kuala lumpur",
    "kuala lumpur": "w.p. kuala lumpur",
    "n9": "negeri sembilan",
    "tg malim": "tanjong malim",
    "tanjung malim": "tanjong malim",
    "cameron highland": "cameron highlands"
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
                "curi": "theft", "pencuri": "theft", "pencurian": "theft",
                "rogol": "rape", "perogol": "rape", "merogol": "rape",
                "rompak": "robbery", "merompak": "robbery", "rompakan": "robbery",
                "bunuh": "murder", "membunuh": "murder", "pembunuhan": "murder", "terbunuh": "murder"
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

# Extract State and District from Text
def extract_location(text):
    try:
        #Normalize the text by replacing abbreviations
        text_lower = text.lower()
        for abbrev, full_form in ABBREVIATIONS.items():
            text_lower = text_lower.replace(abbrev, full_form)

        # Use regex to detect common location phrases (e.g., "di shah alam")
        match = re.search(r"(di|kat|di dalam|di kawasan)\s+([\w\.\s]+)", text_lower)
        if match:
            possible_location = match.group(2).strip()
            for loc in MALAYSIAN_LOCATIONS:
                if loc.lower() in possible_location.lower():
                    # Check if the location is a district
                    if loc.lower() in DISTRICT_TO_STATE:
                        state = DISTRICT_TO_STATE[loc.lower()]
                        return state, loc  # Return state and district
                    else:
                        return loc, "Unknown"  # Return location as state, district as Unknown
                        
        #If regex fails, return "Unknown" for both state and district
        logging.warning(f"No location found in text: {text}")
        return "Unknown", "Unknown"  # Default case

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
def process_and_upload():
    try:
        logging.info("Starting data processing and upload...")
        
        # Fetch data from Google Sheets
        df = fetch_google_sheets()
        if df.empty:
            logging.info("No data to process.")
            return

        # Get already processed IDs from Firebase
        processed_ref = db.reference("processed_ids")
        processed_ids = processed_ref.get() or {}

        # Filter new rows
        new_rows = []
        for _, row in df.iterrows():
            row_id = generate_row_id(row)
            if row_id and row_id not in processed_ids:
                new_rows.append(row)

        if not new_rows:
            logging.info("No new data to process.")
            return
            
        # Process new rows
        new_df = pd.DataFrame(new_rows)
        test_location = extract_location(new_df["Tweet Text"].iloc[0])
        print(f"Extracted Location Example: {test_location}")  # Should be a tuple (State, District)
        new_df["Cleaned Text"] = new_df["Tweet Text"].apply(preprocess_text)
        new_df[["Category", "Type"]] = new_df["Main Topic"].apply(lambda x: pd.Series(map_malay_to_type_and_category(x)))
        new_df[["State", "District"]] = new_df["Tweet Text"].apply(lambda x: extract_location(x)).apply(pd.Series)

        # Log the processed DataFrame
        logging.info(f"Processed DataFrame columns: {new_df.columns.tolist()}")
        logging.info(f"Processed DataFrame first row: {new_df.iloc[0].to_dict()}")

        # Upload data to Firebase
        crime_ref = db.reference("crime_data")
        batch = {}
        
        for _, row in new_df.iterrows():
            row_id = generate_row_id(row)
            if row_id:
                # Convert date to string
                date_str = row["Date (GMT)"].isoformat()  # Convert date to ISO format string
                crime_data = {
                    "state": row["State"],
                    "district": row["District"],
                    "category": row["Category"],  # "Assault" or "Property"
                    "type": row["Type"],  # Malay term (e.g., "pencuri", "rogol")
                    "date": date_str  # Use the string representation of the date
                }
                batch[row_id] = crime_data
                processed_ids[row_id] = True  # Mark as processed

        # Atomic update to Firebase
        crime_ref.update(batch)
        processed_ref.update(processed_ids)
        logging.info(f"Added {len(new_df)} new records to Firebase!")
    except Exception as e:
        logging.error(f"Error in process_and_upload: {e}")
        
# Main execution
if __name__ == "__main__":
    try:
        initialize_firebase()
        process_and_upload()
    except Exception as e:
        logging.error(f"Script failed: {e}")
