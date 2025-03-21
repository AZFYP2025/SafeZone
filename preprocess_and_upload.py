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

            # Malay Crime Terms Mapping
            crime_mapping = {
                "curi": "stealing", "pencuri": "stealing", "pencurian": "stealing",
                "rogol": "rape", "perogol": "rape", "merogol": "rape",
                "rompak": "robbery", "merompak": "robbery", "rompakan": "robbery"
            }
            df["Main Topic"] = df["Main Topic"].replace(crime_mapping)

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
        doc = nlp(text)
        locations = [ent.text for ent in doc.ents if ent.type == "GPE"]
        if len(locations) >= 2:
            return locations[0], locations[1]  # State & District
        elif len(locations) == 1:
            return locations[0], "Unknown"  # State, No District
        else:
            return "Unknown", "Unknown"  # Default case
    except Exception as e:
        logging.error(f"Error extracting location: {e}")
        return "Unknown", "Unknown"

# Map Malay crime terms to Type and Category
def map_malay_to_type_and_category(topic):
    topic = topic.lower()  # Ensure case insensitivity
    if topic in ["pencuri", "pencurian", "curi", "rompak", "merompak", "rompakan"]:
        return "Property", topic  # Type is the Malay term, Category is "Property"
    elif topic in ["rogol", "perogol", "merogol"]:
        return "Assault", topic  # Type is the Malay term, Category is "Assault"
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
            logging.info("✅ No new data to process.")
            return
            
        # Process new rows
        new_df = pd.DataFrame(new_rows)
        test_location = extract_location(new_df["Tweet Text"].iloc[0], nlp)
        print(f"Extracted Location Example: {test_location}")  # Should be a tuple (State, District)
        new_df["Cleaned Text"] = new_df["Tweet Text"].apply(preprocess_text)
        new_df[["Category", "Type"]] = new_df["Main Topic"].apply(lambda x: pd.Series(map_malay_to_type_and_category(x)))
        new_df[["State", "District"]] = new_df["Tweet Text"].apply(lambda x: extract_location(x, nlp)).apply(pd.Series)

        # Upload data to Firebase
        crime_ref = db.reference("crime_data")
        batch = {}
        
        for _, row in new_df.iterrows():
            row_id = generate_row_id(row)
            if row_id:
                crime_data = {
                    "state": row["State"],
                    "district": row["District"],
                    "category": row["Category"],  # "Assault" or "Property"
                    "type": row["Type"],  # Malay term (e.g., "pencuri", "rogol")
                    "date": row["Date (GMT)"]
                }
                batch[row_id] = crime_data
                processed_ids[row_id] = True  # Mark as processed

        # Atomic update to Firebase
        crime_ref.update(batch)
        processed_ref.update(processed_ids)
        logging.info(f"✅ Added {len(new_df)} new records to Firebase!")
    except Exception as e:
        logging.error(f"Error in process_and_upload: {e}")

# Main execution
if __name__ == "__main__":
    try:
        initialize_firebase()
        nlp = initialize_nlp()
        process_and_upload()
    except Exception as e:
        logging.error(f"Script failed: {e}")
