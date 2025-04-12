from fastapi import FastAPI
import pickle
import numpy as np
import matplotlib.pyplot as plt
import io
import base64

from firebase_admin import db
import firebase_admin
from firebase_admin import credentials

# Initialize Firebase app if not already initialized
if not firebase_admin._apps:
    cred = credentials.Certificate("firebase-credentials.json")
    firebase_admin.initialize_app(cred, {
        'databaseURL': "https://safezone-660a9-default-rtdb.asia-southeast1.firebasedatabase.app/"
    })

app = FastAPI()
model = pickle.load(open("model.pkl", "rb"))

def fetch_data_from_firebase(path: str) -> list:
    ref = db.reference(path)
    data = ref.get()
    return data if isinstance(data, list) else []

@app.get("/predict_from_firebase")
def predict_from_firebase():
    data = fetch_data_from_firebase("input_data")  # Replace with your actual path in Firebase
    if not data:
        return {"error": "No data found at Firebase path"}
    prediction = model.predict(np.array([data]))
    return {"prediction": prediction.tolist()}

@app.get("/plot_from_firebase")
def plot_from_firebase():
    data = fetch_data_from_firebase("input_data")
    if not data:
        return {"error": "No data found at Firebase path"}
    prediction = model.predict(np.array([data]))
    plt.plot(prediction)
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    return {"image": base64.b64encode(buf.read()).decode()}
