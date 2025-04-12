from fastapi import FastAPI
import pickle
import numpy as np
import matplotlib.pyplot as plt
import io
import base64

app = FastAPI()
model = pickle.load(open("model.pkl", "rb"))

@app.post("/predict")
def predict(data: list):
    prediction = model.predict(np.array([data]))
    return {"prediction": prediction.tolist()}

@app.post("/plot")
def plot(data: list):
    prediction = model.predict(np.array([data]))
    plt.plot(prediction)
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    return {"image": base64.b64encode(buf.read()).decode()}
