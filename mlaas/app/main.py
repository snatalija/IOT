from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import joblib
import os

app = FastAPI()

MODEL_PATH = os.getenv("MODEL_PATH", "/app/model.pkl")
_bundle = joblib.load(MODEL_PATH)
model = _bundle["model"]
threshold_min = _bundle.get("threshold_min", 30)

FEATURES = ["city", "weather", "traffic", "distanceKm", "hour", "weekday"]

class PredictIn(BaseModel):
    city: str
    weather: str
    traffic: str
    distanceKm: float
    hour: int
    weekday: int

@app.post("/predict")
def predict(payload: PredictIn):
    X = pd.DataFrame([payload.dict()])[FEATURES]

    proba = float(model.predict_proba(X)[0][1])
    late = int(model.predict(X)[0])

    return {
        "late": late,
        "proba_late": proba,
        "threshold_min": threshold_min
    }
