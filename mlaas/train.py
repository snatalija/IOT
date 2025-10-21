import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from joblib import dump
import os
import math
import re

CSV_PATH = os.getenv("CSV_PATH", "data/amazon_delivery.csv")
MODEL_PATH = os.getenv("MODEL_PATH", "model.pkl")
THRESHOLD_MIN = float(os.getenv("SLA_THRESHOLD_MIN", "30"))

def parse_minutes(x):
    """Prihvata broj ili string i vrati float minuta."""
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float)):
        return float(x)
    m = re.search(r"(\d+(\.\d+)?)", str(x))
    return float(m.group(1)) if m else np.nan

def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distanca u km."""
    for v in (lat1, lon1, lat2, lon2):
        if pd.isna(v):
            return np.nan
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2 * R * math.asin(math.sqrt(a))

df = pd.read_csv(CSV_PATH)

df.columns = (
    df.columns.astype(str)
    .str.strip().str.lower()
    .str.replace(r"[^a-z0-9]+", "_", regex=True)
    .str.replace(r"_+", "_", regex=True)
    .str.strip("_")
)

cols = set(df.columns)

if "area" not in cols:
    raise KeyError(f"Nedostaje kolona 'area' (dostupno: {sorted(cols)})")
df = df.rename(columns={"area": "city"})

if "traffic" not in cols or "weather" not in cols:
    raise KeyError("Očekujem kolone 'traffic' i 'weather' u CSV-u.")

if "delivery_time" not in cols:
    raise KeyError("Očekujem kolonu 'delivery_time' (vreme isporuke u minutima).")
df["timeTakenMin"] = df["delivery_time"].apply(parse_minutes)

need = {"store_latitude","store_longitude","drop_latitude","drop_longitude"}
if not need.issubset(cols):
    raise KeyError(f"Nedostaju lat/long kolone za distancu: {sorted(need - cols)}")

df["distanceKm"] = [
    haversine_km(slat, slon, dlat, dlon)
    for slat, slon, dlat, dlon in zip(
        df["store_latitude"], df["store_longitude"], df["drop_latitude"], df["drop_longitude"]
    )
]

if "order_date" in cols and "order_time" in cols:
    df["deliveryTimestamp"] = pd.to_datetime(
        df["order_date"].astype(str) + " " + df["order_time"].astype(str),
        errors="coerce", utc=True
    )


needed = ["city", "weather", "traffic", "distanceKm", "timeTakenMin"]
df = df.dropna(subset=needed).copy()

df["late"] = (df["timeTakenMin"] > THRESHOLD_MIN).astype(int)

if "deliveryTimestamp" in df.columns:
    ts = pd.to_datetime(df["deliveryTimestamp"], errors="coerce", utc=True)
    df["hour"] = ts.dt.hour
    df["weekday"] = ts.dt.weekday
else:
    df["hour"] = 12
    df["weekday"] = 3


features = ["city", "weather", "traffic", "distanceKm", "hour", "weekday"]
X = df[features]
y = df["late"]

num_cols = ["distanceKm", "hour", "weekday"]
cat_cols = ["city", "weather", "traffic"]

pre = ColumnTransformer(
    transformers=[
        ("num", StandardScaler(), num_cols),
        ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
    ]
)

clf = RandomForestClassifier(n_estimators=200, random_state=42)
pipe = Pipeline([("pre", pre), ("clf", clf)])

X_tr, X_te, y_tr, y_te = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
pipe.fit(X_tr, y_tr)
y_pr = pipe.predict(X_te)
print(classification_report(y_te, y_pr))

dump({"model": pipe, "threshold_min": THRESHOLD_MIN}, MODEL_PATH)
print(f"Model saved to {MODEL_PATH}")
