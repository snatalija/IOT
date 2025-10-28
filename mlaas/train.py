import os
import math
import pandas as pd
import numpy as np
from typing import Optional, Tuple, List, Dict

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from joblib import dump


CSV_PATH = os.getenv("CSV_PATH", "data/amazon_delivery.csv")
MODEL_PATH = os.getenv("MODEL_PATH", "model.pkl")
SLA_THRESHOLD_MIN = float(os.getenv("SLA_THRESHOLD_MIN", "30"))


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Vrati kopiju df sa lower-case imenima kolona bez razmaka/underscore normalizovanih."""
    df = df.copy()
    new_cols = {}
    for c in df.columns:
        k = c.strip().replace(" ", "_").lower()
        new_cols[c] = k
    df.rename(columns=new_cols, inplace=True)
    return df


def pick(cols: set, *candidates: str) -> Optional[str]:
    """Pronadji prvu postojeću kolonu iz liste kandidata (kandidati su već lower-case)."""
    for c in candidates:
        if c in cols:
            return c
    return None


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Izracunaj rastojanje u km Haversine formulom."""
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return float(R * c)


def build_timestamp(df: pd.DataFrame, cols: set) -> pd.Series:
    """
    Vrati pd.Series (datetime64[ns, UTC]) koji predstavlja timestamp događaja.
    Pokušava redom:
      1) direktno: 'delivery_time' ili 'timestamp'
      2) kombinacija datuma i vremena: ('order_date' + 'order_time') ili ('pickup_date' + 'pickup_time')
      3) fallback: sinteticki niz (1h korak)
    """
    ts_col = pick(cols, "timestamp", "delivery_timestamp", "delivered_at")
    if ts_col:
        ts = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
        if ts.notna().any():
            return ts

    date_col = pick(cols, "order_date", "pickup_date")
    time_col = pick(cols, "order_time", "pickup_time")
    if date_col and time_col:
        ts = pd.to_datetime(df[date_col].astype(str) + " " + df[time_col].astype(str),
                            errors="coerce", utc=True)
        if ts.notna().any():
            return ts

    if date_col and not time_col:
        ts = pd.to_datetime(df[date_col].astype(str) + " 12:00:00", errors="coerce", utc=True)
        if ts.notna().any():
            return ts

    return pd.date_range("2025-01-01", periods=len(df), freq="h", tz="UTC")


def compute_distance(df: pd.DataFrame, cols: set) -> pd.Series:
    """
    Vrati Series distanceKm.
    Pokušava:
      - postojecu 'distancekm', 'distance_km', 'distance'
      - ako nema, računa iz store_latitude/store_longitude i drop_latitude/drop_longitude
    """
    dist_col = pick(cols, "distancekm", "distance_km", "distance")
    if dist_col:
        return pd.to_numeric(df[dist_col], errors="coerce")

    store_lat = pick(cols, "store_latitude", "store_lat", "pickup_latitude")
    store_lon = pick(cols, "store_longitude", "store_lon", "pickup_longitude")
    drop_lat = pick(cols, "drop_latitude", "dest_latitude", "delivery_latitude")
    drop_lon = pick(cols, "drop_longitude", "dest_longitude", "delivery_longitude")

    if store_lat and store_lon and drop_lat and drop_lon:
        return df.apply(
            lambda r: haversine_km(r[store_lat], r[store_lon], r[drop_lat], r[drop_lon]),
            axis=1
        )

    return pd.Series(np.nan, index=df.index)

print(f"Loading CSV: {CSV_PATH}")
df_raw = pd.read_csv(CSV_PATH)
df = normalize_columns(df_raw)

cols = set(df.columns)

area_col     = pick(cols, "area", "city")
weather_col  = pick(cols, "weather")
traffic_col  = pick(cols, "traffic")
veh_col      = pick(cols, "vehicle")
delivery_min = pick(cols, "time_taken_min", "delivery_time", "time_min", "duration_min")

missing_crit = []
if not delivery_min:
    missing_crit.append("delivery_time (npr. 'Delivery_Time' ili 'time_taken_min')")
if not weather_col:
    missing_crit.append("weather")
if not traffic_col:
    missing_crit.append("traffic")
if missing_crit:
    raise KeyError(
        "Nedostaju ključne kolone: " + ", ".join(missing_crit) +
        f". Dostupne kolone: {sorted(cols)}"
    )

df["delivery_minutes"] = pd.to_numeric(df[delivery_min], errors="coerce")
df = df.dropna(subset=[ "delivery_minutes", weather_col, traffic_col ])
df["late"] = (df["delivery_minutes"] > SLA_THRESHOLD_MIN).astype(int)

df["distancekm"] = compute_distance(df, set(df.columns))

ts = build_timestamp(df, set(df.columns))
df["hour"] = ts.dt.hour
df["weekday"] = ts.dt.weekday

if not area_col:
    df["area"] = "Unknown"
    area_col = "area"

cat_cols = [area_col, weather_col, traffic_col]
num_cols = ["distancekm", "hour", "weekday"]

for nc in num_cols:
    df[nc] = pd.to_numeric(df[nc], errors="coerce")

keep = cat_cols + num_cols + ["late"]
df = df.dropna(subset=num_cols + [weather_col, traffic_col])

X = df[cat_cols + num_cols]
y = df["late"]

stratify_arg = y if y.nunique() == 2 else None

X_tr, X_val, y_tr, y_val = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=stratify_arg
)
pre = ColumnTransformer(
    transformers=[
        ("num", StandardScaler(), num_cols),
        ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
    ]
)

clf = RandomForestClassifier(
    n_estimators=200,
    random_state=42,
    n_jobs=-1,
    class_weight=None
)

pipe = Pipeline([("pre", pre), ("clf", clf)])

pipe.fit(X_tr, y_tr)

y_val_pred = pipe.predict(X_val)

report = classification_report(y_val, y_val_pred, digits=3)
print(report)

meta: Dict[str, object] = {
    "model": pipe,
    "threshold_min": SLA_THRESHOLD_MIN,
    "feature_names_in": cat_cols + num_cols,
    "class_labels": list(pipe.named_steps["clf"].classes_),
}

dump(meta, MODEL_PATH)
print(f"Model saved to {MODEL_PATH}")
