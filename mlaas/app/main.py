import os
import io
import json
from typing import Optional, Dict, Any, List

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from joblib import load, dump

from prometheus_client import (
    Counter, Histogram, Gauge, CollectorRegistry,
    generate_latest, CONTENT_TYPE_LATEST
)
from fastapi.responses import Response

MODEL_PATH = os.getenv("MODEL_PATH", "/app/model.pkl")
CSV_PATH = os.getenv("CSV_PATH", "/app/data/amazon_delivery.csv")
SLA_THRESHOLD_MIN = float(os.getenv("SLA_THRESHOLD_MIN", "30"))
LATE_DECISION_THRESHOLD = float(os.getenv("LATE_DECISION_THRESHOLD", "0.5"))

app = FastAPI(title="MLaaS - Delivery Delay Risk", version="1.0.0")

registry = CollectorRegistry()
REQ_COUNTER = Counter("mlaas_requests_total", "Broj poziva endpointa", ["endpoint"], registry=registry)
PRED_LATENCY = Histogram("mlaas_predict_latency_seconds", "Latencija predict() poziva", registry=registry)
MODEL_LOADED = Gauge("mlaas_model_loaded", "Da li je model učitan (1/0)", registry=registry)

MODEL: Optional[Dict[str, Any]] = None


def load_model() -> Optional[Dict[str, Any]]:
    global MODEL
    try:
        MODEL = load(MODEL_PATH)
        MODEL_LOADED.set(1)
    except Exception:
        MODEL = None
        MODEL_LOADED.set(0)
    return MODEL


def ensure_df_with_features(payload: Dict[str, Any], expected_order: List[str]) -> pd.DataFrame:
    """
    Iz zahteva pravi DataFrame sa kolonama tačno onim redosledom koje je model
    imao na treningu (case-insensitive mapiranje).
    """
    data = payload.copy()
    if "area" not in data and "city" in data:
        data["area"] = data.pop("city")

    lower_map = {k.lower(): v for k, v in data.items()}

    row = []
    for col in expected_order:
        v = None
        if col.lower() in lower_map:
            v = lower_map[col.lower()]
        else:
            if col.lower() in ("hour", "weekday"):
                v = 12 if col.lower() == "hour" else 3
            else:
                v = 0
        row.append(v)

    X_df = pd.DataFrame([row], columns=[c.lower() for c in expected_order])
    return X_df


class PredictIn(BaseModel):
    city: Optional[str] = Field(default=None, description="Grad ili oblast (alias za 'area')")
    area: Optional[str] = Field(default=None, description="Grad/oblast; ima prioritet nad 'city' ako postoje oba")
    weather: str
    traffic: str
    distanceKm: float
    hour: int
    weekday: int


@app.on_event("startup")
def _startup():
    load_model()


@app.get("/health")
def health():
    REQ_COUNTER.labels(endpoint="/health").inc()
    return {"status": "ok", "has_model": MODEL is not None}


@app.post("/predict")
def predict(req: PredictIn):
    REQ_COUNTER.labels(endpoint="/predict").inc()

    if MODEL is None:
        raise HTTPException(status_code=503, detail="Model nije učitan")

    model = MODEL["model"]
    feat_names = MODEL.get("feature_names_in")
    threshold_min = MODEL.get("threshold_min", SLA_THRESHOLD_MIN)

    if feat_names is None:
        raise HTTPException(status_code=500, detail="Model nema meta informaciju 'feature_names_in'")

    area_value = req.area if req.area is not None else req.city

    payload = {
        "area": area_value,
        "weather": req.weather,
        "traffic": req.traffic,
        "distanceKm": req.distanceKm,
        "hour": req.hour,
        "weekday": req.weekday,
    }

    with PRED_LATENCY.time():
        X_df = ensure_df_with_features(payload, feat_names)
        proba = float(model.predict_proba(X_df)[0][1])
        late = int(proba >= LATE_DECISION_THRESHOLD)

    return {
        "late": late,
        "proba_late": round(proba, 3),
        "threshold_min": float(threshold_min),
    }


@app.get("/metrics")
def metrics():
    REQ_COUNTER.labels(endpoint="/metrics").inc()
    data = generate_latest(registry)
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@app.post("/train")
def train():
    """
    Ponovno treniranje unutar servisa (učitava CSV_PATH, trenira i snima model na MODEL_PATH),
    sa istom logikom kao train.py – ali lokalno ovde radi bez forkovanja procesa.
    """
    REQ_COUNTER.labels(endpoint="/train").inc()

    try:
        from sklearn.model_selection import train_test_split
        from sklearn.compose import ColumnTransformer
        from sklearn.preprocessing import OneHotEncoder, StandardScaler
        from sklearn.pipeline import Pipeline
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import classification_report

        def pick(cols, *cands):
            norm = {c.lower().replace("_", "").replace(" ", ""): c for c in cols}
            for cand in cands:
                k = cand.lower().replace("_", "").replace(" ", "")
                if k in norm:
                    return norm[k]
            raise KeyError(f"Missing column. Tried: {cands}, has: {sorted(cols)}")

        def haversine_km(lat1, lon1, lat2, lon2):
            R = 6371.0
            p = math.pi / 180.0
            dlat = (lat2 - lat1) * p
            dlon = (lon2 - lon1) * p
            a = (np.sin(dlat / 2) ** 2 +
                 np.cos(lat1 * p) * np.cos(lat2 * p) * np.sin(dlon / 2) ** 2)
            c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
            return R * c

        import math

        df_raw = pd.read_csv(CSV_PATH)
        cols = list(df_raw.columns)

        area_col    = pick(cols, "area", "city", "region")
        weather_col = pick(cols, "weather")
        traffic_col = pick(cols, "traffic")
        vehicle_col = None
        try:
            vehicle_col = pick(cols, "vehicle", "vehicle_type")
        except KeyError:
            pass
        cat_col = None
        try:
            cat_col = pick(cols, "category")
        except KeyError:
            pass
        agent_age_col = None
        agent_rating_col = None
        try:
            agent_age_col = pick(cols, "agent_age")
        except KeyError:
            pass
        try:
            agent_rating_col = pick(cols, "agent_rating")
        except KeyError:
            pass

        store_lat_col = store_lon_col = drop_lat_col = drop_lon_col = None
        try:
            store_lat_col = pick(cols, "store_latitude", "store_lat")
            store_lon_col = pick(cols, "store_longitude", "store_lon")
            drop_lat_col  = pick(cols, "drop_latitude", "drop_lat")
            drop_lon_col  = pick(cols, "drop_longitude", "drop_lon")
        except KeyError:
            pass

        order_date_col = order_time_col = None
        try:
            order_date_col = pick(cols, "order_date", "date")
            order_time_col = pick(cols, "order_time", "time")
        except KeyError:
            pass

        delivery_time_col = pick(cols, "delivery_time", "timetakenmin", "time_taken_min")

        df = df_raw.copy()

        if all([store_lat_col, store_lon_col, drop_lat_col, drop_lon_col]):
            df["distanceKm"] = haversine_km(
                df[store_lat_col].astype(float),
                df[store_lon_col].astype(float),
                df[drop_lat_col].astype(float),
                df[drop_lon_col].astype(float),
            )
        else:
            df["distanceKm"] = 0.0

        if order_date_col and order_time_col:
            ts = pd.to_datetime(df[order_date_col] + " " + df[order_time_col], errors="coerce", utc=True)
        else:
            ts = pd.date_range("2025-01-01", periods=len(df), freq="h", tz="UTC")

        df["hour"] = ts.hour
        df["weekday"] = ts.weekday
        df["late"] = (df[delivery_time_col].astype(float) > SLA_THRESHOLD_MIN).astype(int)

        cat_cols = [area_col, weather_col, traffic_col]
        if vehicle_col:
            cat_cols.append(vehicle_col)
        if cat_col:
            cat_cols.append(cat_col)

        num_cols = ["distanceKm", "hour", "weekday"]
        if agent_age_col:
            num_cols.append(agent_age_col)
        if agent_rating_col:
            num_cols.append(agent_rating_col)

        X = df[cat_cols + num_cols].copy()
        X.columns = [c.lower() for c in X.columns]
        y = df["late"]

        pre = ColumnTransformer(
            transformers=[
                ("num", StandardScaler(), [c.lower() for c in num_cols]),
                ("cat", OneHotEncoder(handle_unknown="ignore"), [c.lower() for c in cat_cols]),
            ]
        )
        clf = RandomForestClassifier(
            n_estimators=200,
            random_state=42,
            class_weight="balanced"
        )
        pipe = Pipeline([("pre", pre), ("clf", clf)])

        X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        pipe.fit(X_tr, y_tr)
        y_pr = pipe.predict(X_te)
        report = classification_report(y_te, y_pr)

        payload = {
            "model": pipe,
            "threshold_min": SLA_THRESHOLD_MIN,
            "feature_names_in": list(X.columns),
            "class_labels": sorted(list(np.unique(y)))
        }
        dump(payload, MODEL_PATH)

        load_model()

        return {
            "status": "ok",
            "message": "Model retrained",
            "log_tail": report[-500:]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
