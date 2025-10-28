#!/usr/bin/env python3
import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from dateutil import parser as dtp
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm


# ===== Helpers =====
def to_iso(ts: Any) -> str:
    """
    Vrati ISO8601 string u UTC sa sufiksom 'Z'.
    Ako ts nema tzinfo, tretira se kao UTC.
    Ako parsiranje padne, koristi se trenutni UTC.
    """
    def now_utc_z() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    try:
        if ts is None or (isinstance(ts, float) and pd.isna(ts)):
            return now_utc_z()
        dt = dtp.parse(str(ts))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return now_utc_z()


def map_row(r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mapira jedan red (dict) iz Kaggle CSV-a na REST payload za Gateway.
    Dodaj ili prilagodi alias-e u pick(...) da odgovaraju tačno tvom CSV-u.
    """
    def pick(*keys: str, default=None):
        for k in keys:
            if k in r and pd.notna(r[k]):
                return r[k]
        return default
    
    # Calculate distance from coordinates if available
    def calc_distance(lat1, lon1, lat2, lon2):
        from math import radians, cos, sin, asin, sqrt
        try:
            lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
            lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * asin(sqrt(a))
            km = 6371 * c
            return round(km, 2)
        except:
            return 10.0

    store_lat = pick("Store_Latitude", "store_lat")
    store_lon = pick("Store_Longitude", "store_lon")
    drop_lat = pick("Drop_Latitude", "drop_lat")
    drop_lon = pick("Drop_Longitude", "drop_lon")
    
    distance_km = calc_distance(store_lat, store_lon, drop_lat, drop_lon) if all([store_lat, store_lon, drop_lat, drop_lon]) else float(pick("Distance (KM)", "Distance KM", "distance_km", "DistanceKm", default=10.0) or 10.0)

    return {
        # Dodaj varijante ključeva koje ima tvoj CSV (vidi df.columns.tolist())
        "orderId":          str(pick("Order_ID", "Order ID", "order_id", "OrderID", "Order_ID", default="AMZ-NA")),
        "deliveryPersonId": str(pick("Delivery Person ID", "delivery_person_id", "Person ID", "person_id", "Agent_ID", "agent_id", default="D-NA")),
        "city":             str(pick("City", "city", "Customer City", "Area", "area", default="NA")),
        "weather":          str(pick("Weather", "weather", default="Unknown")),
        "traffic":          str(pick("Traffic", "traffic", default="Unknown")).strip(),
        "distanceKm": distance_km,
        "timeTakenMin": float(pick("Time Taken (min)", "Time Taken Min", "time_taken_min", "TimeTakenMin", "Delivery_Time", "delivery_time", default=30.0) or 30.0),
        "deliveryTimestamp": to_iso(pick("Delivery Time", "delivery_timestamp", "Delivered Time", "Order_Date", "order_date")),
        "deliveryStatus":    str(pick("Delivery Status", "delivery_status", "Status", "DeliveryStatus", "Delivery_Status", "Current Status", "Current_Status", default="Delivered")),
    }


def make_session(timeout: float) -> requests.Session:
    """
    Requests Session sa retry/backoff-om za POST na 429/5xx greške.
    """
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))

    orig_request = s.request

    def _request(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return orig_request(method, url, **kwargs)

    s.request = _request
    return s


# ===== Main =====
def main():
    ap = argparse.ArgumentParser(description="Amazon Delivery CSV -> Gateway REST (IoT stream simulator)")
    ap.add_argument("--csv", required=True, help="putanja do CSV (npr. data/amazon_delivery.csv)")
    ap.add_argument("--gateway", default="http://localhost:8080", help="Gateway base URL (default http://localhost:8080)")
    ap.add_argument("--rate", type=float, default=10.0, help="događaja u sekundi (default 10)")
    ap.add_argument("--batch", type=int, default=1, help="zapisa po HTTP request-u (default 1)")
    ap.add_argument("--limit", type=int, default=0, help="maks broj redova (0 = ceo fajl)")
    ap.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout u sekundama (default 10)")
    args = ap.parse_args()

    # Učitaj CSV
    try:
        df = pd.read_csv(args.csv)
    except Exception as e:
        print("Ne mogu da pročitam CSV:", e, file=sys.stderr)
        sys.exit(1)

    if args.limit and args.limit > 0:
        df = df.head(args.limit)

    rows: List[Dict[str, Any]] = df.to_dict(orient="records")
    total = len(rows)
    if total == 0:
        print("CSV nema redova posle filtriranja (limit) – nema šta da se pošalje.")
        return

    url = f"{args.gateway.rstrip('/')}/api/deliveries"
    delay = 1.0 / max(args.rate, 0.1)
    session = make_session(args.timeout)

    sent = 0
    batch_buf: List[Dict[str, Any]] = []

    print(f"Gateway: {url}")
    print(f"Ukupno redova za slanje: {total}  | rate={args.rate}/s  batch={args.batch}  timeout={args.timeout}s")

    for r in tqdm(rows, desc="Slanje", unit="row"):
        payload = map_row(r)
        batch_buf.append(payload)

        if len(batch_buf) >= args.batch:
            for item in batch_buf:
                try:
                    resp = session.post(url, json=item)
                    if resp.status_code >= 300:
                        print(f"POST failed [{resp.status_code}]: {resp.text[:200]}", file=sys.stderr)
                    else:
                        sent += 1
                except Exception as e:
                    print("POST error:", e, file=sys.stderr)
            batch_buf.clear()
            time.sleep(delay)

            if sent % 50 == 0:
                print(f"Poslato {sent}/{total}...")

    if batch_buf:
        for item in batch_buf:
            try:
                resp = session.post(url, json=item)
                if resp.status_code >= 300:
                    print(f"POST failed [{resp.status_code}]: {resp.text[:200]}", file=sys.stderr)
                else:
                    sent += 1
            except Exception as e:
                print("POST error:", e, file=sys.stderr)

    print(f"✅ Gotovo: poslato {sent}/{total} zapisa.")


if __name__ == "__main__":
    main()