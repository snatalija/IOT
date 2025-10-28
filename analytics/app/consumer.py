import os
import json
import threading
import asyncio
import time

import requests
import paho.mqtt.client as mqtt
from nats.aio.client import Client as NATS
from dateutil import parser as date_parser

# === Config iz ENV-a ===
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_IN_TOPIC = os.getenv("MQTT_IN_TOPIC", "iot/deliveries/events")

ML_URL = os.getenv("ML_URL", "http://localhost:9000/predict")

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_SUBJECT = os.getenv("NATS_SUBJECT", "analytics.risk")

_nats_nc = NATS()
_nats_loop = asyncio.new_event_loop()
_nats_ready = threading.Event()

async def _nats_connect():
    await _nats_nc.connect(servers=[NATS_URL])
    print(f"[analytics] Connected to NATS: {NATS_URL}")

def _nats_loop_runner():
    asyncio.set_event_loop(_nats_loop)
    _nats_loop.run_until_complete(_nats_connect())
    _nats_ready.set()
    _nats_loop.run_forever()

def nats_start():
    t = threading.Thread(target=_nats_loop_runner, daemon=True)
    t.start()
    # sačekaj konekciju
    _nats_ready.wait(timeout=10)

async def _nats_publish_async(subject: str, payload: bytes):
    await _nats_nc.publish(subject, payload)

def nats_publish(subject: str, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    fut = asyncio.run_coroutine_threadsafe(
        _nats_publish_async(subject, data), _nats_loop
    )

def on_connect(client, userdata, flags, rc):
    print(f"[analytics] MQTT connected rc={rc}, sub {MQTT_IN_TOPIC}")
    client.subscribe(MQTT_IN_TOPIC, qos=1)

def on_message(client, userdata, msg):
    try:
        raw = msg.payload.decode("utf-8")
        event = json.loads(raw)
        
        # EventManager sends DetectedEvent with structure:
        # {eventType, rule, field, threshold, actual, city, timestamp, originalDeliveryId}
        print(f"[analytics] Received event: {event}")
        
        # Extract timestamp to get hour and weekday
        ts_str = event.get("timestamp", "")
        try:
            dt = date_parser.parse(ts_str)
            hour = dt.hour
            weekday = dt.weekday()
        except:
            hour = 12
            weekday = 3
        
        # Build features for ML prediction
        # We need: area (city), weather, traffic, distanceKm, hour, weekday
        # From DetectedEvent we have: city, actual (which is either distanceKm or timeTakenMin)
        # We don't have weather/traffic in DetectedEvent, so use defaults
        features = {
            "area": event.get("city", "Unknown"),
            "weather": "Clear",  # Not available in DetectedEvent
            "traffic": "Medium",  # Not available in DetectedEvent
            "distanceKm": float(event.get("actual", 1.0)) if event.get("field") == "distanceKm" else 10.0,
            "hour": hour,
            "weekday": weekday,
        }

        resp = requests.post(ML_URL, json=features, timeout=3)
        resp.raise_for_status()
        pred = resp.json()

        out_msg = {
            "eventType": "analytics.risk",
            "source": "analytics",
            "violationRule": event.get("rule"),
            "violationField": event.get("field"),
            "threshold": event.get("threshold"),
            "actual": event.get("actual"),
            "city": event.get("city"),
            "features": features,
            "prediction": pred,
            "originalDeliveryId": event.get("originalDeliveryId"),
            "ts": int(time.time() * 1000),
        }

        nats_publish(NATS_SUBJECT, out_msg)
        print(f"[analytics] ⚠️ VIOLATION DETECTED: {event.get('rule')} in {event.get('city')} | {event.get('field')}={event.get('actual')} > {event.get('threshold')}")
        print(f"[analytics] 🤖 ML PREDICTION: {pred}")
        print(f"[analytics] 📤 NATS publish -> {NATS_SUBJECT}")

    except Exception as ex:
        print(f"[analytics] ERROR handling message: {ex}")

def main():
    nats_start()

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()
