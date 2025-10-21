import os, json, threading
import paho.mqtt.client as mqtt
import requests
from dateutil import parser

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_IN = os.getenv("MQTT_TOPIC_IN", "iot/deliveries/raw")

MLAAS_URL = os.getenv("MLAAS_URL", "http://mlaas:9000/predict")

def extract_features(msg_dict):
    d = msg_dict.get("delivery", {})
    ts = d.get("deliveryTimestamp")
    if ts:
        dt = parser.parse(ts)
        hour = dt.hour
        weekday = dt.weekday()
    else:
        hour, weekday = 12, 3

    return {
        "city": d.get("city", "Unknown"),
        "weather": d.get("weather", "Unknown"),
        "traffic": d.get("traffic", "Unknown"),
        "distanceKm": float(d.get("distanceKm", 0.0)),
        "hour": hour,
        "weekday": weekday,
    }

def on_connect(client, userdata, flags, rc):
    print(f"[Analytics] connected rc={rc}")
    client.subscribe(TOPIC_IN, qos=1)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        feats = extract_features(payload)
        r = requests.post(MLAAS_URL, json=feats, timeout=5)
        r.raise_for_status()
        pred = r.json()
        print(f"[Analytics] IN -> {feats}  |  PRED -> {pred}")
    except Exception as e:
        print(f"[Analytics][ERR] {e}")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()
