import os, json, requests
import paho.mqtt.client as mqtt
import pandas as pd

MQTT_HOST   = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT   = int(os.getenv("MQTT_PORT", "1883"))
IN_TOPIC    = os.getenv("MQTT_IN_TOPIC", "iot/deliveries/raw")
OUT_TOPIC   = os.getenv("MQTT_OUT_TOPIC", "iot/analytics/risk")
ML_URL      = os.getenv("ML_URL", "http://mlaas:9000/predict")

FEATURES = ["city","weather","traffic","distanceKm","hour","weekday"]

def to_features(delivery: dict):
    ts = pd.to_datetime(delivery.get("deliveryTimestamp"), errors="coerce", utc=True)
    hour = int(ts.hour) if pd.notna(ts) else 12
    weekday = int(ts.weekday()) if pd.notna(ts) else 3
    return {
        "city": delivery.get("city", "Unknown"),
        "weather": delivery.get("weather", "Unknown"),
        "traffic": delivery.get("traffic", "Unknown"),
        "distanceKm": float(delivery.get("distanceKm", 0)),
        "hour": hour,
        "weekday": weekday
    }

def on_connect(client, userdata, flags, rc, properties=None):
    print("Analytics connected:", rc)
    client.subscribe(IN_TOPIC, qos=1)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        delivery = payload.get("delivery", payload)
        feat = to_features(delivery)
        r = requests.post(ML_URL, json=feat, timeout=5)
        r.raise_for_status()
        pred = r.json()

        out = {
            "eventType": "analytics.risk",
            "source": "analytics",
            "features": {k: feat[k] for k in FEATURES},
            "prediction": pred,
            "originalDeliveryId": delivery.get("id"),
        }
        client.publish(OUT_TOPIC, json.dumps(out), qos=1, retain=False)
        print("Published ->", OUT_TOPIC, out)
    except Exception as e:
        print("Analytics error:", e)

def main():
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="analytics")
    c.on_connect = on_connect
    c.on_message = on_message
    c.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    c.loop_forever()

if __name__ == "__main__":
    main()
