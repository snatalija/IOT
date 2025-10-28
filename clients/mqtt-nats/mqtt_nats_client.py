import os, json, threading, time
from datetime import datetime

import paho.mqtt.client as mqtt

import asyncio
import signal
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot/deliveries/events")

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_SUBJECT = os.getenv("NATS_SUBJECT", "analytics.risk")

def pretty(ts_ms=None):
    if ts_ms is None:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        return datetime.fromtimestamp(ts_ms/1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def run_mqtt():
    def on_connect(client, userdata, flags, rc, properties=None):
        print(f"[MQTT] Connected rc={rc}, subscribing '{MQTT_TOPIC}'")
        client.subscribe(MQTT_TOPIC, qos=1)

    def on_message(client, userdata, msg):
        body = msg.payload.decode("utf-8", errors="ignore")
        try:
            data = json.loads(body)
        except Exception:
            data = body
        print(f"\n[MQTT] {pretty()} topic='{msg.topic}':")
        print(json.dumps(data, indent=2, ensure_ascii=False))

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_forever()

async def run_nats(stop_event: asyncio.Event):
    nc = NATS()
    try:
        await nc.connect(servers=[NATS_URL])
        print(f"[NATS] Connected to {NATS_URL}, subscribing '{NATS_SUBJECT}'")

        async def handler(msg):
            body = msg.data.decode("utf-8", errors="ignore")
            try:
                data = json.loads(body)
            except Exception:
                data = body
            print(f"\n[NATS] {pretty()} subject='{msg.subject}':")
            print(json.dumps(data, indent=2, ensure_ascii=False))

        await nc.subscribe(NATS_SUBJECT, cb=handler)

        await stop_event.wait()
        await nc.drain()
    except (ErrConnectionClosed, ErrTimeout, ErrNoServers) as e:
        print(f"[NATS] Error: {e}")
    finally:
        if nc.is_connected:
            await nc.close()

def main():
    t = threading.Thread(target=run_mqtt, daemon=True)
    t.start()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        loop.run_until_complete(run_nats(stop_event))
    finally:
        loop.close()

if __name__ == "__main__":
    print("Starting MqttNats client…")
    print(f"MQTT: host={MQTT_HOST} port={MQTT_PORT} topic={MQTT_TOPIC}")
    print(f"NATS: url={NATS_URL} subject={NATS_SUBJECT}")
    main()
