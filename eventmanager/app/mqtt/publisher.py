import json
import threading
from typing import Optional
import paho.mqtt.client as mqtt

from eventmanager.app.config import settings


class EventsPublisher:
    def __init__(self):
        self._client = mqtt.Client()
        self._connected = threading.Event()

        # callbacks
        self._client.on_connect = self._on_connect

        # konekcija
        self._client.connect(settings.MQTT_HOST, settings.MQTT_PORT, keepalive=30)
        self._client.loop_start()
        # sačekaj connect
        self._connected.wait(timeout=5)

    def _on_connect(self, client, userdata, flags, rc):
        # rc == 0 => OK
        print(f"[EventManager Publisher] MQTT connected rc={rc} to {settings.MQTT_HOST}:{settings.MQTT_PORT}")
        self._connected.set()

    def publish_detected(self, evt: dict):
        payload = json.dumps(evt, ensure_ascii=False)
        info = self._client.publish(
            settings.MQTT_OUT_TOPIC,
            payload=payload,
            qos=settings.MQTT_QOS,
            retain=settings.MQTT_RETAIN,
        )
        # opciono: .wait_for_publish() ako želiš sinhrono potvrdu
        return info

    def stop(self):
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:
            pass


_publisher_singleton: Optional[EventsPublisher] = None


def get_publisher() -> EventsPublisher:
    global _publisher_singleton
    if _publisher_singleton is None:
        _publisher_singleton = EventsPublisher()
    return _publisher_singleton
