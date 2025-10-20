import os, json, threading
import paho.mqtt.client as mqtt
from typing import Optional

class Publisher:
    def __init__(self, host: str, port: int, topic: str, qos: int = 1, retain: bool = False):
        self.host = host
        self.port = port
        self.topic = topic
        self.qos = qos
        self.retain = retain

        self._client = mqtt.Client()
        self._client.on_connect = self._on_connect
        self._client.on_publish = self._on_publish

        self._client.connect(self.host, self.port, 60)
        self._client.loop_start()

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        print(f"[MQTT] connected rc={rc} host={self.host}:{self.port}")

    def _on_publish(self, client, userdata, mid):
        print(f"[MQTT] published mid={mid}")

    def publish_delivery(self, payload: dict):
        data = json.dumps(payload)
        res = self._client.publish(self.topic, data, qos=self.qos, retain=self.retain)
        if res.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"publish rc={res.rc}")
        return res

_pub: Optional[Publisher] = None
_lock = threading.Lock()

def get_publisher() -> Publisher:
    global _pub
    if _pub:
        return _pub
    with _lock:
        if _pub:
            return _pub
        host = os.getenv("MQTT_HOST", "mosquitto")
        port = int(os.getenv("MQTT_PORT", "1883"))
        topic = os.getenv("MQTT_TOPIC_DELIVERIES", "iot/deliveries/raw")
        qos = int(os.getenv("MQTT_QOS", "1"))
        retain = os.getenv("MQTT_RETAIN", "false").lower() == "true"
        print(f"[MQTT] init host={host} port={port} topic={topic} qos={qos} retain={retain}")
        _pub = Publisher(host, port, topic, qos, retain)
        return _pub
