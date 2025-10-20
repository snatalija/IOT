import json
import paho.mqtt.client as mqtt
from typing import List

from eventmanager.app.config import settings
from eventmanager.app.models import DeliveryEvent, DetectedEvent
from eventmanager.app.mqtt.publisher import get_publisher


def detect_violations(evt: DeliveryEvent) -> List[DetectedEvent]:
    out: List[DetectedEvent] = []

    # pravilo 1: trajanje > prag
    if evt.delivery.timeTakenMin > settings.THRESHOLD_TIME_TAKEN_MIN:
        out.append(DetectedEvent(
            rule="timeTakenMin_over_threshold",
            field="timeTakenMin",
            threshold=settings.THRESHOLD_TIME_TAKEN_MIN,
            actual=evt.delivery.timeTakenMin,
            city=evt.delivery.city,
            timestamp=evt.delivery.deliveryTimestamp,
            originalDeliveryId=evt.delivery.id,
        ))

    # pravilo 2: distanca > prag
    if evt.delivery.distanceKm > settings.THRESHOLD_DISTANCE_KM:
        out.append(DetectedEvent(
            rule="distanceKm_over_threshold",
            field="distanceKm",
            threshold=settings.THRESHOLD_DISTANCE_KM,
            actual=evt.delivery.distanceKm,
            city=evt.delivery.city,
            timestamp=evt.delivery.deliveryTimestamp,
            originalDeliveryId=evt.delivery.id,
        ))

    return out


class RawConsumer:
    def __init__(self):
        self._client = mqtt.Client()
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

    def start(self):
        self._client.connect(settings.MQTT_HOST, settings.MQTT_PORT, keepalive=30)
        self._client.loop_forever()

    # callbacks
    def _on_connect(self, client, userdata, flags, rc):
        client.subscribe(settings.MQTT_IN_TOPIC, qos=settings.MQTT_QOS)
        print(f"[EventManager] connected and subscribed to {settings.MQTT_IN_TOPIC} (rc={rc})")

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8"))
            incoming = DeliveryEvent.model_validate(data)  # pydantic v2

            violations = detect_violations(incoming)
            if not violations:
                return

            pub = get_publisher()
            for v in violations:
                pub.publish_detected(v.model_dump())
                print(f"[EventManager] publish -> {settings.MQTT_OUT_TOPIC}: {v.model_dump()}")
        except Exception as ex:
            print(f"[EventManager][WARN] invalid message or publish failed: {ex}")
