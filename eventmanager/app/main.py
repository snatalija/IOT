from eventmanager.app.config import settings
from eventmanager.app.mqtt.consumer import RawConsumer

def main():
    print(
        "[EventManager] starting with config:\n"
        f"- IN  topic: {settings.MQTT_IN_TOPIC}\n"
        f"- OUT topic: {settings.MQTT_OUT_TOPIC}\n"
        f"- thresholds: timeTakenMin>{settings.THRESHOLD_TIME_TAKEN_MIN}, "
        f"distanceKm>{settings.THRESHOLD_DISTANCE_KM}\n"
        f"- mqtt: {settings.MQTT_HOST}:{settings.MQTT_PORT} qos={settings.MQTT_QOS} retain={settings.MQTT_RETAIN}"
    )
    RawConsumer().start()

if __name__ == "__main__":
    main()
