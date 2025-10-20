from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # MQTT konekcija
    MQTT_HOST: str = "mosquitto"
    MQTT_PORT: int = 1883
    MQTT_QOS: int = 1
    MQTT_RETAIN: bool = False

    # Topici
    MQTT_IN_TOPIC: str = "iot/deliveries/raw"
    MQTT_OUT_TOPIC: str = "iot/deliveries/events"

    # Pragovi
    THRESHOLD_TIME_TAKEN_MIN: float = 30.0
    THRESHOLD_DISTANCE_KM: float = 20.0

    # Ostalo
    SERVICE_ID: str = "eventmanager-1"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
