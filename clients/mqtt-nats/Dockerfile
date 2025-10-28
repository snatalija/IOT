FROM python:3.11-slim
WORKDIR /app

COPY clients/mqtt-nats/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY clients/mqtt-nats/mqtt_nats_client.py /app/mqtt_nats_client.py

ENV PYTHONUNBUFFERED=1
ENV MQTT_HOST=mosquitto
ENV MQTT_PORT=1883
ENV MQTT_TOPIC=iot/deliveries/events
ENV NATS_URL=nats://nats:4222
ENV NATS_SUBJECT=analytics.risk

CMD ["python","/app/mqtt_nats_client.py"]
