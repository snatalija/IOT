# IoT Amazon Delivery — Mikroservisi (REST + gRPC + MQTT + NATS + ML)

**End‑to‑end** demo sistem za isporuke koji:
- prima događaje sa “senzora” (simulator),
- skladišti ih u **PostgreSQL** preko **gRPC** servisa (*DataManager*),
- izlaže **REST** API preko .NET **Gateway**‑a,
- obrađuje događaje preko **MQTT** (*EventManager*),
- računa rizik kašnjenja koristeći **MLaaS** (FastAPI),
- i objavljuje analitiku na **NATS** (*analytics.risk*).

Sve komponente se podižu preko **Docker Compose**.

---

## Arhitektura 

```
SensorGenerator ──> Gateway (REST) ──(gRPC)──> DataManager ──> PostgreSQL
                               │
                               └──────────────> EventManager (MQTT in/out)
                                                    │
                               Mosquitto <──────────┘  (iot/deliveries/events)
                                                    │
                           Analytics ──HTTP──> MLaaS (FastAPI /predict)
                               │
                               └──────────────> NATS (analytics.risk)
                                                 │
                                   nats-box / mqttnats-client (prikaz)
```

**Ključni tokovi:**
1. **CRUD** nad isporukama ide REST‑om preko **Gateway**‑a → poziva **DataManager** gRPC → čuva u **PostgreSQL**.
2. **EventManager** i/ili **DataManager** objavljuju MQTT poruke na temu `iot/deliveries/events` (sirovi ili izvedeni događaji).
3. **Analytics** se pretplaćuje na MQTT, gradi feature‑e i zove **MLaaS** `/predict`, pa rezultat objavi na **NATS** u subject `analytics.risk`.
4. **nats-box** i **mqttnats-client** služe za demo/inspekciju poruka.

---

## Struktura repozitorijuma

```
.
├─ analytics/
│  └─ app/
│     ├─consumer.py                 # MQTT -> ML -> NATS
      ├─main.py
   └─Dockerfile.analytics
├─ asyncapi/
   ├─ analytics-risk.yml             # šema za NATS subject "analytics.risk"
   ├─ analytics-nats.yml 
   ├─ datamanager-mqtt.yaml
   ├─ event-manager-mqtt.yaml
├─ clients/
│  └─ mqtt-nats/
│     ├─ mqtt_nats_client.py         # konzolna aplikacija za prikaz MQTT & NATS
│     └─ Dockerfile
├─ data/
│  └─ amazon_delivery.csv            # set podataka za ML (Kaggle)
├─ datamanager/
│  └─ app/ 
├─ docker/
│  ├─ Dockerfile.datamanager
│  ├─ Dockerfile.eventmanager
│  ├─ Dockerfile.gateway
│  └─ mosquitto.conf                 # MQTT broker konfiguracija
                          # gRPC servisi (+ SQLAlchemy modeli, repo)
├─ eventmanager/
│  └─ app/                           # MQTT subscribe/procesiranje/republish
├─ gateway/
├─ mlaas/
│  ├─ app/
│  │  └─ main.py                     # FastAPI: /health, /predict, /metrics, /train
│  ├─ train.py                       # trenira i snima model.pkl
│  └─ requirements.txt
├─ sensor-generator/
│  └─ send_csv.py
├─ proto/
│  └─ delivery.proto                 # gRPC definicije servisa i poruka
├─ web/
   └─ mqtt-client.html
├─ docker-compose.yml
└─ README.md
```

---

## Servisi
- **Gateway** – ASP.NET Core 8 REST API (CRUD + aggregations) komunicira sa DataManager putem gRPC.
- **DataManager** – Python servis, implementira gRPC API (+ PostgreSQL za čuvanje podataka).
- **EventManager** – Python servis koji se povezuje na MQTT (consumer/producer) – obradjuje pristigle događaje, izvodi zaključke i objavljuje nove događaje.
- **MLaaS** – Python FastAPI model servis, trenira model, služi ga putem /predict, i izlaže metrike preko /metrics. Koristi se za predviđanje kašnjenja isporuka.
- **Analytics** – Python servis koji prima podatke sa MQTT, prosleđuje ih MLaaS-u radi analize, i objavljuje rezultate u NATS poruke (kašnjenja).
- **Mosquitto** MQTT broker – razmenjuje poruke između uređaja/servisa koji koriste MQTT protokol.
- **NATS** - message broker
- **nats-box** - CLI testing container, za testiranje NATS sistema
- **mqttnats-client** – Python konzola koja prikazuje poruke sa MQTT i NATS sistema u realnom vremenu radi praćenja toka podataka.
- **PostgreSQL** – baza podataka.
- **Docker Compose** – pokretanje svih servisa u kontejnerima.

---

## Quick Start

### 1) Preduslovi
- Docker Desktop / Docker Engine + Compose
- (Opcionalno) Postman (za REST i gRPC), `curl`, `jq`

### 2) Podići infrastrukturu + servise
```bash
docker compose build
docker compose up
docker compose ps
```

### 3) Provera ML servisa
```bash
curl -s http://localhost:9000/health | jq
# → { "status": "ok", "has_model": true }

# Opciona obuka
curl -s -X POST http://localhost:9000/train | jq

# Probni /predict
curl -s -X POST http://localhost:9000/predict \
  -H "Content-Type: application/json" \
  -d '{"city":"Belgrade","weather":"Clear","traffic":"Low","distanceKm":4.2,"hour":13,"weekday":1}' | jq
```

### 4) Demo tok podataka (MQTT → ML → NATS)
Objavi primer događaja na **MQTT** (Mosquitto):
```bash
docker exec -it mosquitto sh -lc \
"mosquitto_pub -h localhost -t 'iot/deliveries/events' \
 -m '{\"deliveryId\":\"AMZ-NATS-TEST\",\"city\":\"Belgrade\",\"weather\":\"Clear\",\"traffic\":\"Low\",\"distanceKm\":4.2,\"hour\":13,\"weekday\":1}'"
```

Pretplata na **NATS** i prikaz izlaza **Analytics**‑a:
```bash
docker exec -it nats-box sh -lc 'nats sub analytics.risk -s nats://nats:4222'
# Primer izlaza:
# {"eventType":"analytics.risk","source":"analytics", "features":{...},
#  "prediction":{"late":1,"proba_late":0.9,"threshold_min":30.0}, ...}
```

---

## Gateway (REST) — Primeri

**Base URL:** `http://localhost:8080`

- `POST /deliveries` — kreiranje isporuke  
- `GET /deliveries/{id}` — čitanje po ID  
- `PUT /deliveries/{id}` — izmena (pošalji ceo objekat sa izmenama)  
- `DELETE /deliveries/{id}` — brisanje  
- `GET /deliveries?city=Belgrade&limit=10&offset=0` — lista sa filterima/paginacijom  
- `GET /deliveries/aggregate?city=Belgrade&from=...&to=...&fields=distance_km:avg,time_taken_min:max` — agregacije  

Ako je omogućen **Swagger**, otvori: `http://localhost:8080/swagger`

---

## DataManager (gRPC + Postgres)

**Servis:** `delivery.DeliveryService` (primer)  
**Metode:**

- **Create** (`DeliveryService/Create`)
```json
{
  "item": {
    "id": "D-001",
    "order_id": "O-123",
    "delivery_person_id": "P-7",
    "city": "Belgrade",
    "weather": "Clear",
    "traffic": "Low",
    "distance_km": 4.2,
    "time_taken_min": 25,
    "delivery_timestamp": "2025-10-23T13:00:00Z",
    "delivery_status": "delivered"
  }
}
```

- **GetById**
```json
{ "id": "D-001" }
```

- **Update** (vrati ceo objekat sa izmenjenim poljima)
```json
{ "item": { "...isto kao Create, sa izmenjenim poljima..." } }
```

- **Delete**
```json
{ "id": "D-001" }
```

- **List** (filter + paginacija)
```json
{ "filter": { "city": "Belgrade" }, "limit": 10, "offset": 0 }
```

- **Aggregate** (AVG/MIN/MAX/SUM po poljima)
```json
{
  "filter": { "city": "Belgrade", "from_ts": "2025-01-01T00:00:00Z", "to_ts": "2025-12-31T23:59:59Z" },
  "fields": [
    { "field_name": "distance_km", "op": "AVG" },
    { "field_name": "time_taken_min", "op": "MAX" }
  ]
}
```

---

## EventManager (MQTT)

- **Ulazna tema (in):** `iot/deliveries/events`
- **Izlazna tema (opciono out):** npr. `iot/deliveries/derived`
- **Logika:** prima “sirove” događaje, može da primeni pragove (npr. `THRESHOLD_TIME_TAKEN_MIN`, `THRESHOLD_DISTANCE_KM`) i objavi “alarm/derived” događaj.

**Promenljive okruženja:**
- `MQTT_HOST`, `MQTT_PORT`
- `MQTT_IN_TOPIC`, `MQTT_OUT_TOPIC`
- `THRESHOLD_TIME_TAKEN_MIN`, `THRESHOLD_DISTANCE_KM`

---

## Analytics (MQTT → MLaaS → NATS)

- **Pretplata (MQTT):** `iot/deliveries/events`  
- **Poziva ML:** `POST http://mlaas:9000/predict`  
- **Objava (NATS):** subject `analytics.risk`

**Promenljive okruženja:**
- `MQTT_HOST`, `MQTT_PORT`, `MQTT_IN_TOPIC`
- `ML_URL` (npr. `http://mlaas:9000/predict`)
- `NATS_URL` (npr. `nats://nats:4222`), `NATS_SUBJECT` (npr. `analytics.risk`)

---

## MLaaS (FastAPI)

- `GET /health` → `{ "status": "ok", "has_model": true/false }`
- `POST /predict`  
  **Ulaz:**  
  `{"city","weather","traffic","distanceKm","hour","weekday"}`  
  **Izlaz:**  
  `{"late": 0/1, "proba_late": <0..1>, "threshold_min": <float>}`
- `POST /train` → re‑trening i snimanje `model.pkl`
- `GET /metrics` → Prometheus format

**Model:** treniran nad `data/amazon_delivery.csv` (Kaggle), skladišten kao `MODEL_PATH` (npr. `/app/model.pkl`).

---

## MQTT & NATS kratki vodič

**MQTT publish primer:**
```bash
mosquitto_pub -h localhost -t iot/deliveries/events \
  -m '{"deliveryId":"TEST-1","city":"Belgrade","weather":"Clear","traffic":"Low","distanceKm":3.1,"hour":10,"weekday":2}'
```

**NATS subscribe primer (iz kontejnera):**
```bash
docker exec -it nats-box sh -lc 'nats sub analytics.risk -s nats://nats:4222'
```

---

## Promenljive okruženja (primeri)

**Gateway**
- `GRPC_URL` (npr. `datamanager:50051`)
- `ASPNETCORE_URLS` (npr. `http://0.0.0.0:8080`)

**DataManager**
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME`
- `GRPC_PORT` (podrazumevano 50051)

**EventManager**
- `MQTT_HOST`, `MQTT_PORT`, `MQTT_IN_TOPIC`, `MQTT_OUT_TOPIC`
- `THRESHOLD_TIME_TAKEN_MIN`, `THRESHOLD_DISTANCE_KM`

**Analytics**
- `MQTT_HOST`, `MQTT_PORT`, `MQTT_IN_TOPIC`
- `ML_URL`, `NATS_URL`, `NATS_SUBJECT`

**MLaaS**
- `MODEL_PATH`, `DATA_PATH`, `PORT` (9000)

---

## Testiranje (Postman)

- **REST (Gateway):** import kolekcije ili rute ručno; podesite `{{baseUrl}} = http://localhost:8080`  
- **gRPC (DataManager):** import `proto/delivery.proto`, podesite target `localhost:50051`  
- **MQTT:** koristite eksterni MQTT klijent ili `mosquitto_pub/sub` iz kontejnera  
- **NATS:** `nats-box` komande (`nats sub`, `nats pub`)

---

## Šema podataka (primer)

**Delivery**:
- `id: string`
- `order_id: string`
- `delivery_person_id: string`
- `city: string`
- `weather: enum("Clear","Rain","Clouds",...)`
- `traffic: enum("Low","Medium","High")`
- `distance_km: number`
- `time_taken_min: number`
- `delivery_timestamp: RFC3339 string`
- `delivery_status: enum("pending","in_progress","delivered","failed")`

---

## Resource

https://www.kaggle.com/datasets/sujalsuthar/amazon-delivery-dataset?select=amazon_delivery.csv
