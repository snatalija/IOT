from pydantic import BaseModel, Field
from typing import Optional


class Delivery(BaseModel):
    id: str
    orderId: str
    deliveryPersonId: str
    city: str
    weather: str
    traffic: str
    distanceKm: float
    timeTakenMin: float
    deliveryTimestamp: str  # ISO8601 string
    deliveryStatus: str


class DeliveryEvent(BaseModel):
    eventType: str  # "created" | "updated"
    source: str     # "datamanager"
    delivery: Delivery


class DetectedEvent(BaseModel):
    eventType: str = Field(default="threshold.exceeded")
    rule: str
    field: str
    threshold: float
    actual: float
    city: Optional[str] = None
    timestamp: Optional[str] = None  # preuzimamo iz deliveryTimestamp
    originalDeliveryId: Optional[str] = None
    sourceId: str = "eventmanager"
