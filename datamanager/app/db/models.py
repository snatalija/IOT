from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Float, TIMESTAMP, text
import uuid

class Base(DeclarativeBase):
    pass

def gen_uuid() -> str:
    return str(uuid.uuid4())

class Delivery(Base):
    __tablename__ = "deliveries"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    order_id: Mapped[str] = mapped_column(String(64))
    delivery_person_id: Mapped[str] = mapped_column(String(64))
    city: Mapped[str] = mapped_column(String(64))
    weather: Mapped[str] = mapped_column(String(64))
    traffic: Mapped[str] = mapped_column(String(64))
    distance_km: Mapped[float] = mapped_column(Float)
    time_taken_min: Mapped[float] = mapped_column(Float)
    delivery_timestamp: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"))
    delivery_status: Mapped[str] = mapped_column(String(32))
