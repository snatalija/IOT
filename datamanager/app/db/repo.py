from sqlalchemy import create_engine, select, func, and_
from sqlalchemy.orm import sessionmaker
from .models import Base, Delivery
import os
from datetime import datetime

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/iot_delivery"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

def init_db():
    Base.metadata.create_all(engine)

class FilterObj:
    def __init__(self, city="", person_id="", status="", from_ts="", to_ts=""):
        self.city = city or ""
        self.person_id = person_id or ""
        self.status = status or ""
        self.from_ts = from_ts or ""
        self.to_ts = to_ts or ""

def _parse_ts(ts: str):
    if not ts: return None
    try:
        return datetime.fromisoformat(ts.replace("Z","+00:00"))
    except Exception:
        return None

def _filters(q, f: FilterObj):
    conds = []
    if f.city:      conds.append(Delivery.city == f.city)
    if f.person_id: conds.append(Delivery.delivery_person_id == f.person_id)
    if f.status:    conds.append(Delivery.delivery_status == f.status)
    f_from = _parse_ts(f.from_ts)
    f_to   = _parse_ts(f.to_ts)
    if f_from: conds.append(Delivery.delivery_timestamp >= f_from)
    if f_to:   conds.append(Delivery.delivery_timestamp <= f_to)
    if conds: q = q.where(and_(*conds))
    return q

def create(item_dict):
    with SessionLocal() as s:
        obj = Delivery(**item_dict)
        s.add(obj); s.commit(); s.refresh(obj)
        return obj

def get_by_id(id_):
    with SessionLocal() as s:
        return s.get(Delivery, id_)

def update(item_dict):
    with SessionLocal() as s:
        obj = s.get(Delivery, item_dict["id"])
        if not obj: return None
        for k,v in item_dict.items():
            setattr(obj, k, v)
        s.commit(); s.refresh(obj); return obj

def delete(id_):
    with SessionLocal() as s:
        obj = s.get(Delivery, id_)
        if not obj: return False
        s.delete(obj); s.commit(); return True

def list_(filt: FilterObj, limit=50, offset=0):
    with SessionLocal() as s:
        q = select(Delivery)
        q = _filters(q, filt).limit(limit).offset(offset)
        return s.execute(q).scalars().all()

def aggregate(filt: FilterObj, fields):
    with SessionLocal() as s:
        results = []
        for field_name, op in fields:
            col = getattr(Delivery, field_name)
            agg = {"MIN": func.min(col), "MAX": func.max(col),
                   "AVG": func.avg(col), "SUM": func.sum(col)}[op]
            q = select(agg.label("v"))
            q = _filters(q, filt)
            v = s.execute(q).scalar()
            results.append((field_name, op, float(v) if v is not None else 0.0))
        return results
