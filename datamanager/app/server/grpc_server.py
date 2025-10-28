import grpc
from concurrent import futures
import os

from datamanager.app.db import repo
from datamanager.app.generated import delivery_pb2 as pb
from datamanager.app.generated import delivery_pb2_grpc as pbg
from datamanager.app.mqtt.publisher import get_publisher


class DeliveryService(pbg.DeliveryServiceServicer):

    def _to_pb(self, o):
        if not o:
            return None
        return pb.Delivery(
            id=o.id,
            order_id=o.order_id,
            delivery_person_id=o.delivery_person_id,
            city=o.city,
            weather=o.weather,
            traffic=o.traffic,
            distance_km=o.distance_km,
            time_taken_min=o.time_taken_min,
            delivery_timestamp=str(o.delivery_timestamp),
            delivery_status=o.delivery_status,
        )

    def _to_dict(self, o):
        if not o:
            return None
        return {
            "id": o.id,
            "order_id": o.order_id,
            "delivery_person_id": o.delivery_person_id,
            "city": o.city,
            "weather": o.weather,
            "traffic": o.traffic,
            "distance_km": o.distance_km,
            "time_taken_min": o.time_taken_min,
            "delivery_timestamp": str(o.delivery_timestamp),
            "delivery_status": o.delivery_status,
        }

    def _publish_after_write_obj(self, o, event_type: str):
            """Pretvori ORM objekat u dict i pošalji na MQTT."""
            try:
                payload = {
                    "eventType": event_type,
                    "source": "datamanager",
                    "delivery": {
                        "id": getattr(o, "id", None),
                        "orderId": getattr(o, "order_id", None),
                        "deliveryPersonId": getattr(o, "delivery_person_id", None),
                        "city": getattr(o, "city", None),
                        "weather": getattr(o, "weather", None),
                        "traffic": getattr(o, "traffic", None),
                        "distanceKm": float(getattr(o, "distance_km", 0) or 0),
                        "timeTakenMin": float(getattr(o, "time_taken_min", 0) or 0),
                        "deliveryTimestamp": str(getattr(o, "delivery_timestamp", "")),
                        "deliveryStatus": getattr(o, "delivery_status", None),
                    }
                }
                pub = get_publisher()
                print(f"[MQTT] publish -> {pub.topic}: {payload}") 
                pub.publish_delivery(payload)
            except Exception as e:
                print(f"[WARN] MQTT publish failed: {e}")
    # --- gRPC handlers ---
    def Create(self, request, context):
        d = request.item
        obj = repo.create({
            "id": d.id or None,
            "order_id": d.order_id,
            "delivery_person_id": d.delivery_person_id,
            "city": d.city,
            "weather": d.weather,
            "traffic": d.traffic,
            "distance_km": d.distance_km,
            "time_taken_min": d.time_taken_min,
            "delivery_timestamp": d.delivery_timestamp,
            "delivery_status": d.delivery_status,
        })
        # MQTT publish (created)
        self._publish_after_write_obj(obj, event_type="created")
        return pb.CreateResponse(item=self._to_pb(obj))

    def GetById(self, request, context):
        obj = repo.get_by_id(request.id)
        return pb.GetByIdResponse(item=self._to_pb(obj) if obj else None)

    def Update(self, request, context):
        d = request.item
        obj = repo.update({
            "id": d.id,
            "order_id": d.order_id,
            "delivery_person_id": d.delivery_person_id,
            "city": d.city,
            "weather": d.weather,
            "traffic": d.traffic,
            "distance_km": d.distance_km,
            "time_taken_min": d.time_taken_min,
            "delivery_timestamp": d.delivery_timestamp,
            "delivery_status": d.delivery_status,
        })
        if obj:
            self._publish_after_write_obj(obj, event_type="updated")

        return pb.UpdateResponse(item=self._to_pb(obj) if obj else None)

    def Delete(self, request, context):
        ok = repo.delete(request.id)
        return pb.DeleteResponse(success=ok)

    def List(self, request, context):
        f = request.filter
        filt = repo.FilterObj(
            city=f.city, person_id=f.person_id, status=f.status,
            from_ts=f.from_ts, to_ts=f.to_ts
        )
        items = repo.list_(filt, request.limit or 50, request.offset or 0)
        return pb.ListResponse(items=[self._to_pb(o) for o in items])

    def Aggregate(self, request, context):
        f = request.filter
        filt = repo.FilterObj(
            city=f.city, person_id=f.person_id, status=f.status,
            from_ts=f.from_ts, to_ts=f.to_ts
        )
        fields = []
        for af in request.fields:
            fields.append((af.field_name, pb.AggregateOp.Name(af.op)))
        results = repo.aggregate(filt, fields)
        out = []
        for (fname, op, val) in results:
            out.append(pb.AggregateResult(field_name=fname, op=pb.AggregateOp.Value(op), value=val))
        return pb.AggregateResponse(results=out)


def serve():
    repo.init_db()
    port = os.environ.get("GRPC_PORT", "50051")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pbg.add_DeliveryServiceServicer_to_server(DeliveryService(), server)
    server.add_insecure_port(f"[::]:{port}")
    print(f"gRPC DataManager listening on {port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
