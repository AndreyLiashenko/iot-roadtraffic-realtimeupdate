import json
import asyncio
import time
from collections import defaultdict, deque
from typing import Tuple, Union, Deque

from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, Field, validator

from edge_mapper import EdgeMapper

# --- Settings ---
SERVICE_NAME = "traffic-app-producer"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "traffic_updates"
GRAPH_FILE = "irpin_drive_graph.graphml"
RECONNECT_DELAY = 5
MAX_RETRIES = 10
WINDOW_SECONDS = 60

# --- Global state ---
app = FastAPI(title="Traffic Real-time Update API Gateway (Async)")
mapper = EdgeMapper(GRAPH_FILE)
producer: AIOKafkaProducer | None = None

# --- Sliding window storage for Data Density ---
class DensityStore:
    def __init__(self, window_seconds: int = WINDOW_SECONDS):
        self.window = window_seconds
        self._events: dict[int, Deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def update_and_get(self, edge_id: int, ts: float) -> int:
        threshold = ts - self.window
        async with self._lock:
            dq = self._events[edge_id]
            while dq and dq[0] < threshold:
                dq.popleft()
            dq.append(ts)
            return len(dq)

density_store = DensityStore()

# --- Models ---
class TrafficData(BaseModel):
    edge: Tuple[Union[int, str], Union[int, str]]
    speed: float = Field(..., ge=0)
    car_count: int = Field(..., ge=0)
    capacity: int = Field(..., ge=1)
    load_factor: float = Field(..., ge=0)
    timestamp: float

    @validator("load_factor")
    def _lf_clip(cls, v):
        return max(0.0, float(v))

# --- Kafka connection ---
async def connect_kafka():
    global producer
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[{SERVICE_NAME}] Connecting to Kafka... (attempt {attempt})")
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=20,
            )
            await producer.start()
            print(f"✅ [{SERVICE_NAME}] Kafka connected.")
            return
        except Exception as e:
            print(f"❌ Kafka connection failed: {e}")
            await asyncio.sleep(RECONNECT_DELAY)
    raise RuntimeError("Failed to connect to Kafka after multiple retries.")

async def stop_kafka():
    global producer
    if producer:
        try:
            await producer.stop()
            print("🔌 Kafka connection closed.")
        except Exception as e:
            print(f"⚠️ Error during Kafka shutdown: {e}")

@app.on_event("startup")
async def on_startup():
    print(f"🚀 [{SERVICE_NAME}] Starting up...")
    await connect_kafka()

@app.on_event("shutdown")
async def on_shutdown():
    await stop_kafka()

# --- Routes ---
@app.post("/update-traffic")
async def update_traffic_data(data: TrafficData):
    global producer
    if not producer:
        raise HTTPException(status_code=500, detail="Kafka producer not initialized")

    try:
        u, v = data.edge
        edge_id = mapper.get_edge_id(u, v)

        density = await density_store.update_and_get(edge_id, data.timestamp)

        message = {
            "edge_id": edge_id,
            "speed": data.speed,
            "timestamp": data.timestamp,
            "car_count": data.car_count,
            "capacity": data.capacity,
            "load_factor": data.load_factor,
            "data_density_60s": density,
        }

        await producer.send_and_wait(
            topic=KAFKA_TOPIC,
            key=str(edge_id),
            value=message,
        )

        print(
            f"📤 edge={edge_id} speed={data.speed:.1f} cc={data.car_count} "
            f"cap={data.capacity} lf={data.load_factor:.3f} dens60={density}"
        )
        return {"status": "ok", "edge_id": edge_id, "data_density_60s": density}

    except Exception as e:
        print(f"❌ Kafka send error: {e}")
        raise HTTPException(status_code=500, detail=f"Kafka send failed: {e}")

@app.get("/health")
async def health_check():
    status = "connected" if producer and producer._closed is False else "disconnected"
    return {"status": "ok", "kafka": status}

@app.get("/")
async def index():
    return {"message": "Traffic API Gateway (async) is running!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)