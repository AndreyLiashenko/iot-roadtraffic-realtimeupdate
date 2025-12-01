import json
import asyncio
import time
from collections import defaultdict, deque
from typing import Tuple, Union, Deque, Dict

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
WINDOW_SECONDS = 30 # Зменшив до 30, щоб реакція була швидшою

# --- Global state ---
app = FastAPI(title="Traffic Real-time Update API Gateway (Async)")
mapper = EdgeMapper(GRAPH_FILE)
producer: AIOKafkaProducer | None = None

class TrafficStateStore:
    def __init__(self, window_seconds: int = WINDOW_SECONDS):
        self.window = window_seconds
        self._history: dict[int, Deque[Tuple[float, float]]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def process_update(self, edge_id: int, ts: float, current_speed: float) -> Tuple[int, float]:
        threshold = ts - self.window
        
        async with self._lock:
            dq = self._history[edge_id]
            
            while dq and dq[0][0] < threshold:
                dq.popleft()
            
            # 2. Додаємо нові дані
            dq.append((ts, current_speed))
            
            # 3. Рахуємо щільність даних (кількість пакетів за вікно)
            density = len(dq)
            
            if dq:
                start_speed = dq[0][1] # Швидкість 30 сек тому
                delta_speed = current_speed - start_speed
            else:
                delta_speed = 0.0

            return density, delta_speed

state_store = TrafficStateStore()

# --- Models ---
class TrafficData(BaseModel):
    edge: Tuple[Union[int, str], Union[int, str]]
    speed: float = Field(..., ge=0)
    speed_limit: float = Field(..., ge=1)
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

        density, delta_speed = await state_store.process_update(edge_id, data.timestamp, data.speed)
        
        speed_efficiency = data.speed / data.speed_limit
        
        traffic_trend = delta_speed / data.speed_limit

        message = {
            "edge_id": edge_id,
            "timestamp": data.timestamp,
            "speed": data.speed,
            "speed_limit": data.speed_limit,
            "load_factor": data.load_factor,
            "speed_efficiency": speed_efficiency,
            "traffic_trend": traffic_trend,
            "car_count": data.car_count,
            "data_density_60s": density,
        }

        await producer.send_and_wait(
            topic=KAFKA_TOPIC,
            key=str(edge_id),
            value=message,
        )

        if int(data.timestamp * 10) % 50 == 0: # Рідше логуємо
            print(
                f"📤 edge={edge_id} "
                f"Eff={speed_efficiency:.2f} "
                f"Load={data.load_factor:.2f} "
                f"Trend={traffic_trend:.3f}" # Тепер тут будуть числа типу -0.2, +0.1
            )
            
        return {
            "status": "ok", 
            "edge_id": edge_id, 
            "vector": [speed_efficiency, data.load_factor, traffic_trend]
        }

    except Exception as e:
        print(f"❌ Kafka send error: {e}")
        raise HTTPException(status_code=500, detail=f"Kafka send failed: {e}")

@app.get("/health")
async def health_check():
    status = "connected" if producer and producer._closed is False else "disconnected"
    return {"status": "ok", "kafka": status}

@app.get("/")
async def index():
    return {"message": "Traffic API Gateway (Smart) is running!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)