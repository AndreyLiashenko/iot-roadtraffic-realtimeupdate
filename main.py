import json
import asyncio
import time
from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, Field
from typing import Tuple, Union
from edge_mapper import EdgeMapper

# --- Settings ---
SERVICE_NAME = "traffic-app-producer"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "traffic_updates"
GRAPH_FILE = "irpin_drive_graph.graphml"
RECONNECT_DELAY = 5
MAX_RETRIES = 10

# --- Global state ---
app = FastAPI(title="Traffic Real-time Update API Gateway (Async)")
mapper = EdgeMapper(GRAPH_FILE)
producer: AIOKafkaProducer | None = None


# --- Models ---
class TrafficData(BaseModel):
    edge: Tuple[Union[int, str], Union[int, str]]
    speed: float = Field(..., ge=0)
    timestamp: float


# --- Kafka connection ---
async def connect_kafka():
    """Connects to Kafka with retries and stores producer globally."""
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
    """Safely closes Kafka producer."""
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
    """Receives real-time traffic data and publishes to Kafka."""
    global producer

    if not producer:
        raise HTTPException(status_code=500, detail="Kafka producer not initialized")

    try:
        # Map (u, v) → edge_id
        u, v = data.edge
        edge_id = mapper.get_edge_id(u, v)

        message = {
            "edge_id": edge_id,
            "speed": data.speed,
            "timestamp": data.timestamp,
        }

        # Async send to Kafka
        await producer.send_and_wait(
            topic=KAFKA_TOPIC,
            key=str(edge_id),
            value=message,
        )

        print(f"📤 Sent edge {edge_id} speed={data.speed:.1f}")
        return {"status": "ok", "edge_id": edge_id}

    except Exception as e:
        print(f"❌ Kafka send error: {e}")
        raise HTTPException(status_code=500, detail=f"Kafka send failed: {e}")


@app.get("/health")
async def health_check():
    """Checks Kafka connection status."""
    status = "connected" if producer and producer._closed is False else "disconnected"
    return {"status": "ok", "kafka": status}


@app.get("/")
async def index():
    return {"message": "Traffic API Gateway (async) is running!"}


# --- Main entry (for local testing) ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)