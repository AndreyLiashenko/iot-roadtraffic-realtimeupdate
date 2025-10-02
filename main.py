from fastapi import FastAPI
from kafka import KafkaProducer
import json
from pydantic import BaseModel, Field
from typing import Tuple, Union
import time

app = FastAPI(title="Traffic Real-time Update API")

KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
KAFKA_TOPIC = 'traffic_updates'

producer = None

@app.on_event("startup")
async def startup_event():
    """Initializes the Kafka Producer on application startup with retries."""
    global producer
    
    max_retries = 10
    retry_delay = 6
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka (Attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda k: k.encode('utf-8'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            if producer.bootstrap_connected():
                print("✅ Kafka Producer connected successfully.")
                return
        except Exception as e:
            print(f"❌ Could not connect to Kafka: {e}")
        
        print(f"Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)

    print("❌ Failed to connect to Kafka after several retries. The application might not work correctly.")

@app.on_event("shutdown")
async def shutdown_event():
    """Closes the Kafka connection on application shutdown."""
    if producer:
        producer.close()
        print("🔌 Kafka connection closed.")

class TrafficData(BaseModel):
    edge: Tuple[Union[int, str], Union[int, str]]
    speed: float = Field(..., ge=0)
    timestamp: float


# --- API Endpoints ---

@app.post("/update-traffic")
def update_traffic_data(data: TrafficData):
    """
    Receives data from the simulator and sends it to a Kafka topic,
    using the edge as a key.
    """
    if not producer:
        return {"status": "error", "message": "Kafka producer is not initialized."}

    print(f"Received data: {data.dict()}")

    try:
        edge_key = str(data.edge)

        future = producer.send(
            KAFKA_TOPIC,
            key=edge_key,
            value=data.dict()
        )

        return {"status": "ok", "message": "Data received and sent to Kafka."}
    except Exception as e:
        print(f"❌ Error sending to Kafka: {e}")
        return {"status": "error", "message": f"Error sending to Kafka: {e}"}

@app.get("/")
def read_root():
    """Main endpoint. Just a check to see if the service is alive."""
    return {"message": "Traffic App is running!"}

@app.get("/health")
def health_check():
    """Endpoint to check the service status, including Kafka status."""
    kafka_status = "connected" if producer and producer.bootstrap_connected() else "disconnected"
    return {"status": "ok", "kafka_status": kafka_status}