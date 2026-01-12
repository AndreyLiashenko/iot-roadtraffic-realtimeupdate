# iot-roadtraffic-realtimeupdate

Real-time Internet-of-Vehicles (IoV) platform for dynamic road-traffic monitoring and ETA-aware routing on a road network using Kafka streaming, adaptive smoothing, fuzzy inference, and TGNN-based A* heuristics.

The system continuously ingests traffic events from moving vehicles, updates time-dependent edge weights of a road graph, and provides the foundation for **time-dependent shortest-path routing** using **TGNN-driven heuristics**.

This project serves as a **digital twin of a real road network (Irpin)** and a research backend for **machine-learning-assisted routing in dynamic graphs**.

---

## 🧭 High-level architecture

```mermaid
flowchart LR
  A[IoT / Traffic Simulator] --> K[(Kafka<br/>traffic_updates)]
  K --> SUT[consumer-sut<br/>EMA]
  K --> SW[consumer-sliding-window]
  K --> FUZZ[consumer-sut-fuzzy<br/>Fuzzy α]

  FUZZ -->|alpha_rules| K
  SUT --> REC[consumer-recalc]
  SW --> REC
  FUZZ --> REC

  REC --> G[CSRGraph / EdgeMapper<br/>Edge ETA state]

  G --> UI[Web UI]
  UI --> B[(Browser)]

  SUT --> P[/Prometheus/]
  SW --> P
  FUZZ --> P
  REC --> P
  KEXP[kafka-exporter] --> P
