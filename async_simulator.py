import asyncio
import aiohttp
import networkx as nx
import random
import time
import socket
import math
from datetime import datetime
import argparse
from typing import Tuple

# --- CONFIG ---
DEFAULT_API_URL = "http://localhost:8001/update-traffic"
GRAPH_FILE = "irpin_drive_graph.graphml"
LOG_FILE = "traffic_log.txt"

class TrafficManager:
    def __init__(self, graph):
        self._edge_states = {}
        self._lock = asyncio.Lock()
        
        print("🔍 TrafficManager: Визначення типів доріг (OSM Highway tags)...")
        
        type_stats = {}

        for u, v, key, data in graph.edges(keys=True, data=True):
            # 1. Визначаємо ТИП ДОРОГИ (highway tag)
            # Це найнадійніше джерело правди про структуру міста
            highway = data.get("highway")
            
            # Іноді це список ['residential', 'unclassified'], беремо перший
            if isinstance(highway, list): 
                highway = highway[0]
            
            highway = str(highway).lower() # Нормалізація
            type_stats[highway] = type_stats.get(highway, 0) + 1

            # 2. ПРАВИЛА (HEURISTICS)
            # Встановлюємо швидкість і ємність на основі типу
            if highway in ['trunk', 'trunk_link', 'primary', 'primary_link']:
                # Магістралі (Варшавка, Соборна)
                speed_limit = 80.0
                capacity = random.randint(40, 60) # Дуже важко забити
                
            elif highway in ['secondary', 'secondary_link']:
                # Основні артерії (Університетська)
                speed_limit = 60.0
                capacity = random.randint(20, 30)
                
            elif highway in ['tertiary', 'tertiary_link']:
                # Звичайні міські дороги
                speed_limit = 45.0
                capacity = random.randint(10, 18)
                
            elif highway in ['residential', 'living_street', 'service', 'unclassified']:
                # Вулички в приватному секторі, двори
                speed_limit = 30.0
                # --- КЛЮЧОВИЙ МОМЕНТ ---
                # Робимо їх дуже "вузькими", щоб 3-5 машин вже створювали затор
                capacity = random.randint(3, 6) 
                
            else:
                # Фолбек для невідомих типів
                speed_limit = 50.0
                capacity = random.randint(8, 12)

            self._edge_states[(u, v, key)] = {
                "car_count": 0,
                "speed_limit": speed_limit,
                "current_speed": speed_limit,
                "capacity": capacity,
                "incident": False,
                "highway_type": highway # Для дебагу
            }
            
        print("📊 Статистика типів доріг:")
        # Покажемо топ-5 типів, щоб ти бачив, що граф розпізнано
        for h_type, count in sorted(type_stats.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"   - {h_type}: {count} сегментів")

    async def _recalculate_speed(self, edge_id):
        state = self._edge_states[edge_id]
        limit = state["speed_limit"]
        
        if state["incident"]:
            state["current_speed"] = 5.0
            return
            
        load_factor = state["car_count"] / state["capacity"]
        
        # --- BPR Function (Агресивна) ---
        # alpha=2.0, beta=2 (квадратична залежність)
        # Швидкість починає помітно падати вже при 50% завантаження.
        # Це створить красиву криву, а не пряму лінію.
        bpr_coef = 1.0 + 2.0 * (load_factor ** 2) 
        new_speed = limit / bpr_coef
        
        # Шум +/- 15% (щоб точки розсипалися хмарою)
        noise = random.uniform(0.85, 1.15)
        new_speed *= noise
        
        # Не менше 1 км/год
        state["current_speed"] = max(new_speed, 1.0)

    async def enter_edge(self, edge_id):
        async with self._lock:
            self._edge_states[edge_id]["car_count"] += 1
            await self._recalculate_speed(edge_id)

    async def leave_edge(self, edge_id):
        async with self._lock:
            val = self._edge_states[edge_id]["car_count"] - 1
            self._edge_states[edge_id]["car_count"] = max(0, val)
            await self._recalculate_speed(edge_id)

    async def get_state(self, edge_id):
        async with self._lock:
            st = self._edge_states.get(edge_id)
            if not st:
                return {"current_speed": 50.0, "speed_limit": 50.0, "car_count": 0, "capacity": 10}
            return {
                "current_speed": st["current_speed"],
                "speed_limit": st["speed_limit"],
                "car_count": st["car_count"],
                "capacity": st["capacity"],
            }

async def worker(session, queue, stats, api_url: str):
    while True:
        data = await queue.get()
        try:
            async with session.post(api_url, json=data, timeout=2) as resp:
                if resp.status == 200:
                    stats["success"] += 1
                else:
                    stats["failed"] += 1
        except Exception:
            stats["failed"] += 1
        finally:
            queue.task_done()

async def car_simulation(car_id, graph, nodes, manager, queue, send_interval: float):
    # Random Walk (Рівномірний розподіл по місту)
    start = random.choice(nodes)
    end = random.choice(nodes)
        
    try:
        path = nx.shortest_path(graph, source=start, target=end, weight="length")
    except nx.NetworkXNoPath:
        return
    except Exception:
        return
    
    for i in range(len(path) - 1):
        u, v = path[i], path[i + 1]
        
        edge_key = 0
        if graph.has_edge(u, v):
             keys = list(graph[u][v].keys())
             edge_key = keys[0]
             
        edge_id = (u, v, edge_key)
        
        await manager.enter_edge(edge_id)
        try:
            edge_data = graph.get_edge_data(u, v, edge_key)
            edge_len = float(edge_data.get("length", 500.0))
            
            elapsed = 0
            while True:
                st = await manager.get_state(edge_id)
                
                speed_kmh = st["current_speed"]
                speed_mps = max(speed_kmh / 3.6, 0.1)
                
                dist_covered = elapsed * speed_mps
                distance_left = max(0, edge_len - dist_covered)
                
                if distance_left <= 0: break
                
                time_to_finish = distance_left / speed_mps
                if time_to_finish <= send_interval:
                    await asyncio.sleep(time_to_finish)
                    break

                data = {
                    "edge": [u, v],
                    "speed": speed_kmh,
                    "speed_limit": st["speed_limit"],
                    "car_count": st["car_count"],
                    "capacity": st["capacity"],
                    "load_factor": st["car_count"] / st["capacity"],
                    "timestamp": time.time(),
                }
                await queue.put(data)
                
                await asyncio.sleep(send_interval)
                elapsed += send_interval
        finally:
            await manager.leave_edge(edge_id)

async def monitor(queue, stats):
    print(f"{'Time':<10} | {'Queue':<6} | {'OK':<6} | {'Fail':<6}")
    while True:
        await asyncio.sleep(2.0)
        q_size = queue.qsize()
        s = stats['success']
        f = stats['failed']
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"{ts:<10} | {q_size:<6} | {s:<6} | {f:<6}")

async def main(total_cars: int, concurrency: int, send_interval: float, api_url: str):
    print(f"📂 Loading graph: {GRAPH_FILE}")
    graph = nx.read_graphml(GRAPH_FILE)
    
    print("⚙️ Converting graph attributes to floats...")
    for u, v, data in graph.edges(data=True):
        if 'length' in data:
            try:
                data['length'] = float(data['length'])
            except ValueError:
                data['length'] = 500.0
    
    nodes = list(graph.nodes)
    manager = TrafficManager(graph)
    queue = asyncio.Queue()
    stats = {"success": 0, "failed": 0}

    connector = aiohttp.TCPConnector(limit=concurrency)
    
    print(f"🚀 Starting simulation: {total_cars} cars")
    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [
            asyncio.create_task(worker(session, queue, stats, api_url))
            for _ in range(concurrency)
        ]
        
        cars = []
        batch_size = 50
        for i in range(0, total_cars, batch_size):
            chunk = range(i, min(i + batch_size, total_cars))
            for car_idx in chunk:
                cars.append(
                    asyncio.create_task(
                        car_simulation(car_idx + 1, graph, nodes, manager, queue, send_interval)
                    )
                )
            await asyncio.sleep(0.05)
            
        monitor_task = asyncio.create_task(monitor(queue, stats))
        
        await asyncio.gather(*cars)
        print("✅ All cars finished trips.")
        
        await queue.join()
        monitor_task.cancel()

def parse_args() -> Tuple[int, int, float, str]:
    p = argparse.ArgumentParser(description="Final Traffic Simulator")
    p.add_argument("-k", "--cars", type=int, default=3000)
    p.add_argument("-c", "--concurrency", type=int, default=200)
    p.add_argument("-s", "--send-interval", type=float, default=2.0)
    p.add_argument("--api", type=str, default=DEFAULT_API_URL)
    a = p.parse_args()
    return a.cars, a.concurrency, a.send_interval, a.api

if __name__ == "__main__":
    cars, conc, interval, api = parse_args()
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main(cars, conc, interval, api))