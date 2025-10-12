import asyncio
import aiohttp
import networkx as nx
import random
import time
import socket
from datetime import datetime
import argparse
from typing import Tuple

DEFAULT_API_URL = "http://host.docker.internal:8001/update-traffic"
GRAPH_FILE = "irpin_drive_graph.graphml"
LOG_FILE = "traffic_log.txt"

class TrafficManager:
    def __init__(self, graph):
        self._edge_states = {}
        self._lock = asyncio.Lock()
        for u, v, key in graph.edges(keys=True):
            base_speed = 50.0
            capacity = 10 + random.randint(-5, 5)
            self._edge_states[(u, v, key)] = {
                "car_count": 0,
                "base_speed": base_speed,
                "current_speed": base_speed,
                "capacity": max(1, capacity),
                "incident": False,
            }

    async def _recalculate_speed(self, edge_id):
        state = self._edge_states[edge_id]
        if state["incident"]:
            state["current_speed"] = 5.0
            return
        load_factor = state["car_count"] / state["capacity"]
        state["current_speed"] = max(state["base_speed"] * (1 - min(load_factor, 1.0)), 5.0)

    async def enter_edge(self, edge_id):
        async with self._lock:
            self._edge_states[edge_id]["car_count"] += 1
            await self._recalculate_speed(edge_id)

    async def leave_edge(self, edge_id):
        async with self._lock:
            self._edge_states[edge_id]["car_count"] = max(
                0, self._edge_states[edge_id]["car_count"] - 1
            )
            await self._recalculate_speed(edge_id)

    async def get_speed(self, edge_id) -> float:
        async with self._lock:
            return self._edge_states.get(edge_id, {}).get("current_speed", 50.0)

    async def get_state(self, edge_id):
        async with self._lock:
            st = self._edge_states.get(edge_id)
            if not st:
                return {"current_speed": 50.0, "car_count": 0, "capacity": 1}
            return {
                "current_speed": st["current_speed"],
                "car_count": st["car_count"],
                "capacity": st["capacity"],
            }


async def worker(session, queue, stats, api_url: str):
    while True:
        data = await queue.get()
        try:
            async with session.post(api_url, json=data, timeout=10) as resp:
                if resp.status == 200:
                    stats["success"] += 1
                else:
                    stats["failed"] += 1
        except Exception:
            stats["failed"] += 1
        finally:
            queue.task_done()


async def car_simulation(car_id, graph, nodes, manager, queue, send_interval: float):
    start = random.choice(nodes)
    end = random.choice(nodes)
    try:
        path = nx.shortest_path(graph, source=start, target=end, weight="length")
    except Exception:
        return
    for i in range(len(path) - 1):
        u, v = path[i], path[i + 1]
        edge_id = (u, v, 0)
        await manager.enter_edge(edge_id)
        try:
            edge_len = graph.get_edge_data(u, v, 0).get("length", 500)
            elapsed = 0
            while True:
                st = await manager.get_state(edge_id)
                speed = st["current_speed"]
                car_count = st["car_count"]
                capacity = st["capacity"]
                load_factor = car_count / capacity if capacity else 1.0

                speed_mps = max(speed / 3.6, 0.1)
                distance_left = edge_len * (
                    1 - (elapsed / (edge_len / speed_mps if elapsed > 0 else edge_len))
                )
                time_left = distance_left / speed_mps
                if time_left <= 5:
                    await asyncio.sleep(time_left)
                    break

                data = {
                    "edge": [u, v],
                    "speed": speed,
                    "car_count": car_count,
                    "capacity": capacity,
                    "load_factor": load_factor,
                    "timestamp": time.time(),
                }
                await queue.put(data)
                await asyncio.sleep(send_interval)
                elapsed += send_interval
        finally:
            await manager.leave_edge(edge_id)


async def monitor(queue, stats):
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("timestamp,queue_size,success,failed\n")
    while True:
        await asyncio.sleep(5)
        line = f"{datetime.now().isoformat()},{queue.qsize()},{stats['success']},{stats['failed']}\n"
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
        print(line.strip())


async def main(total_cars: int, concurrency: int, send_interval: float, api_url: str):
    graph = nx.read_graphml(GRAPH_FILE)
    for _, _, data in graph.edges(data=True):
        for key in ["length", "speed_kph", "eta"]:
            if key in data:
                try:
                    data[key] = float(data[key])
                except ValueError:
                    pass

    nodes = list(graph.nodes)
    manager = TrafficManager(graph)
    queue = asyncio.Queue()
    stats = {"success": 0, "failed": 0}

    connector = aiohttp.TCPConnector(
        limit=concurrency, limit_per_host=concurrency, family=socket.AF_INET
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [
            asyncio.create_task(worker(session, queue, stats, api_url))
            for _ in range(concurrency)
        ]
        cars = [
            asyncio.create_task(
                car_simulation(i + 1, graph, nodes, manager, queue, send_interval)
            )
            for i in range(total_cars)
        ]
        monitor_task = asyncio.create_task(monitor(queue, stats))
        await asyncio.gather(*cars)
        await queue.join()
        await monitor_task


def parse_args() -> Tuple[int, int, float, str]:
    p = argparse.ArgumentParser(description="Async traffic simulator")
    p.add_argument("-k", "--cars", type=int, default=3000, help="Кількість машин")
    p.add_argument("-c", "--concurrency", type=int, default=500, help="Кількість паралельних відправників")
    p.add_argument("-s", "--send-interval", type=float, default=5.0, help="Інтервал відправки (сек)")
    p.add_argument("--api", type=str, default=DEFAULT_API_URL, help="URL API /update-traffic")
    a = p.parse_args()
    return a.cars, a.concurrency, a.send_interval, a.api


if __name__ == "__main__":
    cars, conc, interval, api = parse_args()
    print(f"Start: cars={cars}, concurrency={conc}, send_interval={interval}, api={api}")
    asyncio.run(main(cars, conc, interval, api))