import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, PlainTextResponse
import hashlib

from starlette.middleware.gzip import GZipMiddleware
import live_transport
from datetime import datetime

app = FastAPI()
app.add_middleware(GZipMiddleware)


def load_stations(file_path):
    if not os.path.exists(file_path):
        print(f"Файл {file_path} не найден.")
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump([], file, ensure_ascii=False, indent=4)
        return []

    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)


def stations_connector():
    city = "Ufa" + "/"
    stations = load_stations(city + 'сhernikovka.json')
    stations += load_stations(city + 'sipajlovo.json')
    stations += load_stations(city + 'center.json')
    stations += load_stations(city + 'novostroyki.json')
    stations += load_stations(city + 'soviet.json')
    stations += load_stations(city + 'oktyabrskij.json')
    stations += load_stations(city + 'zelyonaya_roshcha.json')
    stations += load_stations(city + 'ordzhonikidzevskij_yug.json')
    stations += load_stations(city + 'inors.json')
    return stations


stations = stations_connector()
stations_json = json.dumps(stations, ensure_ascii=False, sort_keys=True)
stations_hash = hashlib.sha256(stations_json.encode('utf-8')).hexdigest()
clients = {}
live_transport_list = ""


@asynccontextmanager
async def lifespan(app: FastAPI):
    task_live_data = asyncio.create_task(get_live_data(live_transport.LiveGeo().live()))
    task_broadcast = asyncio.create_task(broadcast_transport())
    yield
    task_live_data.cancel()
    task_broadcast.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/stations")
def get_stations():
    return JSONResponse(content=stations)


@app.get("/stations/hash")
def get_stations_hash():
    return PlainTextResponse(content=stations_hash)


@app.get("/transport")
def get_transport():
    bus = load_stations('buses_20000.json')
    return JSONResponse(content=bus)


@app.websocket("/transport")
async def transport_ws(websocket: WebSocket):
    await websocket.accept()
    client_id = str(uuid.uuid4())
    clients[client_id] = {"websocket": websocket, "last_geo": None, "last_send": None}

    try:
        while True:
            data = await websocket.receive_json()
            last_geo = {
                "lat_min": data["lat_min"],
                "lat_max": data["lat_max"],
                "lon_min": data["lon_min"],
                "lon_max": data["lon_max"],
            }
            clients[client_id]["last_geo"] = last_geo
    except WebSocketDisconnect:
        del clients[client_id]
    except:
        del clients[client_id]


async def broadcast_transport():
    global live_transport_list
    old = ""
    while True:
        if live_transport_list != old:
            for client_id, info in list(clients.items()):
                ws = info["websocket"]
                last_geo = info["last_geo"]
                last_send = info["last_send"]

                if not last_geo:
                    continue

                lat_min = last_geo["lat_min"]
                lat_max = last_geo["lat_max"]
                lon_min = last_geo["lon_min"]
                lon_max = last_geo["lon_max"]

                answer = []
                for item in live_transport_list:
                    try:
                        u_lat = float(item['u_lat'])
                        u_long = float(item['u_long'])
                        if lat_min <= u_lat <= lat_max and lon_min <= u_long <= lon_max:
                            answer.append(item)
                    except:
                        continue
                try:
                    new_send = hashlib.sha256(json.dumps(answer, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
                    if last_send != new_send:
                        await ws.send_json(answer)
                        info["last_send"] = new_send
                except:
                    del clients[client_id]
            old = live_transport_list
        await asyncio.sleep(5)


async def get_live_data(live_data):
    global live_transport_list
    data = "None"
    old = ""
    new = ""
    #live_data = load_stations('transrb_real_ufa.json')
    while True:
        async for line in live_data:
            data = line
            break
        if data != "None":
            if isinstance(data, str):
                data = data.replace('\\"', '"')
                data = json.loads(data)
            if data.get('result'):
                new = data.get('result')
        if new != old:
            old = new
            live_transport_list = new


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=2727, reload=True)
