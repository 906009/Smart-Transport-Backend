import asyncio
import httpx
import re
import json


class GeoStartSession:
    def __init__(self, url: str, headers: dict):
        self.url = url
        self.headers = headers

    async def start(self):
        sid = "None"
        payload = {
            "jsonrpc": "2.0",
            "method": "startSession",
            "params": {},
            "id": 1
        }
        async with httpx.AsyncClient(verify=False) as client:
            try:
                response = await client.post(self.url, json=payload, headers=self.headers)
                if response.status_code == 200:
                    try:
                        result = response.json()
                        sid = result.get("result", {}).get("sid")
                    except:
                        pass
            except:
                pass
        return sid


class GeoParser:
    def __init__(self, url: str, headers: dict, sid: str):
        self.url = url
        self.headers = headers
        self.sid = sid

    async def parser(self, id: int):
        data = "None"
        payload = {
            "jsonrpc": "2.0",
            "method": "getUnitsInRect",
            "params": {
                "sid": self.sid,
                "minlat": 21.152821376838904,
                "maxlat": 77.77378781013385,
                "minlong": 39.93164956569672,
                "maxlong": 113.05664956569673
            },
            "id": id
        }
        async with httpx.AsyncClient(verify=False) as client:
            try:
                response = await client.post(self.url, json=payload, headers=self.headers)
                if response.status_code == 200:
                    data = response.json()
                    data = json.dumps(data)
                    data = re.sub(r'(".*?")', lambda match: match.group(0).replace('"', '\\"'), data)
                    data = json.loads(data)
                    data = str(data).replace("'", "\"")
                    yield data
            except:
                pass
        yield data


class LiveGeo:
    def __init__(self):
        self.url = "https://transportrb.ru/api/rpc.php"
        self.headers = {"Content-Type": "application/json"}
        self.session = GeoStartSession(self.url, self.headers)

    async def live(self):
        while True:
            sid = await self.session.start()
            while not sid or sid == "None":
                sid = await self.session.start()
                await asyncio.sleep(10)

            parser = GeoParser(self.url, self.headers, sid)
            for id in range(2, 366):
                async for data in parser.parser(id):
                    yield data
                    await asyncio.sleep(5)
