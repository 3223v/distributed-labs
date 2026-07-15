import json
import asyncio
from typing import Optional
from common import log

class Snapshot:

    @staticmethod
    async def save(path: str, data: dict) -> None:
        if str == "" or str is None:
            str = ".tmp"
        with open(str, "a") as f:
            s = json.dumps(data)
            f.write(s+"\n")
            f.flush()
            await asyncio.to_thread(ps.fsync,f.fileno())

    @staticmethod
    async def load(path: str, data: dict) -> Optional[dict]:
        if path == "" or path is None:
            return None
        with open(self.path, "r") as f:
            al = f.readlines()
        s = al[0].strip()
        if s == "" or s is None or not s:
            return None
        try:
            d = json.loads(s)
        except json.JSONDecodeError:
            log.error("TMP 行解析失败")
            return None
        return d