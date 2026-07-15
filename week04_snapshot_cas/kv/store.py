from common import log
from storage import wal
from typing import Optional

class KvStore:
    def __init__(self, wal_path: str, sync_mode="always"):
        """创建 KvStore，内部持有 WAL 实例"""
        self.wal_path = wal_path
        self.sync_mode = sync_mode
        self.wal = wal.WAL(self.wal_path, self.sync_mode)
        self.data = dict()

    async def put(self, key: str, value: str, client_id: str, seq: int) -> dict:
        """写操作：先写 WAL → fsync → 再改内存 → 返回"""
        record = {
            "op" : "Put",
            "key" : key,
            "value" : value,
            "client_id" : client_id,
            "seq" : str(seq)
        }
        await self.wal.append(record)

        self.data[key] = value

        return {
            "ok" : True,
            "result" : "OK",
        }
    async def get(self, key: str) -> Optional[str]:
        """读操作：直接读内存"""
        return self.data.get(key)
    async def delete(self, key: str, client_id: str, seq: int) -> dict:
        """删除操作：也是先写 WAL"""
        record = {
            "op" : "Del",
            "key" : key,
            "client_id" : client_id,
            "seq" : seq
        }
        await self.wal.append(record)
        v = self.data.pop(key, None)
        return {
            "ok" : True,
            "result" : "OK"
        }
    async def recover(self):
        """启动恢复：replay WAL → 重建内存 HashMap"""
        records = await self.wal.replay()
        for re in records:
            if re.get("op","").lower() == "put":
                self.data[re.get("key","")] = re.get("value","")
            elif re.get("op","").lower() =="del":
                self.data.pop(re.get("key",""), None)
            else:
                continue
    async def cas(self,key: str,expected_version: int,new_value: dict,client_id: str,seq: int) -> dict:
        value_old = self.data.get[key]
        if value_old is 
    async def close(self):
        """关闭"""
        pass