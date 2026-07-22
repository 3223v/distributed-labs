from mini_raft_kv.common.command import Command
from mini_raft_kv.common.query import Query
from mini_raft_kv.kv.state_machine import StateMachine
from mini_raft_kv.replication.base import Engine
from mini_raft_kv.storage.wal import WAL
from mini_raft_kv.storage.snapshot import Snapshot


class LocalEngine(Engine):

    def __init__(self, wl:WAL, sm:StateMachine, sst:Snapshot):
        self.wl = wl
        self.sm = sm
        self.sst = sst

    async def submit(self,cmd:Command)->dict:
        if not cmd.islegal():
            return {
                "ok" : False,
                "result" : None,
                "error" : {
                    "code" : "",
                    "data" : "",
                    "message" : "参数不合法"
                }
            }
        await self.wl.append(cmd)
        return self.sm.apply(cmd)

    async def query(self,qry:Query)->dict:
        return self.sm.read(qry)

    async def snapshot(self):
        index = self.wl.index
        await self.sst.save(self.sm, index)
        await self.wl.truncate_before(index)
        return {"ok": True, "result": {"wal_index": index}, "error": None}