from mini_raft_kv.common.command import Command
from mini_raft_kv.common.query import Query
from mini_raft_kv.kv.state_machine import StateMachine
from mini_raft_kv.replication.base import Engine
from mini_raft_kv.storage.wal import WAL


class LocalEngine(Engine):

    def __init__(self, wl:WAL, sm:StateMachine):
        self.wl = wl
        self.sm = sm

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