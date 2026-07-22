import os
import json
import asyncio
from mini_raft_kv.kv.client_table import ClientTable
from mini_raft_kv.kv.state_machine import StateMachine
from mini_raft_kv.common.config import SnapShotConfig

class Snapshot:
    def __init__(self, cfg):
        self.path = cfg.path

    async def save(self, sm: StateMachine,index):
        d = sm.dt
        c = sm.ct.to_dict()
        l_a = index
        r = {
            "last_applied": l_a,
            "kv": d,
            "client_table": c
        }
        # 原子写：.tmp → fsync → rename → fsync 目录
        tmp_path = self.path
        with open(tmp_path, "w") as f:
            f.write(json.dumps(r))
            f.flush()
            await asyncio.to_thread(os.fsync, f.fileno())
        os.rename(tmp_path, self.path)
        # fsync 目录，确保 rename 落盘
        dir_fd = os.open(os.path.dirname(self.path) or ".", os.O_RDONLY)
        await asyncio.to_thread(os.fsync, dir_fd)
        os.close(dir_fd)

    async def load(self):
        if not os.path.exists(self.path):
            return None
        with open(self.path, "r") as f:
            raw = f.read()
        return json.loads(raw)
