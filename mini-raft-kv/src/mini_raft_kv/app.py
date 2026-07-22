import asyncio
from mini_raft_kv.common.config import load_config
from mini_raft_kv.kv.state_machine import StateMachine
from mini_raft_kv.replication.local import LocalEngine
from mini_raft_kv.rpc.server import Server
from mini_raft_kv.storage.wal import WAL
from mini_raft_kv.storage.snapshot import Snapshot
import os

async def main():
    cfg = load_config("config/local.yaml")
    os.makedirs(os.path.dirname(cfg.wal.path) or ".", exist_ok=True)
    wal = WAL(cfg.wal)
    sst = Snapshot(cfg.sst)
    await wal.start()
    sm  = StateMachine()
    # 恢复：优先 load snapshot → 灌入状态机 → 再 replay WAL
    snapshot_data = await sst.load()
    if snapshot_data is not None:
        sm.dt = snapshot_data.get("kv", {})
        sm.ct.from_dict(snapshot_data.get("client_table", {}))
        sm.last_applied = snapshot_data.get("last_applied", 0)
    for cmd in await wal.replay():
        sm.apply(cmd)
    eg  = LocalEngine(wal, sm , sst)
    srv = Server(eg, cfg.server)
    try:
        await srv.run()
    finally:
        await wal.stop()

if __name__ == "__main__":
    asyncio.run(main())