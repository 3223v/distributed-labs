import asyncio
from mini_raft_kv.common.config import load_config
from mini_raft_kv.kv.state_machine import StateMachine
from mini_raft_kv.replication.local import LocalEngine
from mini_raft_kv.rpc.server import Server
from mini_raft_kv.storage.wal import WAL
import os

async def main():
    cfg = load_config("config/local.yaml")
    os.makedirs(os.path.dirname(cfg.wal.path) or ".", exist_ok=True)
    wal = WAL(cfg.wal)
    await wal.start()
    sm  = StateMachine()
    for cmd in await wal.replay():
        sm.apply(cmd)
    eg  = LocalEngine(wal, sm)
    srv = Server(eg, cfg.server)
    try:
        await srv.run()
    finally:
        await wal.stop()

if __name__ == "__main__":
    asyncio.run(main())