import asyncio
from mini_raft_kv.common.config import load_config
from mini_raft_kv.kv.state_machine import StateMachine
from mini_raft_kv.replication.local import LocalEngine
from mini_raft_kv.rpc.server import Server
from mini_raft_kv.storage.wal import WAL

async def main():
    # TODO(组装，自上而下的依赖在这里 new 并注入):
    cfg = load_config("config/local.yaml")
    wal = WAL(cfg.wal)
    sm  = StateMachine()
    for cmd in await wal.replay(): 
        sm.apply(cmd)
    eg  = LocalEngine(wal, sm)
    srv = Server(eg, cfg.server)
    await srv.run()

if __name__ == "__main__":
    asyncio.run(main())