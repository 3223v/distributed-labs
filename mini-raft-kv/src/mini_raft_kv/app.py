import asyncio
from mini_raft_kv.rpc.server import Server

async def main():
    srv = Server()
    await srv.run()

if __name__ == "__main__":
    asyncio.run(main())