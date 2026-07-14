import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rpc import server
import asyncio

async def main():
      srv = server.Server("data/1.log")
      await srv.kvstore.recover()
      await srv.run()

asyncio.run(main())