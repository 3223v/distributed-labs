from rpc import server
import asyncio
server = server.Server()
asyncio.run(server.run())