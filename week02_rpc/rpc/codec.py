import json
import struct

def encode_message(obj: dict) -> bytes:
    body = json.dumps(obj).encode('utf-8')
    length = struct.pack('>I', len(body))
    return length + body
async def decode_message(data: bytes) -> dict:
    header = await data.readexactly(4)
    length = struct.unpack('>I', header)[0]
    body = await data.readexactly(length)
    return json.loads(body.decode('utf-8'))