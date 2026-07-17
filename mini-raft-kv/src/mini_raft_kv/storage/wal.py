import os
import json
import asyncio
import binascii
from mini_raft_kv.common import log
from mini_raft_kv.common.config import WalConfig

class WAL:
    def __init__(self, cfg :WalConfig):
        """path: wal 文件路径，如 data/wal.log"""
        self.path = cfg.path
        self.sync_mode = cfg.sync_mode

    async def append(self, cmd: Command) -> None:
        """追加一条记录到 WAL。sync=always 时每条都 fsync"""
        record = {
            "key" : cmd.key,
            "op" : cmd.op,
            "value" : cmd.value,
            "seq" : cmd.seq,
            "version" : cmd.version,
            "request_id" : cmd.request_id
        }
        with open(self.path, "a") as f:
            append_str  = json.dumps(record)
            
            append_str_bytes = append_str.encode("utf-8")

            crc = binascii.crc32(append_str_bytes) & 0xffffffff

            record["crc32"] = crc

            full = json.dumps(record)

            f.write(full+"\n")
            if self.sync_mode == "always":
                f.flush()
                await asyncio.to_thread(os.fsync, f.fileno())

    async def replay(self) -> list[Command]:
        """启动时重放 WAL，返回所有有效记录。遇损坏记录则截断"""
        result = []
        if not os.path.exists(self.path):
            return result

        # 一次性读入所有行，避免 for line in f 导致 f.tell() 不可用
        with open(self.path, "r") as f:
            all_lines = f.readlines()

        valid_bytes = 0
        for line in all_lines:
            stripped = line.strip()
            if not stripped:
                valid_bytes += len(line)
                continue

            try:
                data = json.loads(stripped)
            except json.JSONDecodeError:
                log.error("WAL 行 JSON 解析失败，截断到此", line=stripped[:50])
                break

            stored_crc = data.pop("crc32", None)
            if stored_crc is None:
                log.error("WAL 行缺少 crc32，截断到此")
                break

            recomputed = json.dumps(data).encode("utf-8")
            re_crc = binascii.crc32(recomputed) & 0xffffffff
            if re_crc != stored_crc:
                log.error("WAL crc32 不匹配，截断", stored=stored_crc, computed=re_crc)
                break

            # data 映射回cmd
            cmd = Command(data["op"],data["key"],data["value"],data["client_id"],data[seq],data[version],data[request_id])
            result.append(cmd)
            valid_bytes += len(line)

        # 有损坏行则截断文件
        file_size = os.path.getsize(self.path)
        if valid_bytes < file_size:
            with open(self.path, "ab") as fw:
                fw.truncate(valid_bytes)
            log.warn("WAL 已截断", path=self.path, valid_bytes=valid_bytes, original=file_size)

        return result    

    async def close(self):
        """关闭 WAL 文件"""
        # with 打开自动关闭
        pass