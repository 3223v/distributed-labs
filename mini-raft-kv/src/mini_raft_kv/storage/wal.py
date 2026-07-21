import os
import json
import asyncio
import binascii
from mini_raft_kv.common import log
from mini_raft_kv.common.command import Command
from mini_raft_kv.common.config import WalConfig

class WAL:
    def __init__(self, cfg :WalConfig):
        """path: wal 文件路径，如 data/wal.log"""
        self.path = cfg.path
        self.sync_mode = cfg.sync_mode
        self.batch_count = cfg.batch_count
        self.bc = cfg.batch_count
        self.in_ms = cfg.batch_interval_ms
        self.fd = None
        self.unsynced = 0
        self.running = False
    
    async def start(self):
        self.fd = open(self.path, "a") 
        self.running  = True
        if self.sync_mode == "timer":
            self.timer_task = asyncio.create_task(self.periodic_sync())

    async def periodic_sync(self):
        """后台协程：每 batch_interval_ms 毫秒 fsync 一次（兜底）"""
        while self.running:
            await asyncio.sleep(self.in_ms / 1000)   # 比如 100ms
            if self.unsynced > 0:
                self.fd.flush()
                await asyncio.to_thread(os.fsync, self.fd.fileno())
                self.unsynced = 0
                log.debug("batch timer fsync")

    async def append(self, cmd: Command) -> None:
        """追加一条记录到 WAL。sync=always 时每条都 fsync"""
        self.bc -=1
        record = {
            "key" : cmd.key,
            "op" : cmd.op,
            "value" : cmd.value,
            "client_id" : cmd.client_id,
            "seq" : cmd.seq,
            "version" : cmd.version,
            "request_id" : cmd.request_id
        }
        append_str  = json.dumps(record)
        
        append_str_bytes = append_str.encode("utf-8")

        crc = binascii.crc32(append_str_bytes) & 0xffffffff

        record["crc32"] = crc

        full = json.dumps(record)

        self.fd.write(full+"\n")
        if self.sync_mode == "always":
            self.fd.flush()
            await asyncio.to_thread(os.fsync, self.fd.fileno())
        if self.sync_mode in ("batch", "timer"):
            self.unsynced += 1
            if self.bc <= 0:
                self.fd.flush()
                self.reset_bc()
                await asyncio.to_thread(os.fsync, self.fd.fileno())
                self.unsynced = 0
            
    def reset_bc(self):
        self.bc = self.batch_count

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
            cmd = Command(data["op"],data["key"],data["value"],data["client_id"],data["seq"],data["version"],data["request_id"])
            result.append(cmd)
            valid_bytes += len(line)

        # 有损坏行则截断文件
        file_size = os.path.getsize(self.path)
        if valid_bytes < file_size:
            with open(self.path, "ab") as fw:
                fw.truncate(valid_bytes)
            log.warn("WAL 已截断", path=self.path, valid_bytes=valid_bytes, original=file_size)

        return result    

    async def stop(self):
        """关闭：停定时器 → 最后 fsync → 关文件"""
        self.running = False
        if hasattr(self, "timer_task") and self.timer_task:
            self.timer_task.cancel()
            try:
                await self.timer_task
            except asyncio.CancelledError:
                pass
        if self.fd:
            if self.unsynced > 0:
                self.fd.flush()
                await asyncio.to_thread(os.fsync, self.fd.fileno())
                self.unsynced = 0
            self.fd.close()
            self.fd = None