# WAL 单元测试：append / replay / checksum / 尾部截断
import asyncio, json, os, sys, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from mini_raft_kv.storage.wal import WAL
from mini_raft_kv.common.command import Command


class _FakeWalCfg:
    def __init__(self, path, sync_mode="always"):
        self.path = path
        self.sync_mode = sync_mode
        self.batch_count = 10
        self.batch_interval_ms = 1000


def _cmd(op, key, value, client_id="t1", seq=1):
    return Command(op, key, value, client_id, seq, version=None, request_id=1)


def test_append_and_replay():
    """正常 append 后 replay 可恢复"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as f:
        path = f.name

    try:
        wal = WAL(_FakeWalCfg(path))
        asyncio.run(wal.start())
        asyncio.run(wal.append(_cmd("put", "k1", "v1")))
        asyncio.run(wal.append(_cmd("put", "k2", "v2")))
        asyncio.run(wal.stop())

        records = asyncio.run(wal.replay())
        assert len(records) == 2, f"expected 2 records, got {len(records)}"
        assert records[0].key == "k1" and records[0].op == "put"
        assert records[1].key == "k2" and records[1].value == "v2"
        print("PASS test_append_and_replay")
    finally:
        os.unlink(path)


def test_corrupted_tail_truncation():
    """WAL 尾部加损坏行后 replay 应截断并恢复前面合法记录"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as f:
        path = f.name

    try:
        wal = WAL(_FakeWalCfg(path))
        asyncio.run(wal.start())
        asyncio.run(wal.append(_cmd("put", "good", "data")))
        asyncio.run(wal.stop())

        # 手动追加损坏行
        with open(path, "a") as fh:
            fh.write('{"key":"bad","op":"put","crc32":0}\n')

        records = asyncio.run(wal.replay())
        assert len(records) == 1, f"expected 1 record after truncation, got {len(records)}"
        assert records[0].key == "good"
        print("PASS test_corrupted_tail_truncation")
    finally:
        os.unlink(path)


def test_empty_wal():
    """不存在的 WAL 文件 replay 返回空列表"""
    wal = WAL(_FakeWalCfg("/tmp/no_such_wal_file_42.log"))
    records = asyncio.run(wal.replay())
    assert records == []
    print("PASS test_empty_wal")


if __name__ == "__main__":
    test_append_and_replay()
    test_corrupted_tail_truncation()
    test_empty_wal()
