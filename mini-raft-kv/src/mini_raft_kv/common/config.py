"""
配置系统：从 YAML 文件读取，提供默认值。

用法:
    from mini_raft_kv.common.config import load_config
    cfg = load_config("config/local.yaml")
    print(cfg.server.host, cfg.server.port)
"""

import os


class ServerConfig:
    def __init__(self, data: dict):
        self.host = data.get("host", "127.0.0.1")
        self.port = data.get("port", 8000)
        self.v = data.get("v", 1)

    def __repr__(self):
        return f"ServerConfig(host={self.host}, port={self.port}, v={self.v})"


class ClientConfig:
    def __init__(self, data: dict):
        self.host = data.get("host", "127.0.0.1")
        self.port = data.get("port", 8000)
        self.timeout = data.get("timeout", 5.0)
        self.max_retries = data.get("max_retries", 3)
        self.v = data.get("v", 1)

    def __repr__(self):
        return f"ClientConfig(host={self.host}, port={self.port}, v={self.v})"


class WalConfig:
    def __init__(self, data: dict):
        self.path = data.get("path", "data/wal.log")
        self.sync_mode = data.get("sync_mode", "always")
        self.batch_count = data.get("batch_count",10)
        self.batch_interval_ms = data.get("batch_interval_ms",1000)

    def __repr__(self):
        return f"WalConfig(path={self.path}, sync_mode={self.sync_mode})"


class LogConfig:
    def __init__(self, data: dict):
        self.level = data.get("level", "info")
        self.file = data.get("file", None)
        self.color = data.get("color", True)

    def __repr__(self):
        return f"LogConfig(level={self.level})"


class Config:
    def __init__(self, data: dict):
        self.server = ServerConfig(data.get("server", {}))
        self.client = ClientConfig(data.get("client", {}))
        self.wal = WalConfig(data.get("wal", {}))
        self.log = LogConfig(data.get("log", {}))

    def __repr__(self):
        return f"Config(server={self.server}, client={self.client})"


def load_config(path: str) -> Config:
    """从 YAML 文件加载配置。对于简单 YAML，逐行解析，避免引入第三方库。"""
    if not os.path.exists(path):
        return Config({})

    data = _parse_simple_yaml(path)
    return Config(data)


def _parse_simple_yaml(path: str) -> dict:
    """极简 YAML 解析器：只支持嵌套 dict、字符串、数字、null。"""
    with open(path, "r") as f:
        lines = f.readlines()

    root = {}
    stack = [(root, -1)]  # (current_dict, indent)

    for line in lines:
        stripped = line.rstrip()
        if not stripped or stripped.startswith("#"):
            continue

        # 计算缩进
        indent = len(line) - len(line.lstrip())

        # 回到合适的层级
        while stack and indent <= stack[-1][1]:
            stack.pop()

        current_dict = stack[-1][0]

        # 解析 key: value
        if ":" in stripped:
            key, _, value_str = stripped.partition(":")
            key = key.strip()
            value_str = value_str.strip()
            # 去掉行内注释（# 及之后内容）
            if "#" in value_str:
                value_str = value_str.split("#")[0].strip()

            if value_str in ("", "null", "~"):
                value = None
            elif value_str == "true":
                value = True
            elif value_str == "false":
                value = False
            elif _is_number(value_str):
                value = float(value_str)
                if value == int(value):
                    value = int(value)
            else:
                # 字符串，去掉引号
                value = value_str.strip('"').strip("'")

            # 如果 value 为空且没有引号，说明是嵌套对象
            if value_str == "" and not (stripped.rstrip().endswith('"') or stripped.rstrip().endswith("'")):
                nested = {}
                current_dict[key] = nested
                stack.append((nested, indent))
            else:
                current_dict[key] = value

    return root


def _is_number(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False
