"""
简易日志系统：支持色彩打印、级别过滤、可选文件重定向。

用法:
    from common import log

    log.init(level="info")               # 设置最低级别
    log.info("server 启动", port=8000)    # 绿色
    log.warn("seq 回退", client="c1")    # 黄色
    log.error("连接断开", addr=addr)     # 红色
    log.debug("收到消息", msg=req)       # 蓝色，level=debug 时才显示
"""

import sys
from datetime import datetime

# ── 全局配置 ──────────────────────────────────────────

_level = "info"      # 最低输出级别
_file = None         # 文件路径，None = 不写文件
_color = True        # 控制台是否带 ANSI 颜色
_fh = None           # 文件句柄，惰性打开

_LEVEL_RANK = {
    "debug": 0,
    "info":  1,
    "warn":  2,
    "error": 3,
}

_COLORS = {
    "DEBUG": "\033[34m",   # 蓝
    "INFO":  "\033[32m",   # 绿
    "WARN":  "\033[33m",   # 黄
    "ERROR": "\033[31m",   # 红
    "RESET": "\033[0m",
    "BOLD":  "\033[1m",
}


# ── 公开 API ──────────────────────────────────────────

def init(level: str = "info", file: str = None, color: bool = True):
    """配置日志系统。"""
    global _level, _file, _color, _fh
    _level = level
    _file = file
    _color = color
    if _fh is not None:
        _fh.close()
        _fh = None


def debug(msg: str, **kwargs):
    _log("DEBUG", msg, **kwargs)


def info(msg: str, **kwargs):
    _log("INFO", msg, **kwargs)


def warn(msg: str, **kwargs):
    _log("WARN", msg, **kwargs)


def error(msg: str, **kwargs):
    _log("ERROR", msg, **kwargs)


# ── 内部实现 ──────────────────────────────────────────

def _log(level: str, msg: str, **kwargs):
    global _fh

    # 级别过滤（统一小写查找）
    if _LEVEL_RANK.get(level.lower(), 0) < _LEVEL_RANK.get(_level.lower(), 0):
        return

    # 时间
    now = datetime.now().strftime("%H:%M:%S")

    # 拼接额外参数
    extra = " ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""

    # 纯文本行（文件用）
    plain = f"[{level}] {now} {msg}"
    if extra:
        plain += " " + extra

    # 写入文件
    if _file is not None:
        if _fh is None:
            _fh = open(_file, "a", encoding="utf-8")
        _fh.write(plain + "\n")
        _fh.flush()

    # 控制台输出
    color_code = _COLORS.get(level, "")
    reset = _COLORS["RESET"]
    if _color and color_code:
        line = f"{color_code}[{level}]{reset} {_COLORS['BOLD']}{now}{reset} {msg}"
    else:
        line = f"[{level}] {now} {msg}"

    if extra:
        line += " " + extra

    print(line, file=sys.stderr)
