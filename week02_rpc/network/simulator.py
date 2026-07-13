from rpc import server
import random

class Network:
    """管理所有节点之间的消息传递，支持注入 delay 和 drop"""

    def __init__(self):
        self._rules = {}  # (src, dst) -> {"delay_ms": int, "drop_rate": float}

    def add_rule(self, src: str, dst: str, delay_ms: int = 0, drop_rate: float = 0.0):
        """设置 src → dst 的链路属性"""
        self._rules[(src, dst)] = {"delay_ms": delay_ms, "drop_rate": drop_rate}

    def reset(self):
        """清除所有规则"""
        self._rules.clear()

    async def send(self, src: str, dst: str, message: bytes) -> Optional[bytes]:
        """模拟发送消息：可能延迟、可能丢弃"""
        rule = self._rules.get((src, dst), {})
        delay_ms = rule.get("delay_ms", 0)
        drop_rate = rule.get("drop_rate", 0.0)

        # 丢包
        if random.random() < drop_rate:
            return None

        # 延迟
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

        return message  # 到达