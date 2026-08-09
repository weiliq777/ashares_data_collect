from data_collect.utils.notify import ensure_ding_prefix
from data_collect.config import get_dingtalk_config


def test_ensure_ding_prefix_adds_prefix():
    prefix = get_dingtalk_config().get("message_prefix", "白白胖胖说")
    assert ensure_ding_prefix("测试消息").startswith(prefix)


def test_ensure_ding_prefix_keeps_existing():
    prefix = get_dingtalk_config().get("message_prefix", "白白胖胖说")
    message = f"{prefix}：原消息"
    assert ensure_ding_prefix(message) == message
