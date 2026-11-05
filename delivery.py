"""在事件投递边界处应用回复配额，无需修改核心文件。"""

from __future__ import annotations

import functools
import uuid
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from astrbot.core.pipeline.scheduler import PipelineScheduler
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.message_type import MessageType

from .quota import QuotaStore, Scope

_CURRENT_EVENT: ContextVar[Any | None] = ContextVar(
    "group_bot_limiter_current_event", default=None
)
_GUARD_KEY = "_group_bot_limiter_delivery_guard"
_PLUGIN_NAME = "astrbot_plugin_group_bot_limiter"
_CONTROL_TYPES = {
    "at",
    "atall",
    "reply",
    "break",
    "reasoning",
    "thinking",
    "think",
    "reasoning_content",
    "streaming_finish",
    "streaming_finished",
    "control",
}


def _type_name(value: Any) -> str:
    """提取组件类型名（统一转小写）。"""
    value = getattr(value, "value", value)
    return str(value or "").rsplit(".", 1)[-1].lower()


def has_payload(message: Any) -> bool:
    """判断消息链是否包含用户可见的实际回复内容。"""
    if message is None:
        return False
    if isinstance(message, str):
        return bool(message.strip())
    if isinstance(message, (list, tuple)):
        return any(has_payload(component) for component in message)
    if isinstance(message, dict):
        kind = _type_name(message.get("type"))
        if kind in _CONTROL_TYPES:
            return False
        if kind in {"plain", "text"}:
            data = message.get("data", {})
            text = message.get("text", data.get("text", ""))
            return bool(str(text or "").strip())
        return bool(kind)
    kind = _type_name(getattr(message, "type", None))
    if kind in _CONTROL_TYPES:
        return False
    if hasattr(message, "chain"):
        return has_payload(message.chain)
    if kind in {"plain", "text"} or hasattr(message, "text"):
        return bool(str(getattr(message, "text", "") or "").strip())
    return bool(kind)


@dataclass
class _DeliveryGuard:
    owner: DeliveryLimiter
    event: Any                      # 当前绑定的那条群消息事件对象
    scope: Scope
    user_id: str
    event_id: str                   # 这条群消息的唯一 ID（用来防重）
    accepted: bool = False          # 【状态章 1：已交费】默认 False，交过 1 次费就盖章变成 True
    blocked: bool = False           # 【状态章 2：已拉黑】默认 False，一旦被拒就盖章变成 True

    def admit(self) -> bool:
        """发送前计费一次，包括结果不确定的失败尝试。"""
        if not self.owner._active or not self.owner.enabled():
            return True
        if self.blocked:
            return False
        if self.accepted:
            return True
        window, maximum = self.owner.limits()
        decision = self.owner.store.reserve(
            self.scope, self.user_id, self.event_id, window, maximum
        )
        if decision.duplicate or not decision.allowed:
            self.blocked = True
            self.event.stop_event()
            return False
        if not decision.marked:
            return True
        if not self.owner.store.confirm(decision.token):
            self.blocked = True
            self.event.stop_event()
            return False
        self.accepted = True
        return True


class DeliveryLimiter:
    """限制被标记的群账号，同时保留已有的发送包装器。

    每个入站事件最多消耗一个配额名额。预留与确认在首次真正发送前紧邻执行，
    中间不存在 await。网络失败同样会消耗该名额，因为超时投递结果不确定，
    无限重试会使防循环机制失效。
    """

    def __init__(
        self,
        store: QuotaStore,
        limits: Callable[[], tuple[float, int]],
        enabled: Callable[[], bool],
    ) -> None:
        self.store = store
        self.limits = limits
        self.enabled = enabled
        self._active = True
        self._context: Any | None = None
        self._original_execute: Any | None = None
        self._execute_wrapper: Any | None = None
        self._original_context_send: Any | None = None
        self._context_send_wrapper: Any | None = None
        self._context_had_instance_send = False

    @staticmethod
    def _guard(event: Any) -> _DeliveryGuard | None:
        getter = getattr(event, "get_extra", None)
        return getter(_GUARD_KEY) if callable(getter) else None

    def protect(self, event: Any, scope: Scope, user_id: str) -> bool:
        """提前检查并挂载按事件生效的发送守卫，但不消耗配额。

        参数：
            event: 收到的群消息事件。
            scope: 平台实例 ID 与实际群 ID。
            user_id: 消息发送者 ID。

        返回：
            是否允许继续正常处理该事件。
        """
        if not self._active or not self.enabled() or not event.get_group_id():
            return True
        existing = self._guard(event)
        if existing is not None and existing.owner is self:
            return not existing.blocked
        window, maximum = self.limits()
        status = self.store.status(scope, user_id, window, maximum)
        if not status.marked:
            return True
        message_id = getattr(getattr(event, "message_obj", None), "message_id", None)
        event_id = str(message_id).strip() if message_id is not None else ""
        guard = _DeliveryGuard(
            self, event, scope, user_id, event_id or uuid.uuid4().hex
        )
        event.set_extra(_GUARD_KEY, guard)
        if not status.remaining:
            guard.blocked = True
            event.stop_event()
            return False
        original_send = event.send

        @functools.wraps(original_send)
        async def limited_send(message: Any, *args: Any, **kwargs: Any) -> Any:
            if not self._active or not self.enabled():
                return await original_send(message, *args, **kwargs)
            if not has_payload(message):
                return None
            if not guard.admit():
                return False
            return await original_send(message, *args, **kwargs)

        event.send = limited_send
        if event.get_platform_name() != "aiocqhttp":
            # QQ 的兜底流式输出已经通过该实例的 send() 发送。
            event.set_extra("enable_streaming", False)
        return True

    @staticmethod
    def _same_target(event: Any, session: Any) -> bool:
        """匹配完全相同的隔离来源，或同一平台下的实际群聊。"""
        try:
            if str(session) == str(event.unified_msg_origin):
                return True
            target = (
                MessageSession.from_str(session)
                if isinstance(session, str)
                else session
            )
            kind = getattr(target, "message_type", None)
            if kind != MessageType.GROUP_MESSAGE and _type_name(kind) != "groupmessage":
                return False
            platform = getattr(target, "platform_id", None) or getattr(
                target, "platform_name", None
            )
            return str(platform) == str(event.get_platform_id()) and str(
                getattr(target, "session_id", "")
            ) == str(event.get_group_id())
        except (AttributeError, TypeError, ValueError):
            return False

    @staticmethod
    def _enabled_in_session(context: Any, event: Any) -> bool:
        """在核心解析处理器之前，遵循按会话配置的插件排除规则。"""
        get_config = getattr(context, "get_config", None)
        if not callable(get_config):
            return True
        try:
            config = get_config(umo=event.unified_msg_origin)
            plugins = config.get("plugin_set", ["*"])
            return plugins == ["*"] or _PLUGIN_NAME in plugins
        except Exception:
            # 原有的插件过滤处理器仍作为安全的兜底。
            return False

    def _protect_early(self, context: Any, event: Any) -> bool:
        """连唤醒和命令过滤器产生的错误回复也一并拦截。"""
        if not self.enabled() or not self._enabled_in_session(context, event):
            return True
        getters = [
            getattr(event, name, None)
            for name in ("get_platform_id", "get_group_id", "get_sender_id")
        ]
        if not all(callable(getter) for getter in getters):
            return True
        platform_id, group_id, user_id = (getter() for getter in getters)
        if not platform_id or not group_id or not user_id:
            return True
        get_type = getattr(event, "get_message_type", None)
        if callable(get_type) and _type_name(get_type()) != "groupmessage":
            return True
        return self.protect(event, (str(platform_id), str(group_id)), str(user_id))

    def install(self, context: Any) -> None:
        """安装任务级的投递归属，并保留原有的包装器调用链。"""
        if self._execute_wrapper is not None:
            self._active = True
            return
        self._active = True
        self._context = context
        original_execute = PipelineScheduler.execute
        self._original_execute = original_execute

        @functools.wraps(original_execute)
        async def execute(scheduler: Any, event: Any, *args: Any, **kwargs: Any) -> Any:
            if not self._active:
                return await original_execute(scheduler, event, *args, **kwargs)
            token = _CURRENT_EVENT.set(event)
            try:
                if not self._protect_early(context, event):
                    return None
                return await original_execute(scheduler, event, *args, **kwargs)
            finally:
                _CURRENT_EVENT.reset(token)

        self._execute_wrapper = execute
        PipelineScheduler.execute = execute
        original_send = context.send_message
        self._original_context_send = original_send
        self._context_had_instance_send = "send_message" in vars(context)

        @functools.wraps(original_send)
        async def send_message(
            session: Any, message_chain: Any, *args: Any, **kwargs: Any
        ) -> Any:
            event = _CURRENT_EVENT.get()
            if (
                self._active
                and self.enabled()
                and event is not None
                and self._same_target(event, session)
            ):
                guard = self._guard(event)
                if guard is not None and guard.owner is self:
                    if not has_payload(message_chain):
                        return True
                    if not guard.admit():
                        return False
            return await original_send(session, message_chain, *args, **kwargs)

        self._context_send_wrapper = send_message
        context.send_message = send_message

    def uninstall(self) -> None:
        """停用旧包装器，仅在仍归本插件所有时恢复原方法。"""
        self._active = False
        if PipelineScheduler.execute is self._execute_wrapper:
            PipelineScheduler.execute = self._original_execute
        context = self._context
        if (
            context is not None
            and getattr(context, "send_message", None) is self._context_send_wrapper
        ):
            if self._context_had_instance_send:
                context.send_message = self._original_context_send
            else:
                delattr(context, "send_message")
        self._execute_wrapper = None
        self._context_send_wrapper = None
