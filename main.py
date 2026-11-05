"""基于 AstrBot 插件接口管理 Bot 账号的回复限流。"""

import asyncio
import math
import re
from collections.abc import Mapping
from pathlib import Path

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import At, Plain
from astrbot.api.star import Context, Star, StarTools

from .delivery import DeliveryLimiter
from .quota import MAX_REPLIES, QuotaStore

PLUGIN_NAME = "astrbot_plugin_group_bot_limiter"
HELP = (
    "群聊 Bot 限流（仅当前群生效）\n"
    "/bot限流 标记 @用户 [次数]（也可填写用户 ID）\n"
    "/bot限流 取消 @用户\n"
    "/bot限流 列表\n"
    "/bot限流 状态 @用户\n"
    "/bot限流 重置 @用户\n"
    "AstrBot 管理员、群主及群管理员可管理。\n"
    "次数可填 0～10000，0 表示静默不回复；不填则跟随默认值。\n"
    "再次标记可更新次数，不清空使用计数。\n"
    "时长与默认次数在插件设置里调整，默认最近 60 分钟最多回复 3 轮。"
)


class GroupBotLimiterPlugin(Star):
    """防止被标记的 Bot 账号发起无限制的回复循环。"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.active = False
        self.store = None
        self.delivery = None

    async def initialize(self):
        root = Path(StarTools.get_data_dir(PLUGIN_NAME))
        root.mkdir(parents=True, exist_ok=True)
        self.store = QuotaStore(root / "quota.sqlite3")
        self.active = True
        self.delivery = DeliveryLimiter(self.store, self._limits, self._enabled)
        try:
            self.delivery.install(self.context)
        except Exception:
            await self.terminate()
            raise
        self.logger.info("Group bot reply limiter is ready.")

    def _enabled(self):
        return self.active and self.config.get("enabled", True) is True

    def _integer(self, key, default, maximum):
        value = self.config.get(key, default)
        if isinstance(value, bool):
            return default
        try:
            parsed = int(value)
        except (ValueError, TypeError, OverflowError):
            return default
        return min(maximum, max(1, parsed))

    def _limits(self):
        return (
            self._integer("window_minutes", 60, 525600) * 60,
            self._integer("max_replies", 3, MAX_REPLIES),
        )

    @staticmethod
    def _scope(event):
        return str(event.get_platform_id()), str(event.get_group_id())

    @filter.event_message_type(
        filter.EventMessageType.GROUP_MESSAGE, priority=2_000_000
    )
    async def limit_group_bots(self, event: AstrMessageEvent):
        """在其他插件处理器或模型之前拦截已用尽次数的账号。"""
        if self._enabled() and event.get_group_id() and event.get_sender_id():
            self.delivery.protect(event, self._scope(event), event.get_sender_id())

    async def _can_manage(self, event):
        """校验真实的群管理员身份，不依赖消息文本。"""
        if event.is_admin():
            return True
        if not self.config.get("allow_group_admins", True):
            return False
        if event.get_platform_name() == "aiocqhttp":
            raw = getattr(event.message_obj, "raw_message", None)
            sender = raw.get("sender") if isinstance(raw, Mapping) else None
            role = sender.get("role") if isinstance(sender, Mapping) else None
            if role in ("owner", "admin"):
                return True
            if role is not None:
                return False
            bot = getattr(event, "bot", None)
            if bot is None:
                return False
            try:
                arguments = {
                    "group_id": int(event.get_group_id()),
                    "user_id": int(event.get_sender_id()),
                    "no_cache": True,
                }
                if event.get_self_id():
                    arguments["self_id"] = event.get_self_id()
                member = await asyncio.wait_for(
                    bot.call_action("get_group_member_info", **arguments), timeout=4
                )
                return isinstance(member, Mapping) and member.get("role") in (
                    "owner",
                    "admin",
                )
            except Exception:
                self.logger.debug("Group administrator lookup failed; access denied.")
                return False
        try:
            group = await asyncio.wait_for(event.get_group(), timeout=4)
            user = str(event.get_sender_id())
            return group is not None and (
                user == str(getattr(group, "group_owner", None))
                or user
                in {str(item) for item in (getattr(group, "group_admins", None) or [])}
            )
        except Exception:
            self.logger.debug("Group administrator lookup failed; access denied.")
            return False

    @staticmethod
    def _arguments(event):
        # QQ 适配器会把被 @ 的显示名追加到 message_str。
        # 因此读取原始的 Plain 消息段；ID 来自实际的 At 组件。
        text = " ".join(
            component.text
            for component in event.get_messages()
            if isinstance(component, Plain)
        ).strip()
        match = re.match(r"^.*?(?:bot限流|机器人限流|botlimit)(?:\s+|$)", text)
        return text[match.end() :].split() if match else []

    @staticmethod
    def _target(event, arguments):
        candidates = {
            str(component.qq)
            for component in event.get_messages()
            if isinstance(component, At)
            and str(component.qq) not in ("all", "0", str(event.get_self_id()))
        }
        invalid = False
        for argument in arguments:
            value = argument.removeprefix("@")
            if value.isascii() and value.isdigit() and int(value) > 0:
                candidates.add(value)
            else:
                invalid = True
        if invalid or len(candidates) != 1:
            raise ValueError("请只指定一个用户：@对方，或填写用户 ID。")
        target = candidates.pop()
        if target == str(event.get_self_id()):
            raise ValueError("不能把当前 AstrBot 自己标记为限流对象。")
        return target

    @staticmethod
    def _mark_target(event):
        """按 @ 的边界解析参数。"""
        parts = []
        started = False
        for component in event.get_messages():
            if isinstance(component, Plain):
                text = component.text
                if not started:
                    match = re.search(r"(?:bot限流|机器人限流|botlimit)(?:\s+|$)", text)
                    if match is None:
                        continue
                    started = True
                    text = text[match.end() :]
                parts.extend(text.split())
            elif started:
                parts.append(component)
        arguments = parts[1:]  # 跳过已识别的「标记」动作。
        invalid_target = "请只指定一个用户：@对方，或填写用户 ID。"
        if not arguments:
            raise ValueError(invalid_target)
        first = arguments[0]
        if isinstance(first, At):
            target = str(first.qq)
        elif isinstance(first, str):
            target = first.removeprefix("@")
        else:
            raise ValueError(invalid_target)
        if not re.fullmatch(r"[0-9]{1,20}", target) or int(target) <= 0:
            raise ValueError(invalid_target)
        if target == str(event.get_self_id()):
            raise ValueError("不能把当前 AstrBot 自己标记为限流对象。")
        quota_arguments = arguments[1:]
        if not quota_arguments:
            return target, None
        if any(not isinstance(argument, str) for argument in quota_arguments):
            raise ValueError(invalid_target)
        if len(quota_arguments) != 1 or not re.fullmatch(
            r"[0-9]{1,5}", quota_arguments[0]
        ):
            raise ValueError(
                "次数应为 0～10000 的整数，0 表示静默不回复；"
                "用法：/bot限流 标记 @用户 [次数]，不填次数则跟随默认值。"
            )
        maximum = int(quota_arguments[0])
        if maximum > MAX_REPLIES:
            raise ValueError("次数应为 0～10000 的整数，0 表示静默不回复。")
        return target, maximum

    @filter.command("bot限流", alias={"机器人限流", "botlimit"}, priority=1_999_999)
    async def manage_bots(self, event: AstrMessageEvent):
        """管理当前群内被标记的 Bot，并查看其回复用量。"""
        try:
            async for result in self._management_results(event):
                yield result
        finally:
            # 在下游阶段完成结果投递后终止事件传播。
            event.stop_event()

    async def _management_results(self, event):
        if not event.get_group_id():
            yield event.plain_result("请在需要管理的群聊中使用 /bot限流。")
            return
        arguments = self._arguments(event)
        action = arguments[0].lower() if arguments else "帮助"
        if action in ("帮助", "help"):
            yield event.plain_result(HELP)
            return
        if not await self._can_manage(event):
            yield event.plain_result(
                "只有 AstrBot 管理员、群主或群管理员可以管理 Bot 限流。"
            )
            return
        scope = self._scope(event)
        window, limit = self._limits()
        if action in ("列表", "list"):
            marks = self.store.list_marks(scope)
            if not marks:
                yield event.plain_result("当前群还没有标记 Bot 用户。")
                return
            page = 1
            if len(arguments) > 1:
                try:
                    page = int(arguments[1])
                except ValueError:
                    page = 0
            pages = max(1, math.ceil(len(marks) / 20))
            if page < 1 or page > pages or len(arguments) > 2:
                yield event.plain_result(f"页码应为 1～{pages}，用法：/bot限流 列表 1")
                return
            lines = [f"本群已标记 {len(marks)} 个 Bot（第 {page}/{pages} 页）："]
            for mark in marks[(page - 1) * 20 : page * 20]:
                usage = self.store.status(scope, mark.user_id, window, limit)
                mode = "跟随默认" if mark.max_replies is None else "单独设置"
                if usage.max_replies == 0:
                    lines.append(f"{mark.user_id}：0 轮，静默不回复（{mode}）")
                    continue
                lines.append(
                    f"{mark.user_id}：{usage.used}/{usage.max_replies} 轮，"
                    f"剩余 {usage.remaining} 轮（{mode}）"
                )
            lines.append(f"统计窗口：最近 {window // 60} 分钟；超限静默。")
            yield event.plain_result("\n".join(lines))
            return
        aliases = {
            "标记": "mark",
            "mark": "mark",
            "取消": "unmark",
            "unmark": "unmark",
            "状态": "status",
            "status": "status",
            "重置": "reset",
            "reset": "reset",
        }
        action = aliases.get(action)
        if action is None:
            yield event.plain_result(HELP)
            return
        try:
            if action == "mark":
                target, maximum = self._mark_target(event)
            else:
                target = self._target(event, arguments[1:])
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return
        if action == "mark":
            created = self.store.mark(
                scope, target, event.get_sender_id(), max_replies=maximum
            )
            text = "已标记" if created else "已更新限额，仍标记"
            actual_limit = limit if maximum is None else maximum
            mode = "跟随默认值" if maximum is None else "单独设置"
            effect = "静默不回复。" if actual_limit == 0 else "超限后静默。"
            yield event.plain_result(
                f"{target} {text}为本群 Bot，最近 {window // 60} 分钟"
                f"最多回复 {actual_limit} 轮（{mode}）。{effect}已有使用计数保留。"
            )
        elif action == "unmark":
            removed = self.store.unmark(scope, target)
            yield event.plain_result(
                f"已取消 {target} 的 Bot 标记并清除其限流计数。"
                if removed
                else f"{target} 没有被标记为本群 Bot。"
            )
        else:
            usage = self.store.status(scope, target, window, limit)
            if not usage.marked:
                yield event.plain_result(f"{target} 没有被标记为本群 Bot。")
            elif action == "reset":
                self.store.reset_quota(scope, target)
                yield event.plain_result(
                    f"已清零 {target} 的使用次数，Bot 标记和次数上限保留。"
                )
            else:
                if usage.max_replies == 0:
                    yield event.plain_result(
                        f"{target}：次数上限为 0 轮，静默不回复，"
                        "不会随时间自动恢复；请重新标记并调整次数。"
                    )
                    return
                retry = (
                    f"约 {math.ceil(usage.retry_after)} 秒后恢复一个名额。"
                    if usage.remaining == 0
                    else ""
                )
                yield event.plain_result(
                    f"{target}：最近 {window // 60} 分钟"
                    f"已回复 {usage.used}/{usage.max_replies} 轮，"
                    f"剩余 {usage.remaining} 轮。{retry}"
                )

    async def terminate(self):
        self.active = False
        if self.delivery is not None:
            self.delivery.uninstall()
            self.delivery = None
        if self.store is not None:
            self.store.close()
            self.store = None
