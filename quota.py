"""为管理员标记的 Bot 账号提供持久化、按作用域隔离的回复配额。"""

from __future__ import annotations

import math
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

Scope = tuple[str, str]
MAX_REPLIES = 10000


@dataclass(frozen=True)
class BotMark:
    """在某个平台和群聊中被标记为 Bot 的账号。"""

    user_id: str
    actor: str                          # 是哪个管理员标记的
    marked_at: float                    # 标记时间戳
    max_replies: int | None = None      # None 表示跟随全局默认，整数表示独立上限


@dataclass(frozen=True)
class QuotaStatus:
    """当前用量；待确认的回复同样占用配额名额。"""

    marked: bool
    used: int                  # 数据库里已确认发送的次数
    pending: int               # 内存里正在处理、待确认的预占次数
    remaining: int             # 剩余可用次数 = max(0, max_replies - used - pending)
    retry_after: float         # 距离空出下一个名额还要等多少秒
    window_seconds: float
    max_replies: int


@dataclass(frozen=True)
class QuotaDecision:
    """准入结果，以及待确认或释放的预留名额。"""

    allowed: bool                 # 是否放行
    marked: bool                  # 是否是被标记的 Bot
    token: str | None             # 预占凭证（放行时发一张门票 token，后续凭票 confirm 或 release）
    remaining: int
    retry_after: float
    used: int
    pending: int
    duplicate: bool = False       # 是否是同一个 event_id 的重复检查


@dataclass(frozen=True)
class _Pending:
    """存在内存字典里的预占门票详情"""

    token: str
    scope: Scope
    user_id: str
    event_id: str
    expires_at: float


class QuotaStore:
    """串行化配额判定，并仅持久化已确认的回复。

    作用域严格为 ``(platform_id, group_id)``。预留名额在耗时操作开始前创建，
    在事件首次产生实际回复时确认一次，处理结束仍未回复时释放。同一事件的所有
    分段和工具回复共用一个预留名额。

    待确认的预留有意仅保存在内存中。重新加载插件不会计入未得到回复的请求；
    已确认的用量和事件去重则会跨重启保留。若事件在首次回复前预留已过期，
    调用方应终止该事件，而不是在确认失败后继续发送。
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        clock: Callable[[], float] = time.time,
        pending_ttl: float = 1800,
        retention_seconds: float = 366 * 86400,
    ) -> None:
        """打开一个私有的 SQLite 存储。

        参数：
            db_path: 插件外部数据目录中的数据库文件。
            clock: 墙上时钟，可替换以便进行确定性测试。
            pending_ttl: 未被应答的预留的最长存活时间。
            retention_seconds: 为可配置时间窗口保留的历史记录。请求的时间窗口
                不得超过该值，以确保清理不会悄悄改变有效的配额判定。
        """
        self._pending_ttl = self._positive_number(pending_ttl, "pending_ttl")
        self._retention = self._positive_number(retention_seconds, "retention_seconds")
        self._clock = clock
        self._lock = threading.RLock()
        self._pending: dict[str, _Pending] = {}
        self._event_tokens: dict[tuple[Scope, str, str], str] = {}
        self._last_prune: float | None = None
        self._closed = False
        path = Path(db_path)
        if str(db_path) != ":memory:":
            path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(db_path), check_same_thread=False, timeout=10)
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS bot_marks (
                platform_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                actor TEXT NOT NULL,
                marked_at REAL NOT NULL,
                max_replies INTEGER CHECK (
                    max_replies IS NULL OR max_replies BETWEEN 0 AND 10000
                ),
                PRIMARY KEY (platform_id, group_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS reply_usage (
                platform_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                replied_at REAL NOT NULL,
                PRIMARY KEY (platform_id, group_id, user_id, event_id),
                FOREIGN KEY (platform_id, group_id, user_id)
                    REFERENCES bot_marks(platform_id, group_id, user_id)
                    ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS reply_usage_window
                ON reply_usage(platform_id, group_id, user_id, replied_at);
            CREATE INDEX IF NOT EXISTS reply_usage_retention ON reply_usage(replied_at);
            """
        )

    @staticmethod
    def _positive_number(value: float, name: str) -> float:
        if isinstance(value, bool):
            raise ValueError(f"{name} must be a positive finite number")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be a positive finite number") from exc
        if not math.isfinite(parsed) or parsed <= 0:
            raise ValueError(f"{name} must be a positive finite number")
        return parsed

    @staticmethod
    def _identity(value: str, name: str) -> str:
        if value is None or not str(value).strip():
            raise ValueError(f"{name} must not be empty")
        return str(value).strip()

    @classmethod
    def _scope(cls, scope: Scope) -> Scope:
        if not isinstance(scope, tuple) or len(scope) != 2:
            raise ValueError("scope must be a (platform_id, group_id) tuple")
        return (
            cls._identity(scope[0], "platform_id"),
            cls._identity(scope[1], "group_id"),
        )

    def _limits(self, window_seconds: float, max_replies: int) -> tuple[float, int]:
        window = self._positive_number(window_seconds, "window_seconds")
        if window > self._retention:
            raise ValueError("window_seconds exceeds the configured history retention")
        if isinstance(max_replies, bool) or not isinstance(max_replies, int):
            raise ValueError("max_replies must be a positive integer")
        if max_replies < 1:
            raise ValueError("max_replies must be a positive integer")
        return window, max_replies

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("QuotaStore is closed")

    def _drop_pending(self, token: str) -> _Pending | None:
        pending = self._pending.pop(token, None)
        if pending is not None:
            self._event_tokens.pop(
                (pending.scope, pending.user_id, pending.event_id), None
            )
        return pending

    def _cleanup(self, now: float) -> None:
        for token, pending in tuple(self._pending.items()):
            if pending.expires_at <= now:
                self._drop_pending(token)
        if self._last_prune is None or now - self._last_prune >= 60:
            with self._db:
                self._db.execute(
                    "DELETE FROM reply_usage WHERE replied_at <= ?",
                    (now - self._retention,),
                )
            self._last_prune = now

    def _is_marked(self, scope: Scope, user_id: str) -> bool:
        return (
            self._db.execute(
                "SELECT 1 FROM bot_marks "
                "WHERE platform_id = ? AND group_id = ? AND user_id = ?",
                (*scope, user_id),
            ).fetchone()
            is not None
        )

    def _status(
        self, scope: Scope, user_id: str, window: float, limit: int, now: float
    ) -> QuotaStatus:
        mark = self._db.execute(
            "SELECT max_replies FROM bot_marks "
            "WHERE platform_id = ? AND group_id = ? AND user_id = ?",
            (*scope, user_id),
        ).fetchone()
        if mark is None:
            return QuotaStatus(False, 0, 0, limit, 0, window, limit)
        if mark[0] is not None:
            limit = mark[0]
        committed = self._db.execute(
            "SELECT replied_at FROM reply_usage "
            "WHERE platform_id = ? AND group_id = ? AND user_id = ? "
            "AND replied_at > ?",
            (*scope, user_id, now - window),
        ).fetchall()
        pending = [
            item
            for item in self._pending.values()
            if item.scope == scope and item.user_id == user_id
        ]
        total = len(committed) + len(pending)
        retry_after = 0.0
        # 次数为 0 的配额保持禁用，直到管理员修改上限。
        if limit > 0 and total >= limit:
            expirations = sorted(
                [row[0] + window for row in committed]
                + [item.expires_at for item in pending]
            )
            retry_after = max(0.0, expirations[total - limit] - now)
        return QuotaStatus(
            True,                  # 1. marked: 确实是被标记的 Bot
            len(committed),        # 2. used: 数据库里窗口内已确认的次数
            len(pending),          # 3. pending: 内存里待确认的次数
            max(0, limit - total), # 4. remaining: 剩余可用次数（防负数，最小为 0）
            retry_after,           # 5. retry_after: 还要等多少秒恢复 1 个名额
            window,                # 6. window_seconds: 当前窗口秒数
            limit,                 # 7. max_replies: 当前生效的次数上限
        )

    def mark(
        self,
        scope: Scope,
        user_id: str,
        actor: str = "",
        *,
        max_replies: int | None = None,
    ) -> bool:
        """创建或更新标记且不清空用量；None 表示跟随默认值。"""
        scope = self._scope(scope)
        user_id = self._identity(user_id, "user_id")
        if max_replies is not None and (
            isinstance(max_replies, bool)
            or not isinstance(max_replies, int)
            or not 0 <= max_replies <= MAX_REPLIES
        ):
            raise ValueError(f"max_replies must be an integer from 0 to {MAX_REPLIES}")
        with self._lock:
            self._check_open()
            with self._db:
                created = not self._is_marked(scope, user_id)
                self._db.execute(
                    "INSERT INTO bot_marks "
                    "(platform_id, group_id, user_id, actor, marked_at, max_replies) "
                    "VALUES (?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(platform_id, group_id, user_id) DO UPDATE SET "
                    "max_replies = excluded.max_replies",
                    (*scope, user_id, str(actor), self._clock(), max_replies),
                )
            if max_replies == 0:
                self._discard_user_pending(scope, user_id)
            return created

    def _discard_user_pending(self, scope: Scope, user_id: str) -> int:
        tokens = [
            token
            for token, pending in self._pending.items()
            if pending.scope == scope and pending.user_id == user_id
        ]
        for token in tokens:
            self._drop_pending(token)
        return len(tokens)

    def unmark(self, scope: Scope, user_id: str) -> bool:
        """移除标记及全部用量，并使未使用的令牌失效。"""
        scope = self._scope(scope)
        user_id = self._identity(user_id, "user_id")
        with self._lock:
            self._check_open()
            self._discard_user_pending(scope, user_id)
            with self._db:
                result = self._db.execute(
                    "DELETE FROM bot_marks "
                    "WHERE platform_id = ? AND group_id = ? AND user_id = ?",
                    (*scope, user_id),
                )
            return result.rowcount > 0

    def list_marks(self, scope: Scope) -> list[BotMark]:
        """仅列出指定平台和群聊内的标记。"""
        scope = self._scope(scope)
        with self._lock:
            self._check_open()
            rows = self._db.execute(
                "SELECT user_id, actor, marked_at, max_replies FROM bot_marks "
                "WHERE platform_id = ? AND group_id = ? ORDER BY user_id",
                scope,
            ).fetchall()
            return [BotMark(*row) for row in rows]

    def reserve(
        self,
        scope: Scope,
        user_id: str,
        event_id: str,
        window_seconds: float,
        max_replies: int,
    ) -> QuotaDecision:
        """原子地准入事件，或报告配额名额何时可能释放。"""
        scope = self._scope(scope)
        user_id = self._identity(user_id, "user_id")
        event_id = self._identity(event_id, "event_id")
        window, limit = self._limits(window_seconds, max_replies)
        with self._lock:
            self._check_open()
            now = self._clock()
            self._cleanup(now)
            current = self._status(scope, user_id, window, limit, now)
            if not current.marked:
                return QuotaDecision(True, False, None, limit, 0, 0, 0)
            if current.max_replies == 0:
                return QuotaDecision(
                    False, True, None, 0, 0, current.used, current.pending
                )
            event_key = (scope, user_id, event_id)
            token = self._event_tokens.get(event_key)
            committed = self._db.execute(
                "SELECT 1 FROM reply_usage WHERE platform_id = ? "
                "AND group_id = ? AND user_id = ? AND event_id = ?",
                (*scope, user_id, event_id),
            ).fetchone()
            if token is not None or committed is not None:
                return QuotaDecision(
                    True,
                    True,
                    token,
                    current.remaining,
                    current.retry_after,
                    current.used,
                    current.pending,
                    True,
                )
            if current.remaining == 0:
                return QuotaDecision(
                    False,
                    True,
                    None,
                    0,
                    current.retry_after,
                    current.used,
                    current.pending,
                )
            token = uuid.uuid4().hex
            self._pending[token] = _Pending(
                token, scope, user_id, event_id, now + self._pending_ttl
            )
            self._event_tokens[event_key] = token
            updated = self._status(scope, user_id, window, limit, now)
            return QuotaDecision(
                True,
                True,
                token,
                updated.remaining,
                updated.retry_after,
                updated.used,
                updated.pending,
            )

    def confirm(self, token: str | None) -> bool:
        """仅提交首次实际回复；对失效令牌返回 False。"""
        with self._lock:
            self._check_open()
            now = self._clock()
            self._cleanup(now)
            pending = self._pending.get(token) if token is not None else None
            if pending is None or not self._is_marked(pending.scope, pending.user_id):
                return False
            with self._db:
                result = self._db.execute(
                    "INSERT OR IGNORE INTO reply_usage VALUES (?, ?, ?, ?, ?)",
                    (*pending.scope, pending.user_id, pending.event_id, now),
                )
            self._drop_pending(pending.token)
            return result.rowcount > 0

    def release(self, token: str | None) -> bool:
        """释放未应答的预留，不删除已确认的用量。"""
        with self._lock:
            self._check_open()
            return token is not None and self._drop_pending(token) is not None

    def status(
        self,
        scope: Scope,
        user_id: str,
        window_seconds: float,
        max_replies: int,
    ) -> QuotaStatus:
        """在当前配置下读取用量，不预留名额。"""
        scope = self._scope(scope)
        user_id = self._identity(user_id, "user_id")
        window, limit = self._limits(window_seconds, max_replies)
        with self._lock:
            self._check_open()
            now = self._clock()
            self._cleanup(now)
            return self._status(scope, user_id, window, limit, now)

    def reset_quota(self, scope: Scope, user_id: str) -> int:
        """清空用量但保留标记；旧的待确认令牌仍然失效。"""
        scope = self._scope(scope)
        user_id = self._identity(user_id, "user_id")
        with self._lock:
            self._check_open()
            discarded = self._discard_user_pending(scope, user_id)
            with self._db:
                result = self._db.execute(
                    "DELETE FROM reply_usage "
                    "WHERE platform_id = ? AND group_id = ? AND user_id = ?",
                    (*scope, user_id),
                )
            return result.rowcount + discarded

    def close(self) -> None:
        """关闭数据库并放弃未应答的预留。"""
        with self._lock:
            if not self._closed:
                self._pending.clear()
                self._event_tokens.clear()
                self._db.close()
                self._closed = True
