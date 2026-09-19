#!/usr/bin/env python3
"""
workbuddy2openai — 把 WorkBuddy / CodeBuddy 的订阅暴露成标准 OpenAI 兼容 API。

原理（直连后端，原生 function calling）：
  - 读取本机已登录的 CodeBuddy 桌面端凭据（auth 文件里的 token / uid / enterpriseId）。
  - 直接转发到 CodeBuddy 后端 `https://copilot.tencent.com/v2/chat/completions`。
    该后端本身就是标准 OpenAI chat/completions 协议（含原生 tools / tool_calls / SSE 流式）。
  - 转换器只做两件事：①注入鉴权 header（Authorization / X-User-Id 等）
    ②在本地 /v1/* 与后端 /v2/* 之间做路径映射与透传（含 Anthropic / Chat / Responses 三种协议）。
  - token 过期时自动调 `/v2/plugin/auth/token/refresh` 刷新，并回写 auth 文件。

跨平台：自动定位 auth 目录（macOS / Windows / Linux）。
依赖：fastapi + uvicorn + httpx（pip install fastapi "uvicorn[standard]" httpx）。

用法：
  python3 converter.py                       # 默认 127.0.0.1:3009（可用 API_PORT 覆盖）
  python3 converter.py --port 9000
  python3 converter.py --api-key mysecret    # 启用客户端鉴权
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
import uvicorn

try:
    from desensitize import desensitize_body
except ImportError:  # 模块缺失时降级为不脱敏
    def desensitize_body(body, roles=("system",), desensitize_harness_user=False,
                         desensitize_tools=False, compact_harness=False,
                         strip_tool_metadata=False):
        return body

from responses_adapter import (
    responses_request_to_chat,
    ResponsesStreamConverter,
)
from responses_projection import project_responses_chat_body
from anthropic_adapter import (
    anthropic_request_to_chat,
    AnthropicStreamConverter,
)

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

BACKEND = "https://copilot.tencent.com"
DEFAULT_DOMAIN = "www.codebuddy.cn"
# 限流冷却时长：账号被标记 429/超额 后，pick() 会跳过它这么久，到期自动解除。
RATE_COOLDOWN_SECONDS = int(os.environ.get("CODEBUDDY_RATE_COOLDOWN", "30"))
# 会话粘性：同一会话（按会话指纹）保持在同一账号上，只有该账号出错才换号。
# 这样上游的 prompt cache 能命中，避免同会话在两个号之间来回切导致 token 爆炸。
SESSION_TTL_SECONDS = int(os.environ.get("CODEBUDDY_SESSION_TTL", "1800"))     # 30 分钟无活动解绑
SESSION_MAX_ENTRIES = int(os.environ.get("CODEBUDDY_SESSION_MAX", "2000"))
# 账号级失败后的冷却时长（网络错误/5xx 用这个；429 用下面的限流冷却）
FAILOVER_COOLDOWN_SECONDS = int(os.environ.get("CODEBUDDY_FAILOVER_COOLDOWN", "60"))

# --- 限流(429 / code 6004)冷却策略 -------------------------------------------
# 后端 6004「usage exceeds frequency limit」的 msg 里会给出额度重置时刻，例如
#   ... your usage will reset at 2026-09-17 18:05:46 UTC+8 ...
#   ... 将在 2026-09-17 18:05:45 UTC+8 重置 ...
# 冷却时长 = (重置时刻 - 现在) + 5~15 分钟随机抖动。加抖动是为了错开重置瞬间：
# 否则所有号会在同一秒被同时唤醒，又一起撞回 6004。
RATE_RESET_JITTER_MIN = int(os.environ.get("CODEBUDDY_RESET_JITTER_MIN", str(5 * 60)))
RATE_RESET_JITTER_MAX = int(os.environ.get("CODEBUDDY_RESET_JITTER_MAX", str(15 * 60)))
# 后端没给重置时刻时的兜底冷却。原实现固定 30 秒——太短，会在同一个号上反复撞 6004。
RATE_LIMIT_FALLBACK_SECONDS = int(os.environ.get("CODEBUDDY_RATE_LIMIT_FALLBACK", str(30 * 60)))
# 重置时刻最多往后冷却这么久，防止后端给出异常远的时刻把账号锁死。
RATE_RESET_MAX_SECONDS = int(os.environ.get("CODEBUDDY_RESET_MAX", str(24 * 3600)))
# 单次请求内因限流最多再换几个号重试（0 = 只冷却、不换号重试）。
RATE_RETRY_MAX = int(os.environ.get("CODEBUDDY_RATE_RETRY", "3"))
# 对齐 CodeBuddyIDE 的客户端标识。环境变量可用于在其他客户端标识间切换，
# 不必为改动一个请求头修改源码。
USER_AGENT = os.environ.get("CODEBUDDY_USER_AGENT", "CodeBuddyIDE")
CLIENT_PRODUCT = os.environ.get("CODEBUDDY_CLIENT_PRODUCT", "SaaS")
CLIENT_IDE_NAME = os.environ.get("CODEBUDDY_IDE_NAME", "CodeBuddyIDE")
CLIENT_REQUESTED_WITH = os.environ.get("CODEBUDDY_REQUESTED_WITH", "XMLHttpRequest")

# ---------------------------------------------------------------------------
# 平台相关：定位 auth 目录
# ---------------------------------------------------------------------------

def auth_dirs() -> list[Path]:
    env_dir = os.environ.get("CODEBUDDY_AUTH_DIR")
    if env_dir:
        return [Path(env_dir)]
    home = Path.home()
    plat = sys.platform
    if plat == "darwin":
        return [home / "Library" / "Application Support" / "CodeBuddyExtension" / "Data" / "Public" / "auth"]
    if plat == "win32":
        local = Path(os.environ.get("LOCALAPPDATA", home / "AppData" / "Local"))
        return [local / "CodeBuddyExtension" / "Data" / "Public" / "auth"]
    xdg = Path(os.environ.get("XDG_DATA_HOME", home / ".local" / "share"))
    return [xdg / "CodeBuddyExtension" / "Data" / "Public" / "auth"]


def find_auth_file() -> Path | None:
    for d in auth_dirs():
        if d.is_dir():
            for f in sorted(d.glob("*.info")):
                return f
    return None


# ---------------------------------------------------------------------------
# Auth 凭据管理（读 + 自动刷新 + 回写）
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 国际版（WorkBuddy / CodeBuddy.ai）：*.workbuddy.ai 与 *.codebuddy.ai
# 域名的账号走上独立后端（同域名的 https://www.<apex>），国区仍走 copilot.tencent.com
# ---------------------------------------------------------------------------

INTL_DOMAIN_SUFFIXES = (".workbuddy.ai", ".codebuddy.ai")
INTL_BACKEND = "https://www.workbuddy.ai"   # 兼容 panel 导入（workbuddy 默认值）


def is_intl_domain(domain: str) -> bool:
    return bool(domain) and any(domain.endswith(s) for s in INTL_DOMAIN_SUFFIXES)


def intl_backend(domain: str) -> str:
    """*.workbuddy.ai -> https://www.workbuddy.ai；*.codebuddy.ai -> https://www.codebuddy.ai。"""
    apex = domain.rsplit(".", 2)[-2:]  # ['codebuddy','ai']
    return "https://www." + ".".join(apex)


def domain_backend(domain: str) -> str:
    """按 auth.domain 推断后端：国际版同域名站点，否则国区 copilot.tencent.com。"""
    if is_intl_domain(domain):
        return intl_backend(domain)
    return BACKEND


def _client_for(cred: Optional["CredentialManager"], **kwargs):
    """创建带该账号代理的 AsyncClient（proxy 为空 = 直连/系统默认）。"""
    proxy = cred.proxy if cred is not None else ""
    if proxy:
        kwargs.setdefault("proxy", proxy)
    return httpx.AsyncClient(**kwargs)


# ---------------------------------------------------------------------------
# 限流冷却：时长推导 + 状态落盘
# ---------------------------------------------------------------------------

# 匹配 "2026-09-17 18:05:46 UTC+8" / "2026-09-17T18:05:46+08:00" / "... GMT+8" 等写法
_RATE_RESET_RE = re.compile(
    r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?"
    r"\s*(?:UTC|GMT)?\s*([+-]\d{1,2})(?::?(\d{2}))?",
    re.IGNORECASE,
)


def _parse_rate_reset_dt(text: str):
    """解析「额度重置时刻」，返回 (epoch, 带原始时区的 datetime)；解析不到返回 (None, None)。"""
    m = _RATE_RESET_RE.search(text or "")
    if not m:
        return None, None
    try:
        y, mo, d, h, mi, s, oh, om = m.groups()
        sign = -1 if str(oh).startswith("-") else 1
        tz = timezone(sign * timedelta(hours=abs(int(oh)), minutes=abs(int(om or 0))))
        dt = datetime(int(y), int(mo), int(d), int(h), int(mi), int(s or 0), tzinfo=tz)
        return dt.timestamp(), dt
    except Exception:
        return None, None


def parse_rate_reset(text: str) -> Optional[float]:
    """从限流错误文本里解析「额度重置时刻」，返回 epoch 秒；解析不到返回 None。"""
    return _parse_rate_reset_dt(text)[0]


def rate_limit_cooldown_seconds(text: str) -> tuple[float, str]:
    """算该账号被限流后应该冷却多久，返回 (秒数, 人类可读原因)。

    优先按后端给的重置时刻：(重置时刻 - 现在) + 5~15 分钟抖动。
    重置时刻已经过去（后端把旧窗口的时刻回给我们）时退化为「只加抖动」，
    仍能起到把号短暂摘掉、错峰重试的作用。
    解析不到重置时刻才用兜底固定值。
    """
    now = time.time()
    jitter = random.uniform(RATE_RESET_JITTER_MIN, RATE_RESET_JITTER_MAX)
    reset_ts, reset_dt = _parse_rate_reset_dt(text)
    if reset_ts is None:
        total = RATE_LIMIT_FALLBACK_SECONDS + jitter
        return total, (f"响应无重置时刻，兜底 {RATE_LIMIT_FALLBACK_SECONDS / 60:.0f}min"
                       f"+{jitter / 60:.1f}min抖动")
    ahead = min(max(0.0, reset_ts - now), RATE_RESET_MAX_SECONDS)
    total = ahead + jitter
    # 用后端给的时区显示（如 UTC+8），避免容器 TZ=UTC 时日志时刻看着对不上
    reset_str = reset_dt.strftime("%Y-%m-%d %H:%M:%S%z")
    reset_str = reset_str[:-2] + ":" + reset_str[-2:]
    return total, (f"重置时刻 {reset_str}（还有 {ahead / 60:.1f}min）"
                   f"+{jitter / 60:.1f}min抖动")


# 冷却状态落盘：converter 进程重启（改代码/重建容器）后仍记得「哪个号还在限流冷却中」，
# 否则每次重启都会把每个号重新撞一遍 6004。放在 auth 卷里，与 token 统计同处。
COOLDOWN_STATE_PATH = Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth")) / ".cooldowns.json"
_COOLDOWN_LOCK = threading.Lock()
_cooldown_state: Optional[dict] = None
_cooldown_saved_at: float = 0.0
_cooldown_pending: bool = False


def _cooldown_store() -> dict:
    """读取（首次调用时加载）冷却状态表 {账号文件名: {"until": epoch, "reason": str}}。"""
    global _cooldown_state
    with _COOLDOWN_LOCK:
        if _cooldown_state is None:
            try:
                data = json.loads(COOLDOWN_STATE_PATH.read_text(encoding="utf-8"))
                _cooldown_state = data if isinstance(data, dict) else {}
            except Exception:
                _cooldown_state = {}
        return _cooldown_state


def _cooldown_flush(force: bool = False) -> None:
    """把冷却状态写回磁盘。

    force=True 立即写；否则只在有未落盘改动、且距上次写入 ≥5 秒时才写，
    避免上游网络抖动时每个 5xx 失败都去写一次文件。
    """
    global _cooldown_saved_at, _cooldown_pending
    with _COOLDOWN_LOCK:
        if _cooldown_state is None:
            return
        now = time.time()
        if not force and (not _cooldown_pending or now - _cooldown_saved_at < 5):
            return
        try:
            # 顺手丢掉已过期的条目，文件不会无限膨胀
            alive = {k: v for k, v in _cooldown_state.items()
                     if isinstance(v, dict) and float(v.get("until") or 0) > now}
            _cooldown_state.clear()
            _cooldown_state.update(alive)
            COOLDOWN_STATE_PATH.write_text(
                json.dumps(alive, ensure_ascii=False, indent=1), encoding="utf-8")
            _cooldown_pending = False
            _cooldown_saved_at = now
        except Exception:
            pass


def _cooldown_remember(name: str, until: float, reason: str, urgent: bool = True) -> None:
    """记录某账号的冷却截止时刻。

    urgent=True（限流冷却，往往数分钟到数小时）立即落盘，绝不能丢；
    False（60 秒内的短失败冷却）走节流，进程万一立刻退出最多丢一个短冷却。
    """
    global _cooldown_pending
    if not name:
        return
    store = _cooldown_store()
    with _COOLDOWN_LOCK:
        store[name] = {"until": until, "reason": reason}
        _cooldown_pending = True
    _cooldown_flush(force=urgent)


class CredentialManager:
    """从 auth 文件读取凭据；token 临近过期时自动刷新并回写。

    .info 顶层可选字段（均向后兼容，缺省 = 原有行为）：
      - "proxy":   该账号上游请求走的代理，如 "http://127.0.0.1:7890"。
      - "backend": 覆盖后端基址（缺省按 auth.domain 自动推断）。
      - "headers": 追加/覆盖发往后端的请求头（dict）。
    """

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self._cached: dict | None = None
        self._mtime: float = 0.0
        # 限流冷却：被标记 429/超额 后，pick() 会暂时跳过它
        self._cool_until: float = 0.0
        self._cool_reason: str = ""
        # 恢复上次进程留下的冷却记录（容器重建/改代码重启后不丢）
        _rec = _cooldown_store().get(self.name)
        if isinstance(_rec, dict) and float(_rec.get("until") or 0) > time.time():
            self._cool_until = float(_rec["until"])
            self._cool_reason = str(_rec.get("reason") or "重启前已限流")

    @property
    def name(self) -> str:
        """.info 文件名，作为账号在日志/冷却状态表里的标识。"""
        return os.path.basename(str(self.path))

    def mark_rate_limited(self, seconds: float = None, reason: str = ""):
        """标记该账号被限流，进入冷却；pick() 会在冷却期间跳过它。"""
        if seconds is None:
            seconds = RATE_COOLDOWN_SECONDS
        until = time.time() + seconds
        changed = False
        with self._lock:
            # 取更晚的截止时刻（同一账号可能被重复标记），避免短冷却覆盖长冷却
            if until > self._cool_until:
                self._cool_until = until
                self._cool_reason = reason or self._cool_reason
                changed = True
            cur_until, cur_reason = self._cool_until, self._cool_reason
        if changed:
            # 限流冷却（≥60s）立即落盘；短时失败冷却走节流
            _cooldown_remember(self.name, cur_until, cur_reason, urgent=seconds >= 60)

    def is_cooled(self) -> bool:
        return time.time() < self._cool_until

    def cool_remaining(self) -> float:
        return max(0.0, self._cool_until - time.time())

    # ------ 每账号可选覆盖：代理 / 后端 / 请求头（.info 顶层字段） ------

    @property
    def proxy(self) -> str:
        try:
            s = self._session()
            return str(s.get("proxy") or "").strip()
        except Exception:
            return ""

    @property
    def domain(self) -> str:
        try:
            return str((self._session().get("auth") or {}).get("domain") or "")
        except Exception:
            return ""

    @property
    def is_intl(self) -> bool:
        return is_intl_domain(self.domain)

    @property
    def region(self) -> str:
        """账号所属区：intl（*.codebuddy.ai / *.workbuddy.ai）或 cn（国区）。

        用于按模型分发（见 load_model_regions）：模型在两个区的可用性不一致，
        必须只在有该模型的区里挑账号，否则上游 400 code=11102。
        """
        return "intl" if self.is_intl else "cn"

    @property
    def backend(self) -> str:
        try:
            s = self._session()
        except Exception:
            return BACKEND
        b = str(s.get("backend") or "").strip().rstrip("/")
        return b or domain_backend(str((s.get("auth") or {}).get("domain") or ""))

    def _read_raw(self) -> dict:
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _load_if_stale(self):
        """若文件 mtime 变了（外部刷新过），重新加载缓存。"""
        try:
            mt = self.path.stat().st_mtime
        except OSError:
            return
        if self._cached is None or mt != self._mtime:
            self._cached = self._read_raw()
            self._mtime = mt

    def _session(self) -> dict:
        self._load_if_stale()
        if self._cached is None:
            raise RuntimeError(f"无法读取 auth 文件：{self.path}")
        return self._cached

    def _is_expired(self) -> bool:
        s = self._session()
        expires_at = (s.get("auth") or {}).get("expiresAt") or 0
        # 提前 60s 判定过期
        return time.time() * 1000 >= (expires_at - 60_000)

    def _refresh(self):
        """调后端刷新 token，写回 auth 文件与缓存。"""
        s = self._session()
        auth = s.get("auth") or {}
        headers = self._build_headers_from(auth, s.get("account") or {})
        headers["X-Refresh-Token"] = auth.get("refreshToken", "")
        # 刷新来源标识：workbuddy.ai 用 "workbuddy"；codebuddy.ai 实测用 "plugin"；国区 "plugin"
        headers["X-Auth-Refresh-Source"] = "workbuddy" if self.domain.endswith(".workbuddy.ai") else "plugin"
        url = f"{self.backend}/v2/plugin/auth/token/refresh"
        try:
            kw = {"timeout": 15}
            if self.proxy:
                kw["proxy"] = self.proxy
            with httpx.Client(**kw) as c:
                r = c.post(url, headers=headers, json={})
            data = r.json()
        except Exception as e:
            raise RuntimeError(f"刷新 token 网络失败：{e}")
        if data.get("code") != 0 or not data.get("data"):
            raise RuntimeError(f"刷新 token 失败：{data.get('msg', data)}")
        new_auth = data["data"]
        # 继承部分字段
        new_auth["domain"] = new_auth.get("domain") or auth.get("domain")
        new_auth["lastRefreshTime"] = int(time.time() * 1000)
        # 计算 expiresAt（若后端没直接给）
        if not new_auth.get("expiresAt") and new_auth.get("expiresIn"):
            new_auth["expiresAt"] = int(time.time() * 1000) + new_auth["expiresIn"] * 1000
        if not new_auth.get("refreshExpiresAt") and new_auth.get("refreshExpiresIn"):
            new_auth["refreshExpiresAt"] = int(time.time() * 1000) + new_auth["refreshExpiresIn"] * 1000
        s["auth"] = new_auth
        # 原子写回
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(s, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)
        self._cached = s
        self._mtime = self.path.stat().st_mtime

    def _build_headers_from(self, auth: dict, account: dict) -> dict:
        domain = auth.get("domain") or DEFAULT_DOMAIN
        h = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {auth.get('accessToken','')}",
            "X-User-Id": account.get("uid", ""),
            "X-Enterprise-Id": account.get("enterpriseId", ""),
            "X-Tenant-Id": account.get("enterpriseId", ""),
            "X-Domain": domain,
            "X-Product": CLIENT_PRODUCT,
            "X-IDE-Name": CLIENT_IDE_NAME,
            "X-Requested-With": CLIENT_REQUESTED_WITH,
            "User-Agent": USER_AGENT,
        }
        # 国际版（workbuddy.ai）后端会校验 Origin/Referer 与 UA，对齐官方 CLI 指纹
        if is_intl_domain(domain):
            origin = intl_backend(domain)
            h["Origin"] = origin
            h["Referer"] = origin + "/"
            h["User-Agent"] = "CLI/2.63.2 CodeBuddy/2.63.2"
        # 每账号自定义 header（.info 顶层 "headers" 字段），优先级最高
        try:
            extra = self._session().get("headers")
            if isinstance(extra, dict):
                h.update({str(k): str(v) for k, v in extra.items()})
        except Exception:
            pass
        return h

    def get_headers(self) -> dict:
        """返回带最新 token 的后端请求 header；必要时先刷新。"""
        with self._lock:
            if self._is_expired():
                self._refresh()
            s = self._session()
            return self._build_headers_from(s.get("auth") or {}, s.get("account") or {})

    def summary(self) -> dict:
        s = self._session()
        auth = s.get("auth") or {}
        acct = s.get("account") or {}
        exp = auth.get("expiresAt", 0)
        return {
            "uid": acct.get("uid"),
            "nickname": acct.get("nickname"),
            "enterpriseName": acct.get("enterpriseName"),
            "domain": auth.get("domain"),
            "proxy": self.proxy or None,
            "backend": self.backend,
            "token_expires_at": exp,
            "token_expired": self._is_expired(),
        }


# ---------------------------------------------------------------------------
# 多账号池（轮询 / 热加载）
# ---------------------------------------------------------------------------

class AccountPool:
    """扫描 auth 目录下的所有 *.info，构建账号池。

    - **会话粘性**：同一会话固定用同一账号（保住上游 prompt cache），
      只有该账号报错（429/5xx/网络错误）才解绑并冷却，下个请求自动换号。
    - 无会话指纹时退化为"全局粘性"（沿用上一个可用账号）。
    - 热加载：目录 mtime 变化时自动增删账号，新增/删除 .info 即时生效，无需重启。
    - 单账号场景自动降级为普通单凭据。
    """

    def __init__(self, auth_dir: Path):
        self.auth_dir = Path(auth_dir)
        self._lock = threading.Lock()
        self._managers: dict[str, CredentialManager] = {}  # 绝对路径 -> 管理器
        self._order: list[str] = []                          # 稳定顺序（按文件名排序）
        self._idx = 0
        self._dir_mtime = 0.0
        self._affinity: dict[str, tuple] = {}                # 会话指纹 -> (账号路径, 最后使用时间)
        self._sticky: Optional[str] = None                   # 无会话指纹时的全局粘性
        self._scan()

    def _scan(self):
        try:
            mt = self.auth_dir.stat().st_mtime
        except OSError:
            return
        # 目录未变且已有账号，则跳过
        if mt == self._dir_mtime and self._managers:
            return
        self._dir_mtime = mt
        files = sorted(self.auth_dir.glob("*.info"))
        paths = {str(f) for f in files}
        # 移除已删除的账号
        for p in list(self._managers.keys()):
            if p not in paths:
                del self._managers[p]
        # 新增的账号
        new_order: list[str] = []
        for f in files:
            p = str(f)
            if p not in self._managers:
                try:
                    self._managers[p] = CredentialManager(f)
                except Exception:
                    continue
            new_order.append(p)
        self._order = new_order

    def list_accounts(self) -> list[dict]:
        with self._lock:
            self._scan()
            out: list[dict] = []
            for p in self._order:
                mgr = self._managers.get(p)
                base = os.path.basename(p)
                if mgr is None:
                    out.append({"file": base, "error": "load failed"})
                    continue
                try:
                    s = mgr.summary()
                    s["file"] = base
                    out.append(s)
                except Exception as e:
                    out.append({"file": base, "error": str(e)})
            return out

    def pick(self, session_key: str = "", model: str = "") -> Optional[CredentialManager]:
        """选号：**会话粘性优先，报错才换**（不再是每请求轮询）。

        1) 该会话已绑定的账号、健康、且属于该模型允许的区 → 直接复用（保住上游 prompt cache）
        2) 无会话键时沿用上一个账号（全局粘性）
        3) 绑定失效/无绑定/不属于允许的区 → 从该区健康账号里挑一个并绑定
        失败路径（429/5xx/网络错误）会由 failover() 解绑+冷却，下个请求自然换号。

        model 用于**按区分发**：只有 model_regions.json 里列了的模型才限定区；
        未列出的模型 allowed_regions=None，等价于改造前的"任意账号"行为。
        """
        allowed = allowed_regions_for_model(model)
        with self._lock:
            self._scan()
            if not self._order:
                return None
            self._gc_affinity_locked()
            mgr = self._bound_locked(session_key, allowed)
            if mgr is not None:
                return mgr
            mgr = self._pick_healthy_locked(allowed)
            if mgr is not None:
                self._bind_locked(session_key, mgr)
            return mgr

    # ---- 粘性内部实现（调用方需已持有 self._lock） ----

    @staticmethod
    def _region_ok(mgr: "CredentialManager", allowed: Optional[set]) -> bool:
        """账号是否属于该模型允许的区；allowed 为 None 表示不限制。"""
        if allowed is None:
            return True
        try:
            return mgr.region in allowed
        except Exception:
            return False

    def _bound_locked(self, session_key: str,
                      allowed: Optional[set] = None) -> Optional[CredentialManager]:
        path = None
        if session_key:
            ent = self._affinity.get(session_key)
            path = ent[0] if ent else None
            if path is None:
                return None
        else:
            path = self._sticky
            if path is None:
                return None
        mgr = self._managers.get(path)
        # 账号被删 / 被停用 / 正在限流冷却 / 不属于该模型允许的区 → 解绑，交给调用方重新选
        if mgr is None or mgr.is_cooled() or not self._region_ok(mgr, allowed):
            if session_key:
                self._affinity.pop(session_key, None)
            else:
                self._sticky = None
            return None
        if session_key:
            self._affinity[session_key] = (path, time.time())
        else:
            self._sticky = path
        return mgr

    def _pick_healthy_locked(self, allowed: Optional[set] = None) -> Optional[CredentialManager]:
        """从健康账号里挑（跳过冷却中的），起点用轮询游标；全不可用则选冷却最快结束的。

        allowed 非 None 时只在指定区（cn/intl）的账号里挑——该区一个账号都没有时返回 None，
        调用方据此报"当前模型无可用账号"，而不是随便挑个别区的账号去撞 11102。
        """
        n = len(self._order)
        if n == 0:
            return None
        best_fallback, best_remain = None, float("inf")
        first_eligible = None
        for _ in range(n):
            p = self._order[self._idx % n]
            self._idx += 1
            mgr = self._managers.get(p)
            if mgr is None or not self._region_ok(mgr, allowed):
                continue
            if first_eligible is None:
                first_eligible = mgr
            if mgr.is_cooled():
                r = mgr.cool_remaining()
                if r < best_remain:
                    best_remain, best_fallback = r, mgr
                continue
            return mgr
        if best_fallback is not None:
            return best_fallback
        return first_eligible

    def _bind_locked(self, session_key: str, mgr: CredentialManager):
        path = str(getattr(mgr, "path", "") or "")
        if not path:
            return
        if session_key:
            if len(self._affinity) >= SESSION_MAX_ENTRIES:
                self._gc_affinity_locked(force_drop=True)
            self._affinity[session_key] = (path, time.time())
        else:
            self._sticky = path

    def _gc_affinity_locked(self, force_drop: bool = False):
        """清理过期绑定；条目过多时按最后使用时间淘汰。"""
        now = time.time()
        for k, (_, ts) in list(self._affinity.items()):
            if now - ts > SESSION_TTL_SECONDS:
                self._affinity.pop(k, None)
        if force_drop and len(self._affinity) >= SESSION_MAX_ENTRIES:
            keep = sorted(self._affinity.items(), key=lambda kv: kv[1][1], reverse=True)[: SESSION_MAX_ENTRIES // 2]
            self._affinity = dict(keep)

    def unbind_account(self, path) -> int:
        """把绑定到该账号的会话全部解绑（账号出错时调用），返回解绑数量。"""
        p = str(path or "")
        if not p:
            return 0
        n = 0
        with self._lock:
            for k, (ap, _) in list(self._affinity.items()):
                if ap == p:
                    self._affinity.pop(k, None)
                    n += 1
            if self._sticky == p:
                self._sticky = None
                n += 1
        return n

    def failover(self, mgr: CredentialManager, seconds: float = None, reason: str = ""):
        """账号级失败：冷却该账号并解绑其所有会话 → 下一个请求自动换号。"""
        if mgr is None:
            return
        self.mark_ratelimited(mgr, seconds=seconds, reason=reason)
        n = self.unbind_account(getattr(mgr, "path", None))
        left = mgr.cool_remaining()
        _log(f"⚠ 账号 {mgr.name} 失败({reason})，"
             f"冷却 {left / 60:.1f} 分钟并解绑 {n} 个会话")

    def affinity_view(self, limit: int = 20) -> dict:
        with self._lock:
            items = sorted(self._affinity.items(), key=lambda kv: kv[1][1], reverse=True)[:limit]
            return {"sessions": len(self._affinity),
                    "sticky": os.path.basename(self._sticky) if self._sticky else None,
                    "bindings": {k: os.path.basename(v[0]) for k, v in items}}


    def mark_ratelimited(self, mgr: CredentialManager, seconds: float = None,
                         reason: str = ""):
        """外部给指定账号打冷却标记（后端 429/超额 时调用）。"""
        with self._lock:
            mgr.mark_rate_limited(seconds=seconds, reason=reason)

    def cooled_accounts(self) -> list[dict]:
        with self._lock:
            out = []
            for p in self._order:
                mgr = self._managers.get(p)
                if mgr is not None and mgr.is_cooled():
                    left = mgr.cool_remaining()
                    out.append({"file": mgr.name,
                                "cool_seconds": round(left),
                                "cool_minutes": round(left / 60, 1),
                                "until": int(mgr._cool_until),
                                "reason": mgr._cool_reason})
            out.sort(key=lambda x: x["cool_seconds"])
            return out

    def count(self) -> int:
        with self._lock:
            self._scan()
            return len(self._order)


# ---------------------------------------------------------------------------
# 模型列表
# ---------------------------------------------------------------------------

DEFAULT_MODELS = [
    "glm-5.3-flash", "kimi-k3", "deepseek-v4.1-flash", "hy4-preview",
]

# 模型清单持久化文件（放 auth 挂载卷，容器重建不丢；面板「模型管理」写这里）
MODELS_FILE = os.environ.get("CODEBUDDY_MODELS_FILE") or (
    os.path.join(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"), "models.json"))


def load_model_list() -> list:
    """优先读 models.json（面板手动维护），缺失/为空/坏 JSON 时回退内置 DEFAULT_MODELS。"""
    try:
        with open(MODELS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        ms = data.get("models") if isinstance(data, dict) else data
        if isinstance(ms, list):
            ids = [str(m).strip() for m in ms if str(m).strip()]
            if ids:
                return ids
    except Exception:
        pass
    return list(DEFAULT_MODELS)


# ---------------------------------------------------------------------------
# 按区分发（国区 cn / 国际区 intl）
# ---------------------------------------------------------------------------
# 背景：同一批模型名在两个区的可用性并不一致（glm-5.3-flash 只有国区有，
# claude-*/gemini-* 只有国际区有），而账号池里国区、国际区账号是混在一起的。
# 若不管模型随便落号，请求会打到没有该模型的区，上游直接 400 code=11102。
#
# 规则放在 model_regions.json（与 models.json 同目录的**独立文件**——故意分开，
# 这样面板「模型管理」整体覆盖 models.json 时不会把分区配置冲掉）：
#     {"glm-5.3-flash": ["cn"], "deepseek-v4.1-flash": ["cn", "intl"]}
# 也接受 {"regions": {...}} 包裹写法。**未列出的模型不限制**（= 改造前的旧行为）。
# 文件每次请求热读，改完即时生效，无需重启。

REGIONS_FILE = os.environ.get("CODEBUDDY_MODEL_REGIONS_FILE") or (
    os.path.join(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"), "model_regions.json"))

VALID_REGIONS = ("cn", "intl")


def load_model_regions() -> dict:
    """读 model_regions.json → {模型名: {"cn"/"intl"}}；缺失或格式不对返回 {} = 不限制。"""
    try:
        with open(REGIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    if isinstance(data, dict) and isinstance(data.get("regions"), dict):
        data = data["regions"]
    if not isinstance(data, dict):
        return {}
    out: dict = {}
    for k, v in data.items():
        name = str(k).strip()
        if not name:
            continue
        if isinstance(v, str):
            v = [v]
        if not isinstance(v, list):
            continue
        regions = {str(x).strip().lower() for x in v if str(x).strip()}
        regions &= set(VALID_REGIONS)
        if regions:
            out[name] = regions
    return out


def allowed_regions_for_model(model: str) -> Optional[set]:
    """该模型允许落在哪些区的账号上；None = 不限制（未配置 / 文件缺失）。"""
    m = str(model or "").strip()
    if not m:
        return None
    return load_model_regions().get(m) or None


# 后端请求体里出现过的额外字段（透传时若客户端给了就保留）
PASSTHROUGH_BODY_KEYS = {
    "model", "messages", "tools", "tool_choice", "temperature",
    "max_tokens", "max_completion_tokens", "top_p", "stream",
    "stream_options", "stop", "presence_penalty", "frequency_penalty",
    "n", "response_format", "seed", "user", "reasoning_effort",
    "verbosity", "reasoning_summary",
}

# ---------------------------------------------------------------------------
# FastAPI 应用
# ---------------------------------------------------------------------------

app = FastAPI(title="workbuddy2openai", version="2.0")
CONFIG: dict = {"api_key": "", "api_key_file": None, "cred": None, "pool": None,
                "log_path": None, "desensitize": False, "no_compact": False}
# cred: CredentialManager | None ; pool: AccountPool | None（多账号轮询，优先于单 cred）


# ---------------------------------------------------------------------------
# 日志（写文件）
# ---------------------------------------------------------------------------

_LOG_LOCK = threading.Lock()


def _log(msg: str):
    """写一行日志到 CONFIG['log_path'] 指定的文件（追加，带时间戳）。未设置则丢弃。"""
    path = CONFIG.get("log_path")
    if not path:
        return
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n"
    try:
        with _LOG_LOCK:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
    except OSError:
        pass  # 日志失败不应影响主流程




def _truncate(s: str, n: int = 80) -> str:
    s = str(s).replace("\n", " ").strip()
    return s[:n] + ("…" if len(s) > n else "")


def _check_auth(authorization: Optional[str], x_api_key: Optional[str]):
    key = CONFIG["api_key"]
    # 没有显式 key 时，尝试从 key 文件热读取（支持运行时改 key 不重启）
    if not key:
        kf = CONFIG.get("api_key_file")
        if kf and os.path.exists(kf):
            try:
                with open(kf, "r", encoding="utf-8") as f:
                    key = (f.readline() or "").strip()
            except OSError:
                key = ""
    if not key:
        return
    token = ""
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:].strip()
    if not token and x_api_key:
        token = x_api_key
    if token != key:
        raise HTTPException(status_code=401, detail={"error": {"message": "invalid api key", "type": "auth_error"}})


def _session_key(payload: dict, request: Request = None) -> str:
    """从请求里推导"会话指纹"，用于账号粘性（同一会话固定同一账号）。

    优先级：显式会话头 > OpenAI 的 user / Anthropic 的 metadata.user_id > 首条 user 消息指纹。
    同一轮对话里首条 user 消息不变，所以 agent 多轮循环会稳定命中同一账号；
    跨会话/跨客户端则各用各的粘性，不会互相抢号。
    """
    try:
        if request is not None:
            for h in ("x-session-id", "x-conversation-id", "conversation-id", "x-thread-id"):
                v = request.headers.get(h)
                if v:
                    return "h:" + v.strip()[:120]
        if not isinstance(payload, dict):
            return ""
        u = str(payload.get("user") or "").strip()
        if u:
            return "u:" + u[:120]
        md = payload.get("metadata")
        if isinstance(md, dict):
            u2 = str(md.get("user_id") or md.get("session_id") or "").strip()
            if u2:
                return "m:" + u2[:120]
        first = ""
        msgs = payload.get("messages")
        if isinstance(msgs, list):
            for m in msgs:
                if isinstance(m, dict) and m.get("role") == "user":
                    c = m.get("content")
                    if isinstance(c, str):
                        first = c
                    elif isinstance(c, list):
                        first = " ".join(str(b.get("text") or "") for b in c if isinstance(b, dict))
                    break
        if not first:
            inp = payload.get("input")
            if isinstance(inp, str):
                first = inp
            elif isinstance(inp, list):
                for it in inp:
                    if isinstance(it, dict) and it.get("role") == "user":
                        c = it.get("content")
                        if isinstance(c, str):
                            first = c
                        elif isinstance(c, list):
                            first = " ".join(str(b.get("text") or "") for b in c if isinstance(b, dict))
                        break
        if first:
            import hashlib
            return "s:" + hashlib.md5(first[:4000].encode("utf-8", "replace")).hexdigest()[:16]
    except Exception:
        pass
    return ""


def _cred(session_key: str = "", model: str = "") -> CredentialManager:
    pool = CONFIG.get("pool")
    if pool is not None:
        mgr = pool.pick(session_key, model)
        if mgr is not None:
            return mgr
        # 该模型有分区限制，但对应区一个可用账号都没有 → 明确报错；
        # 绝不能退到别区的账号去撞上游 400 code=11102。
        allowed = allowed_regions_for_model(model)
        if allowed is not None:
            raise HTTPException(status_code=503, detail={"error": {
                "message": f"模型 {model} 只允许在 {'/'.join(sorted(allowed))} 区账号上运行，"
                           f"但当前没有该区的可用账号（可能都在冷却中），请稍后重试。",
                "type": "no_available_account"}})
    if CONFIG["cred"] is not None:
        return CONFIG["cred"]
    raise HTTPException(status_code=503, detail={"error": {"message": "未找到登录凭据，请先在桌面端登录 CodeBuddy/WorkBuddy，或在 auth 目录放入 .info 文件", "type": "auth_error"}})


def _ensure_system_first(body: dict, cred: CredentialManager):
    """腾讯渠道对消息角色有校验，两类问题都在这里归一化：

    1) 任何 role=developer 的消息（新版 OpenAI 客户端把 system 提示写成
       developer）会被渠道整体拒绝 → code 11128 Illegal API invocation from an
       unapproved channel（2026-09-12 实测：单条 developer 的最小请求即可复现）。
       这里统一改写成 system，国区/国际版后端都做。
    2) workbuddy.ai 国际版后端还要求首条消息必须是 system（否则 11128），
       客户端没带时补一条中性 system；国区后端无此要求，不做补齐。"""
    msgs = body.get("messages") or []
    for m in msgs:
        if isinstance(m, dict) and m.get("role") == "developer":
            m["role"] = "system"
    if cred is None or not cred.is_intl:
        return
    if not msgs or msgs[0].get("role") != "system":
        msgs.insert(0, {"role": "system", "content": "You are a helpful assistant."})
        body["messages"] = msgs


# ---------------------------------------------------------------------------
# 思考强度（reasoning_effort）强制
# ---------------------------------------------------------------------------
# 实测（2026-09-11）：后端只认 reasoning_effort 这一个字段，且 max 效果最强
# （deepseek-v4.1-flash 从 0 思考变成约 50 reasoning tokens，glm-5.2 约 130）。
# 其他写法（thinking / enable_thinking / reasoning.effort / thinking_budget）都被静默忽略。
# 取值优先级：环境变量 CODEBUDDY_REASONING_EFFORT > auth 目录下 .reasoning_effort 文件首行（热改）
# > 默认值。**默认 ""＝不强制**（客户端传什么就是什么，等价于改造前行为）；
# 要开启就显式配置，例如文件写入 "max"（全局）或 "deepseek-v4.1-flash=max"（按模型）。
# 取值为空 / off / none / false / 0 时不强制，透传客户端传的值。

DEFAULT_REASONING_EFFORT = ""


def forced_reasoning_effort(model: str = "") -> str:
    """返回要强制的强度值；空字符串表示不强制。

    文件支持两种写法（每行一条）：
      max                      —— 全局强制
      deepseek-v4.1-flash=max  —— 只对该模型强制（优先匹配，其次看 "=" 左侧为 * 的行）
    """
    raw = os.environ.get("CODEBUDDY_REASONING_EFFORT")
    if raw is None:
        try:
            d = auth_dirs()[0]
            f = d / ".reasoning_effort"
            if f.is_file():
                raw = f.read_text(encoding="utf-8")
        except Exception:
            raw = ""
    if raw is None:
        raw = DEFAULT_REASONING_EFFORT
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if not lines:
        return ""
    per_model: dict[str, str] = {}
    default = ""
    for ln in lines:
        if "=" in ln:
            k, v = ln.split("=", 1)
            k, v = k.strip(), v.strip()
            if k == "*":
                default = v
            elif k:
                per_model[k] = v
        else:
            default = ln          # 纯值 = 全局
    val = per_model.get(model, default) if model else default
    val = (val or "").strip()
    if val.lower() in ("", "off", "none", "false", "0"):
        return ""
    return val


def _extract_incoming_reasoning(payload: dict) -> tuple:
    """从客户端原始请求体里识别「思考强度」写法，返回 (effort, veto, source)。

    上游只认顶层 reasoning_effort，而各客户端的写法五花八门，这里统一识别：
      - 顶层 reasoning_effort: "high"                       （OpenAI 风格）
      - reasoning: {"effort": "high", "enabled": true}      （Hermes / OpenRouter 风格）
      - thinking: {"type": "enabled", "budget_tokens": 8192}（Claude Code 风格）
      - enable_thinking: true / thinking_budget: N
    veto=True 表示客户端明确要求"不思考"（reasoning.enabled=false / thinking.type=disabled），
    此时不再强制，尊重客户端的关闭意图。
    """
    if not isinstance(payload, dict):
        return ("", False, "")

    # Claude / Anthropic 风格 thinking（含 budget → 粗粒度映射）
    th = payload.get("thinking")
    if isinstance(th, dict):
        t = str(th.get("type") or "").lower()
        if t in ("disabled", "off") or th.get("enabled") is False:
            return ("", True, "thinking.disabled")
        if t in ("enabled", "on") or th.get("enabled") is True:
            try:
                budget = int(th.get("budget_tokens") or 0)
            except Exception:
                budget = 0
            if budget and budget <= 2048:
                return ("low", False, "thinking(low)")
            if budget and budget <= 8192:
                return ("medium", False, "thinking(medium)")
            return ("high", False, "thinking(high)")
        return ("", False, "thinking")

    # Hermes / OpenRouter 风格 reasoning 对象
    r = payload.get("reasoning")
    if isinstance(r, dict):
        if r.get("enabled") is False:
            return ("", True, "reasoning.disabled")
        e = str(r.get("effort") or "").strip().lower()
        return (e, False, f"reasoning({e or 'default'})")
    if isinstance(r, str):
        return (r.strip().lower(), False, "reasoning(str)")

    # 顶层标准字段
    e = str(payload.get("reasoning_effort") or "").strip().lower()
    if e:
        return (e, False, "reasoning_effort")
    if payload.get("enable_thinking") is True:
        return ("high", False, "enable_thinking")
    if payload.get("enable_thinking") is False:
        return ("", True, "enable_thinking=false")
    return ("", False, "")


def _force_reasoning_effort(body: dict, payload: dict = None) -> str:
    """决定最终发给上游的 reasoning_effort。

    规则（按优先级）：
      1) 配置值（auth/.reasoning_effort 或环境变量，如 max）——**无条件覆盖**，
         连客户端明确的 thinking:disabled 也会被改成 max（"传什么都是 max"）。
      2) 没有配置时：透传归一化后的客户端值；客户端明确关闭思考则不传（尊重客户端）。
    """
    inc, veto, src = _extract_incoming_reasoning(payload if payload is not None else body)
    forced = forced_reasoning_effort(str(body.get("model") or ""))
    if not forced and veto:
        return ""
    final = forced or inc
    if final:
        body["reasoning_effort"] = final
        try:
            _log(f"reasoning: 客户端[{src or '未传'}] → 上游 reasoning_effort={final}"
                 + ("（配置强制）" if forced else "（透传）"))
        except Exception:
            pass
    return final


@app.get("/health")
def health():
    info: dict = {"status": "ok", "platform": sys.platform, "python": sys.version.split()[0],
                  "mode": "direct-proxy (native function calling)",
                  "multi_account": CONFIG.get("pool") is not None,
                  "account_count": CONFIG.get("pool").count() if CONFIG.get("pool") else (1 if CONFIG["cred"] else 0)}
    pool = CONFIG.get("pool")
    if pool is not None:
        try:
            info["accounts"] = pool.list_accounts()
        except Exception as e:
            info["accounts_error"] = str(e)
        try:
            info["affinity"] = pool.affinity_view()      # 会话粘性现状（会话数/绑定）
        except Exception:
            pass
        try:
            cooled = pool.cooled_accounts()              # 限流冷却中的账号（含原因/解除时刻）
            info["cooled_accounts"] = cooled
            info["available_count"] = pool.count() - len(cooled)
        except Exception:
            pass
    else:
        af = find_auth_file()
        info["auth_file"] = str(af or "(未找到)")
        cred = CONFIG["cred"]
        if cred is not None:
            try:
                info["credential"] = cred.summary()
            except Exception as e:
                info["credential_error"] = str(e)
    return info


@app.get("/v1/models")
def list_models(authorization: Optional[str] = Header(default=None),
                x_api_key: Optional[str] = Header(default=None, alias="X-Api-Key")):
    _check_auth(authorization, x_api_key)
    data = [{"id": m, "object": "model", "created": 1700000000, "owned_by": "codebuddy"}
            for m in load_model_list()]
    return {"object": "list", "data": data}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request,
                           authorization: Optional[str] = Header(default=None),
                           x_api_key: Optional[str] = Header(default=None, alias="X-Api-Key")):
    _check_auth(authorization, x_api_key)
    try:
        payload = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": {"message": f"bad json: {e}", "type": "invalid_request_error"}})

    messages = payload.get("messages") or []
    if not messages:
        raise HTTPException(status_code=400, detail={"error": {"message": "messages is required", "type": "invalid_request_error"}})

    # 选号：按会话指纹粘住同一账号（同会话不再来回切号）
    session_key = _session_key(payload, request)
    cred = _cred(session_key, payload.get("model") or "")

    # 构造后端 body：只透传已知的合法字段
    client_wants_stream = bool(payload.get("stream"))
    body = {k: payload[k] for k in PASSTHROUGH_BODY_KEYS if k in payload}
    body.setdefault("model", "auto")
    # 后端只支持流式：始终以 stream=True 调后端，非流式由转换器聚合
    body["stream"] = True
    if "stream_options" not in body:
        body["stream_options"] = {"include_usage": True}

    # 可选：脱敏。缓解客户端合规模板（如 Codex CLI / ZCode 注入的说明文字）被后端误判为敏感词。
    # 处理 system / developer 消息、Codex 注入的上下文 user 消息，以及 tools 的 description。
    if CONFIG.get("desensitize"):
        body = desensitize_body(body, roles=("system", "developer"),
                                desensitize_harness_user=True,
                                desensitize_tools=True,
                                compact_harness=not CONFIG.get("no_compact"),
                                strip_tool_metadata=True)

    # 思考强度：先归一化客户端写法（reasoning/thinking/顶层），再按配置强制（默认配置 max）
    forced_effort = _force_reasoning_effort(body, payload)

    # 日志：请求摘要
    model_name = payload.get("model", "auto")
    tool_names = [t.get("function", {}).get("name") for t in (payload.get("tools") or [])
                  if isinstance(t, dict)]
    last_user = _last_user_text(messages)
    rid = os.urandom(4).hex()
    _log(f"[{rid}] ▶ REQUEST {model_name} | stream={client_wants_stream} | msgs={len(messages)}"
         + (f" | tools={tool_names}" if tool_names else "")
         + (f" | last_user={_truncate(last_user, 60)!r}" if last_user else ""))
    # 完整请求体（发往后端的实际内容；若启用脱敏，这里已是脱敏后）
    _log(f"[{rid}] ── REQUEST BODY (发往后端) ──\n{json.dumps(body, ensure_ascii=False, indent=2)}")

    _ensure_system_first(body, cred)
    headers = cred.get_headers()
    url = f"{cred.backend}/v2/chat/completions"
    t0 = time.time()

    if client_wants_stream:
        return StreamingResponse(
            _stream_upstream(url, headers, body, model_name, t0, rid, cred, session_key),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # 非流式：后端只支持流式，这里把后端 SSE 聚合成单个 chat.completion 响应。
    # 限流(429/6004)时冷却该号并换一个可用账号，把这次请求重发一遍。
    attempts = max(1, RATE_RETRY_MAX + 1)
    collected: dict = {}
    try:
        for _attempt in range(attempts):
            async with _client_for(cred, timeout=300) as c:
                async with c.stream("POST", url, headers=headers, json=body) as r:
                    if r.status_code != 200:
                        raw = await r.aread()
                        _log(f"[{rid}] ✗ HTTP {r.status_code} | {model_name} | {_truncate(raw.decode('utf-8','replace'),200)}")
                        _log(f"[{rid}] ── ERROR BODY ──\n{raw.decode('utf-8','replace')}")
                        record_request(cred, model_name, "chat", status=f"HTTP {r.status_code}",
                                       err=raw.decode("utf-8", "replace"), stream=False,
                                       elapsed_ms=(time.time() - t0) * 1000)
                        nxt = (_failover_for_retry(cred, session_key, model_name, r.status_code, raw, rid)
                               if _attempt + 1 < attempts else None)
                        if nxt is None:
                            raise HTTPException(status_code=r.status_code, detail=_safe_err_raw(raw, r.status_code))
                        cred, headers, url = _switch_account(nxt, body)
                        continue
                    collected = await _collect_stream(r)
            break
    except HTTPException:
        raise
    except httpx.HTTPError as e:
        _log(f"[{rid}] ✗ 网络错误 | {model_name} | {e}")
        record_request(cred, model_name, "chat", status="error", err=str(e), stream=False,
                       elapsed_ms=(time.time() - t0) * 1000)
        raise HTTPException(status_code=502, detail={"error": {"message": f"upstream error: {e}", "type": "upstream_error"}})
    _log_finish(model_name, t0, collected, rid, cred=cred, proto="chat")
    return JSONResponse(content=collected)


def _last_user_text(messages: list) -> str:
    """取最后一条 user 消息的文本，用于日志预览。"""
    for m in reversed(messages):
        if m.get("role") != "user":
            continue
        content = m.get("content", "")
        if isinstance(content, list):
            for blk in content:
                if isinstance(blk, dict) and blk.get("type") == "text":
                    return str(blk.get("text", ""))
            return ""
        return str(content)
    return ""


# ---------------------------------------------------------------------------
# token 用量统计（panel /api/token-stats 消费；写在 auth 卷内持久化）
# ---------------------------------------------------------------------------
STATS_LOCK = threading.Lock()
STATS_PATH = Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth")) / ".token_stats.jsonl"


def _stat_record(model_name, usage):
    try:
        rec = {"ts": int(time.time()), "model": str(model_name or "?"),
               "pt": int((usage or {}).get("prompt_tokens") or 0),
               "ct": int((usage or {}).get("completion_tokens") or 0)}
        with STATS_LOCK:
            try:
                if STATS_PATH.exists() and STATS_PATH.stat().st_size > 8_000_000:
                    _lines = STATS_PATH.read_text(encoding="utf-8").splitlines()
                    STATS_PATH.write_text("\n".join(_lines[-20000:]) + "\n", encoding="utf-8")
            except Exception:
                pass
            with STATS_PATH.open("a", encoding="utf-8") as _f:
                _f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 请求日志（真实转发流量，含「哪个账号被调用」；panel /api/logs 消费）
# ---------------------------------------------------------------------------
REQLOG_PATH = Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth")) / ".request_log.jsonl"
REQLOG_MAX_BYTES = 8_000_000
REQLOG_KEEP_LINES = 20000


def _acct_ident(cred) -> tuple:
    """返回 (账号文件名, 昵称)，用于把请求归到具体账号。"""
    if cred is None:
        return ("-", "")
    try:
        name = os.path.basename(str(getattr(cred, "path", "") or "")) or "-"
    except Exception:
        name = "-"
    nick = ""
    try:
        s = cred._session()
        nick = str(((s.get("account") or {}).get("nickname") or ""))
    except Exception:
        pass
    return (name, nick)


def record_request(cred, model, proto, usage=None, elapsed_ms=0, status="ok",
                   finish=None, err=None, stream=None) -> None:
    """记一次真实转发请求：写 .request_log.jsonl（面板统计/日志页用）。失败不影响主流程。"""
    try:
        u = usage or {}
        rt = ((u.get("completion_tokens_details") or {}).get("reasoning_tokens")) or 0
        acct, nick = _acct_ident(cred)
        rec = {
            "ts": int(time.time() * 1000),
            "acct": acct,
            "nick": nick,
            "model": str(model or "?"),
            "proto": proto,
            "pt": int(u.get("prompt_tokens") or 0),
            "ct": int(u.get("completion_tokens") or 0),
            "rt": int(rt),
            "ms": int(elapsed_ms or 0),
            "status": str(status),
            "finish": finish,
            "stream": bool(stream) if stream is not None else None,
            "err": (str(err)[:200] if err else None),
        }
        with STATS_LOCK:
            try:
                if REQLOG_PATH.exists() and REQLOG_PATH.stat().st_size > REQLOG_MAX_BYTES:
                    _lines = REQLOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
                    REQLOG_PATH.write_text("\n".join(_lines[-REQLOG_KEEP_LINES:]) + "\n", encoding="utf-8")
            except Exception:
                pass
            with REQLOG_PATH.open("a", encoding="utf-8") as _f:
                _f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        # 账号级失败（429 / 5xx / 网络错误）→ 冷却该账号并解绑其会话，下个请求自动换号
        st = str(status)
        if st == "error" or st.startswith(("HTTP 5", "HTTP 429", "HTTP 408")):
            pool = CONFIG.get("pool")
            if pool is not None and cred is not None:
                # 限流类（429 / 6004）按后端给的重置时刻算冷却；其他失败用固定值
                if st.startswith("HTTP 429") or "6004" in str(err or ""):
                    secs, why = rate_limit_cooldown_seconds(str(err or ""))
                    reason = f"{st} {why}"
                else:
                    secs, reason = FAILOVER_COOLDOWN_SECONDS, st
                pool.failover(cred, seconds=secs, reason=reason)
    except Exception:
        pass


def _log_finish(model_name: str, t0: float, result: dict, rid: str = "",
                cred=None, proto: str = "chat"):
    """记录一次完成的请求：耗时 / finish_reason / usage / 工具调用 / 审核拦截 + 完整响应。"""
    elapsed = time.time() - t0
    prefix = f"[{rid}] " if rid else ""
    choice = (result.get("choices") or [{}])[0]
    finish = choice.get("finish_reason")
    msg = choice.get("message") or {}
    tcs = msg.get("tool_calls") or []
    usage = result.get("usage") or {}
    _stat_record(model_name, usage)
    record_request(cred, model_name, proto, usage, elapsed_ms=elapsed * 1000,
                   status="ok", finish=finish, stream=False)
    tag = ""
    if finish == "content-filter":
        tag = " ⚠️内容审核拦截"
    tc_names = [t.get("function", {}).get("name") for t in tcs]
    _log(f"{prefix}◀ RESPONSE {model_name} | {elapsed:.1f}s | finish={finish}{tag}"
         + (f" | tool_calls={tc_names}" if tc_names else "")
         + f" | tokens={usage.get('total_tokens', '?')}")
    # 完整响应体
    _log(f"{prefix}── RESPONSE BODY ──\n{json.dumps(result, ensure_ascii=False, indent=2)}")


async def _collect_stream(response: httpx.Response) -> dict:
    """消费后端的 OpenAI SSE 流，聚合成单个非流式 chat.completion 对象。

    合并所有 chunk 的 delta（content / tool_calls），并取 usage / finish_reason。
    """
    content_parts: list[str] = []
    # tool_calls: index -> {id, name, arguments(分片拼接)}
    tool_calls: dict[int, dict] = {}
    model: str | None = None
    finish_reason: str | None = None
    usage: dict | None = None

    async for line in response.aiter_lines():
        line = line.strip()
        if not line or not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            break
        try:
            chunk = json.loads(data)
        except json.JSONDecodeError:
            continue
        model = chunk.get("model") or model
        if chunk.get("usage"):
            usage = chunk["usage"]
        for choice in chunk.get("choices") or []:
            if choice.get("finish_reason"):
                finish_reason = choice["finish_reason"]
            delta = choice.get("delta") or {}
            if delta.get("content"):
                content_parts.append(delta["content"])
            for tc in delta.get("tool_calls") or []:
                idx = tc.get("index", 0)
                slot = tool_calls.setdefault(idx, {"id": None, "name": None, "arguments": ""})
                if tc.get("id"):
                    slot["id"] = tc["id"]
                fn = tc.get("function") or {}
                if fn.get("name"):
                    slot["name"] = fn["name"]
                if fn.get("arguments"):
                    slot["arguments"] += fn["arguments"]

    tcs = None
    if tool_calls:
        tcs = [
            {"id": v["id"], "type": "function",
             "function": {"name": v["name"], "arguments": v["arguments"]}}
            for _, v in sorted(tool_calls.items())
        ]
        finish_reason = finish_reason or "tool_calls"

    message = {"role": "assistant", "content": "".join(content_parts) or None}
    if tcs:
        message["tool_calls"] = tcs
    return {
        "id": "chatcmpl-" + os.urandom(12).hex(),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model or "unknown",
        "choices": [{"index": 0, "message": message,
                     "finish_reason": finish_reason or "stop"}],
        "usage": usage or {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }


def _safe_err_raw(raw: bytes, status: int) -> dict:
    try:
        return json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        return {"error": {"message": raw.decode("utf-8", "replace")[:500], "type": "upstream_error", "code": status}}


async def _stream_upstream(url: str, headers: dict, body: dict,
                           model_name: str = "?", t0: float = 0.0, rid: str = "",
                           cred: Optional[CredentialManager] = None,
                           session_key: str = ""):
    """把后端 SSE 原样转发给客户端（后端已是标准 OpenAI SSE，含 tool_calls）。

    同时轻量解析流，统计 finish_reason / tool_calls / usage 用于日志，不阻塞转发。
    完整原始 SSE 累积后落盘到日志（调试用）。
    限流(429/6004)时在**尚未向客户端输出任何字节**前换号重试，最多 RATE_RETRY_MAX 次。
    """
    finish_reason = None
    tool_names: list[str] = []
    usage: dict = {}
    saw_filter = False
    buf = b""
    raw_parts: list[bytes] = []   # 累积完整原始 SSE
    prefix = f"[{rid}] " if rid else ""

    def _feed(chunk: bytes):
        nonlocal finish_reason, saw_filter, buf
        # 行缓冲解析：把累计的 chunk 按 data: 行切出来统计
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            line = line.strip()
            if not line.startswith(b"data:"):
                continue
            data = line[5:].strip()
            if data == b"[DONE]":
                continue
            try:
                obj = json.loads(data)
            except Exception:
                continue
            if obj.get("usage"):
                usage.update(obj["usage"])
            for ch in obj.get("choices") or []:
                if ch.get("finish_reason"):
                    finish_reason = ch["finish_reason"]
                for tc in (ch.get("delta") or {}).get("tool_calls") or []:
                    nm = (tc.get("function") or {}).get("name")
                    if nm:
                        tool_names.append(nm)
            # 内容审核拦截常以 content-filter 或特殊中文文案返回
            try:
                text_repr = data.decode("utf-8", "replace")
            except Exception:
                text_repr = ""
            if "content-filter" in text_repr or "敏感" in text_repr or "审核" in text_repr:
                saw_filter = True

    attempts = max(1, RATE_RETRY_MAX + 1)
    try:
        for _attempt in range(attempts):
            async with _client_for(cred, timeout=None) as c:
                async with c.stream("POST", url, headers=headers, json=body) as r:
                    if r.status_code != 200:
                        err = await r.aread()
                        _log(f"{prefix}✗ HTTP {r.status_code} | {model_name} | {_truncate(err.decode('utf-8','replace'),200)}")
                        _log(f"{prefix}── ERROR BODY ──\n{err.decode('utf-8','replace')}")
                        record_request(cred, model_name, "chat", status=f"HTTP {r.status_code}",
                                       err=err.decode("utf-8", "replace"), stream=True,
                                       elapsed_ms=(time.time() - t0) * 1000 if t0 else 0)
                        # 限流(429/6004)：冷却该号并换一个可用账号，把这次请求重发一遍
                        nxt = (_failover_for_retry(cred, session_key, model_name, r.status_code, err, rid)
                               if _attempt + 1 < attempts else None)
                        if nxt is None:
                            yield _err_event(err, r.status_code)
                            return
                        cred, headers, url = _switch_account(nxt, body)
                        continue
                    async for chunk in r.aiter_bytes():
                        if chunk:
                            raw_parts.append(chunk)
                            _feed(chunk)
                            yield chunk
                    break
    except httpx.HTTPError as e:
        _log(f"{prefix}✗ 网络错误 | {model_name} | {e}")
        record_request(cred, model_name, "chat", status="error", err=str(e), stream=True,
                       elapsed_ms=(time.time() - t0) * 1000 if t0 else 0)
        yield _err_event(str(e).encode(), 502)

    # 流结束：输出完成日志
    elapsed = time.time() - t0 if t0 else 0
    tag = " ⚠️内容审核拦截" if (saw_filter or finish_reason == "content-filter") else ""
    _log(f"{prefix}◀ RESPONSE {model_name} | {elapsed:.1f}s | stream finish={finish_reason}{tag}"
         + (f" | tool_calls={tool_names}" if tool_names else "")
         + f" | tokens={usage.get('total_tokens', '?')}")
    _stat_record(model_name, usage)
    record_request(cred, model_name, "chat", usage, elapsed_ms=elapsed * 1000,
                   status="ok", finish=finish_reason, stream=True)
    # 完整原始 SSE（后端返回的全部内容）
    _log(f"{prefix}── RESPONSE RAW SSE ──\n{b''.join(raw_parts).decode('utf-8','replace')}")


def _safe_err(r: httpx.Response) -> dict:
    try:
        return {"error": r.json()}
    except Exception:
        return {"error": {"message": r.text[:500], "type": "upstream_error", "code": r.status_code}}


def _mark_ratelimit_if_needed(cred: Optional[CredentialManager],
                              status: int, body: bytes,
                              reason: str = "后端") -> Optional[float]:
    """后端返回限流类错误时，给当前使用的账号打冷却标记，让 pick() 后续跳过它。

    判定为“限流/超额”：
      - HTTP 429 一定算；
      - 响应体含 code 6004 一定算（后端明确的「超额/频率限制」码）；
      - 4xx/5xx 且响应体含典型限流关键字时也算（兼容后端用 5xx 表达限流）。

    冷却时长交给 rate_limit_cooldown_seconds()：有「额度重置时刻」提示时按
    重置时刻 + 5~15 分钟抖动，没有才用兜底值。返回冷却秒数；未判定为限流返回 None。
    """
    if cred is None or status == 200:
        return None
    text = body.decode("utf-8", "replace")
    low = text.lower()
    compact = low.replace(" ", "")
    if status == 429 or '"code":6004' in compact or "'code':6004" in compact:
        hit = True
    else:
        hints = ("429", "rate limit", "too many", "frequent", "frequency",
                 "quota", "exceed", "限制", "限流", "频繁", "超额", "超频", "频控")
        hit = status in (400, 403, 500, 502, 503) and any(h in low for h in hints)
    if not hit:
        return None
    secs, why = rate_limit_cooldown_seconds(text)
    cred.mark_rate_limited(seconds=secs, reason=f"{reason} HTTP{status} {why}")
    _log(f"⚠ 账号 {cred.name} 判定限流(HTTP {status})，"
         f"冷却 {secs / 60:.1f} 分钟（{why}）")
    return secs


def _switch_account(cred: CredentialManager, body: dict) -> tuple:
    """换号后重算该账号专属的请求头与后端地址（body 也需按新号重新归一化）。"""
    _ensure_system_first(body, cred)
    return cred, cred.get_headers(), f"{cred.backend}/v2/chat/completions"


def _failover_for_retry(cred: Optional[CredentialManager], session_key: str = "",
                        model: str = "", status: int = 0, raw: bytes = b"",
                        rid: str = "") -> Optional[CredentialManager]:
    """限流时冷却当前账号 + 解绑其会话，再挑一个可用账号给**同一次请求**重试。

    只有判定为「限流/超额」才换号：普通 4xx（参数错误等）换个号也一样失败，
    直接返回 None 让调用方把原始错误回给客户端。
    返回下一个可用账号；非限流 / 没有别的可用账号时返回 None。
    """
    if not _mark_ratelimit_if_needed(cred, status, raw, reason="限流换号"):
        return None
    pool = CONFIG.get("pool")
    if pool is None or cred is None:
        return None
    try:
        pool.unbind_account(getattr(cred, "path", None))
    except Exception:
        pass
    try:
        nxt = _cred(session_key, model)
    except HTTPException:
        # 该区一个可用账号都没有（都在冷却中）
        return None
    if nxt is None or getattr(nxt, "path", None) == getattr(cred, "path", None):
        return None
    if nxt.is_cooled():
        # 全池都在冷却：没有真正可用的号，别拿冷却中的号再撞一次
        cooled = pool.cooled_accounts()
        _log(f"[{rid}] ⚠ {model} 无可用账号：其余 {len(cooled)} 个号均在冷却中，"
             f"最快 {min((c['cool_seconds'] for c in cooled), default=0) / 60:.1f} 分钟后解除")
        return None
    _log(f"[{rid}] ↻ 限流换号重试：{cred.name} → {nxt.name}")
    return nxt


def _err_event(msg: bytes, status: int) -> bytes:
    # 以 OpenAI SSE 错误 chunk 形式返回
    import json as _json, time as _time
    chunk = {
        "error": {"message": msg.decode("utf-8", "replace")[:500], "type": "upstream_error", "code": status},
    }
    return f"data: {_json.dumps(chunk, ensure_ascii=False)}\n\n".encode("utf-8")


def _looks_like_content_filter_text(text: str) -> bool:
    text = (text or "").lower()
    return (
        "content-filter" in text
        or "content_filter" in text
        or "敏感内容" in text
        or "内容审核" in text
        or "无法响应您的请求" in text
    )


def _chat_body_desensitize(body: dict, *, force_compact: bool = False) -> dict:
    if not CONFIG.get("desensitize"):
        return body
    return desensitize_body(
        body,
        roles=("system", "developer"),
        desensitize_harness_user=True,
        desensitize_tools=True,
        compact_harness=(force_compact or not CONFIG.get("no_compact")),
        strip_tool_metadata=True,
    )


async def _post_backend_once(url: str, headers: dict, body: dict,
                             cred: Optional[CredentialManager] = None) -> tuple[int, bytes]:
    async with _client_for(cred, timeout=120) as c:
        async with c.stream("POST", url, headers=headers, json=body) as r:
            chunks: list[bytes] = []
            async for chunk in r.aiter_bytes():
                if chunk:
                    chunks.append(chunk)
            raw = b"".join(chunks)
            if r.status_code != 200:
                _mark_ratelimit_if_needed(cred, r.status_code, raw)
            return r.status_code, raw


async def _post_backend_with_filter_retry(url: str, headers: dict, body: dict,
                                          rid: str = "", model_name: str = "?",
                                          cred: Optional[CredentialManager] = None) -> tuple[int, bytes, dict]:
    prefix = f"[{rid}] " if rid else ""
    status, raw = await _post_backend_once(url, headers, body, cred=cred)
    text = raw.decode("utf-8", "replace")
    if status == 200 and _looks_like_content_filter_text(text) and CONFIG.get("desensitize") and CONFIG.get("no_compact"):
        retry_body = _chat_body_desensitize(body, force_compact=True)
        _log(f"{prefix}↻ RESPONSES {model_name} | content filter detected, retry with compact harness")
        _log(f"{prefix}── RESPONSES RETRY CHAT BODY ──\n{json.dumps(retry_body, ensure_ascii=False, indent=2)}")
        retry_status, retry_raw = await _post_backend_once(url, headers, retry_body, cred=cred)
        retry_text = retry_raw.decode("utf-8", "replace")
        if retry_status == 200 and not _looks_like_content_filter_text(retry_text):
            return retry_status, retry_raw, retry_body
    return status, raw, body


# ---------------------------------------------------------------------------
# Responses API 端点（Codex CLI 兼容）
# ---------------------------------------------------------------------------

@app.post("/v1/responses")
async def create_response(request: Request,
                          authorization: Optional[str] = Header(default=None),
                          x_api_key: Optional[str] = Header(default=None, alias="X-Api-Key")):
    """OpenAI Responses API 兼容端点。

    Codex CLI 使用 Responses API（wire_api = "responses"）而非 Chat Completions。
    本端点接收 Responses 格式请求，转换为 Chat 格式发往后端，再将后端的 Chat SSE
    转换为 Responses 语义事件流返回。
    """
    _check_auth(authorization, x_api_key)
    try:
        payload = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": {"message": f"bad json: {e}", "type": "invalid_request_error"}})

    # 选号：按会话指纹粘住同一账号
    session_key = _session_key(payload, request)
    cred = _cred(session_key, payload.get("model") or "")

    # 转换请求：Responses → Chat
    try:
        chat_body = responses_request_to_chat(payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": {"message": f"request conversion error: {e}", "type": "invalid_request_error"}})

    chat_body, projection_stats = project_responses_chat_body(chat_body)
    chat_body.setdefault("model", "auto")
    chat_body["stream"] = True
    if "stream_options" not in chat_body:
        chat_body["stream_options"] = {"include_usage": True}

    chat_body = _chat_body_desensitize(chat_body)

    client_wants_stream = payload.get("stream", True)  # Codex CLI 默认 stream
    model_name = payload.get("model", "auto")
    rid = os.urandom(4).hex()
    _log(f"[{rid}] ▶ RESPONSES {model_name} | stream={client_wants_stream} | input_items={len(payload.get('input', []))}")
    _log(
        f"[{rid}] ── RESPONSES PROJECTION ── "
        f"mode={projection_stats.get('mode')} "
        f"| msgs {projection_stats.get('original_messages')}→{projection_stats.get('projected_messages')} "
        f"| chars {projection_stats.get('original_message_chars')}→{projection_stats.get('projected_message_chars')} "
        f"| tools {projection_stats.get('original_tools')}→{projection_stats.get('projected_tools')} "
        f"| tool_chars {projection_stats.get('original_tool_chars')}→{projection_stats.get('projected_tool_chars')} "
        f"| summarized_history={projection_stats.get('summarized_history_messages', 0)} "
        f"| dropped_harness={projection_stats.get('dropped_harness_messages', 0)} "
        f"| anchor_user={projection_stats.get('anchor_user_preserved', False)}"
    )
    forced_effort = _force_reasoning_effort(chat_body, payload)
    _log(f"[{rid}] ── RESPONSES → CHAT BODY ──\n{json.dumps(chat_body, ensure_ascii=False, indent=2)}")

    _ensure_system_first(chat_body, cred)
    headers = cred.get_headers()
    url = f"{cred.backend}/v2/chat/completions"
    t0 = time.time()

    if client_wants_stream:
        return StreamingResponse(
            _stream_responses(url, headers, chat_body, model_name, t0, rid, cred, session_key),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # 非流式：聚合后端 SSE → 非流式 Response 对象。
    # 限流(429/6004)时冷却该号并换一个可用账号，把这次请求重发一遍。
    attempts = max(1, RATE_RETRY_MAX + 1)
    _usage: dict = {}
    converter: Optional[ResponsesStreamConverter] = None
    try:
        for _attempt in range(attempts):
            status_code, raw, final_body = await _post_backend_with_filter_retry(
                url, headers, chat_body, rid, model_name, cred=cred)
            if status_code != 200:
                _log(f"[{rid}] ✗ HTTP {status_code} | {model_name} | {_truncate(raw.decode('utf-8','replace'),200)}")
                record_request(cred, model_name, "responses", status=f"HTTP {status_code}",
                               err=raw.decode("utf-8", "replace"), stream=False,
                               elapsed_ms=(time.time() - t0) * 1000)
                nxt = (_failover_for_retry(cred, session_key, model_name, status_code, raw, rid)
                       if _attempt + 1 < attempts else None)
                if nxt is None:
                    raise HTTPException(status_code=status_code, detail=_safe_err_raw(raw, status_code))
                cred, headers, url = _switch_account(nxt, chat_body)
                continue
            _usage = {}
            converter = ResponsesStreamConverter(model=model_name)
            for line in raw.decode("utf-8", "replace").splitlines():
                if line.startswith("data:"):
                    try:
                        _j = json.loads(line[5:].strip())
                        if _j.get("usage"):
                            _usage = _j["usage"]
                    except Exception:
                        pass
                converter.feed_line(line)
            chat_body = final_body
            break
    except HTTPException:
        raise
    except httpx.HTTPError as e:
        _log(f"[{rid}] ✗ 网络错误 | {model_name} | {e}")
        record_request(cred, model_name, "responses", status="error", err=str(e), stream=False,
                       elapsed_ms=(time.time() - t0) * 1000)
        raise HTTPException(status_code=502, detail={"error": {"message": f"upstream error: {e}", "type": "upstream_error"}})

    result = converter.get_nonstream_response()
    elapsed = time.time() - t0
    _log(f"[{rid}] ◀ RESPONSES {model_name} | {elapsed:.1f}s")
    _log(f"[{rid}] ── RESPONSE OBJ ──\n{json.dumps(result, ensure_ascii=False, indent=2)}")
    record_request(cred, model_name, "responses", _usage, elapsed_ms=elapsed * 1000,
                   status="ok", finish=None, stream=False)
    return JSONResponse(content=result)


async def _stream_responses(url: str, headers: dict, body: dict,
                            model_name: str = "?", t0: float = 0.0, rid: str = "",
                            cred: Optional[CredentialManager] = None,
                            session_key: str = ""):
    """消费后端 Chat SSE，实时转换为 Responses API 事件流输出。

    限流(429/6004)时在**尚未向客户端输出任何事件**前换号重试，最多 RATE_RETRY_MAX 次。
    """
    converter = ResponsesStreamConverter(model=model_name)
    prefix = f"[{rid}] " if rid else ""
    usage: dict = {}
    finish_reason = None
    raw_sse_lines: list[str] = []

    attempts = max(1, RATE_RETRY_MAX + 1)
    try:
        for _attempt in range(attempts):
            status_code, raw, _ = await _post_backend_with_filter_retry(
                url, headers, body, rid, model_name, cred=cred)
            if status_code != 200:
                _log(f"{prefix}✗ HTTP {status_code} | {model_name} | {_truncate(raw.decode('utf-8','replace'),200)}")
                record_request(cred, model_name, "responses", status=f"HTTP {status_code}",
                               err=raw.decode("utf-8", "replace"), stream=True,
                               elapsed_ms=(time.time() - t0) * 1000 if t0 else 0)
                nxt = (_failover_for_retry(cred, session_key, model_name, status_code, raw, rid)
                       if _attempt + 1 < attempts else None)
                if nxt is None:
                    error_evt = {"type": "error", "error": {"message": raw.decode('utf-8','replace')[:500], "code": status_code}}
                    yield f"data: {json.dumps(error_evt, ensure_ascii=False)}\n\n".encode("utf-8")
                    return
                cred, headers, url = _switch_account(nxt, body)
                continue
            raw_sse_lines = []
            for line in raw.decode("utf-8", "replace").splitlines():
                if line.strip():
                    raw_sse_lines.append(line)
                    if line.startswith("data:"):
                        try:
                            _j = json.loads(line[5:].strip())
                            if _j.get("usage"):
                                usage = _j["usage"]
                            for _ch in _j.get("choices") or []:
                                if _ch.get("finish_reason"):
                                    finish_reason = _ch["finish_reason"]
                        except Exception:
                            pass
                events = converter.feed_line(line)
                if events:
                    yield events.encode("utf-8")
            break
    except httpx.HTTPError as e:
        _log(f"{prefix}✗ 网络错误 | {model_name} | {e}")
        record_request(cred, model_name, "responses", status="error", err=str(e), stream=True,
                       elapsed_ms=(time.time() - t0) * 1000 if t0 else 0)
        error_evt = {"type": "error", "error": {"message": str(e)[:500], "code": 502}}
        yield f"data: {json.dumps(error_evt, ensure_ascii=False)}\n\n".encode("utf-8")
        return

    # 发送收尾事件
    finish_events = converter.finish()
    if finish_events:
        yield finish_events.encode("utf-8")

    elapsed = time.time() - t0 if t0 else 0
    _log(f"{prefix}◀ RESPONSES {model_name} | {elapsed:.1f}s | stream done")
    _log(f"{prefix}── RESPONSES RAW SSE ──\n" + "\n".join(raw_sse_lines[-30:]))
    record_request(cred, model_name, "responses", usage, elapsed_ms=elapsed * 1000,
                   status="ok", finish=finish_reason, stream=True)


# ---------------------------------------------------------------------------
# Anthropic Messages API 端点（Claude Code / CC Switch 兼容）
# ---------------------------------------------------------------------------

@app.post("/v1/messages")
async def create_message(request: Request,
                         authorization: Optional[str] = Header(default=None),
                         x_api_key: Optional[str] = Header(default=None, alias="X-Api-Key")):
    """Anthropic Messages API 兼容端点。

    Claude Code / CC Switch 使用 Anthropic Messages API（POST /v1/messages）。
    本端点接收 Anthropic 格式请求，转换为 Chat 格式发往后端，再将后端的 Chat SSE
    转换为 Anthropic SSE 事件流返回。
    """
    _check_auth(authorization, x_api_key)
    try:
        payload = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": {"message": f"bad json: {e}", "type": "invalid_request_error"}})

    # 将 Anthropic 格式消息、工具规范在进入后端前统一转换为 OpenAI Chat 格式。
    messages = payload.get("messages") or []
    if not messages:
        raise HTTPException(status_code=400, detail={"error": {"message": "messages is required", "type": "invalid_request_error"}})

    # 选号：按会话指纹粘住同一账号（Claude Code 传 metadata.user_id 时按它归属）
    session_key = _session_key(payload, request)
    cred = _cred(session_key, payload.get("model") or "")

    try:
        chat_body = anthropic_request_to_chat(payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": {"message": f"request conversion error: {e}", "type": "invalid_request_error"}})

    chat_body.setdefault("model", "auto")
    chat_body["stream"] = True
    if "stream_options" not in chat_body:
        chat_body["stream_options"] = {"include_usage": True}

    if CONFIG.get("desensitize"):
        chat_body = desensitize_body(chat_body, roles=("system", "developer"),
                                     desensitize_harness_user=True,
                                     desensitize_tools=True,
                                     compact_harness=not CONFIG.get("no_compact"),
                                     strip_tool_metadata=True)

    model_name = payload.get("model", "auto")
    chat_messages = chat_body.get("messages", [])
    rid = os.urandom(4).hex()
    _log(f"[{rid}] ▶ ANTHROPIC {model_name} | msgs={len(chat_messages)} | anthropic_msgs={len(messages)}")
    forced_effort = _force_reasoning_effort(chat_body, payload)
    _log(f"[{rid}] ── ANTHROPIC → CHAT BODY ──\n{json.dumps(chat_body, ensure_ascii=False, indent=2)}")

    _ensure_system_first(chat_body, cred)
    headers = cred.get_headers()
    url = f"{cred.backend}/v2/chat/completions"
    t0 = time.time()

    return StreamingResponse(
        _stream_anthropic(url, headers, chat_body, model_name, t0, rid, cred, session_key),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _stream_anthropic(url: str, headers: dict, body: dict,
                            model_name: str = "?", t0: float = 0.0, rid: str = "",
                            cred: Optional[CredentialManager] = None,
                            session_key: str = ""):
    """消费后端 OpenAI Chat SSE，实时转换为 Anthropic Messages SSE 事件流。

    限流(429/6004)时在**尚未向客户端输出任何事件**前换号重试，最多 RATE_RETRY_MAX 次。
    """
    converter = AnthropicStreamConverter(model=model_name)
    prefix = f"[{rid}] " if rid else ""
    usage: dict = {}
    finish_reason = None

    attempts = max(1, RATE_RETRY_MAX + 1)
    try:
        for _attempt in range(attempts):
            async with _client_for(cred, timeout=None) as c:
                async with c.stream("POST", url, headers=headers, json=body) as r:
                    if r.status_code != 200:
                        err = await r.aread()
                        _log(f"{prefix}✗ HTTP {r.status_code} | {model_name} | {_truncate(err.decode('utf-8','replace'),200)}")
                        record_request(cred, model_name, "anthropic", status=f"HTTP {r.status_code}",
                                       err=err.decode("utf-8", "replace"), stream=True,
                                       elapsed_ms=(time.time() - t0) * 1000 if t0 else 0)
                        nxt = (_failover_for_retry(cred, session_key, model_name, r.status_code, err, rid)
                               if _attempt + 1 < attempts else None)
                        if nxt is None:
                            error_evt = {"type": "error", "error": {"message": err.decode('utf-8','replace')[:500], "type": "api_error", "code": r.status_code}}
                            yield f"event: error\ndata: {json.dumps(error_evt, ensure_ascii=False)}\n\n".encode("utf-8")
                            return
                        cred, headers, url = _switch_account(nxt, body)
                        continue
                    async for line in r.aiter_lines():
                        if line.startswith("data:"):
                            try:
                                _j = json.loads(line[5:].strip())
                                if _j.get("usage"):
                                    usage = _j["usage"]
                                for _ch in _j.get("choices") or []:
                                    if _ch.get("finish_reason"):
                                        finish_reason = _ch["finish_reason"]
                            except Exception:
                                pass
                        events = converter.feed_line(line)
                        if events:
                            yield events.encode("utf-8")
                    break
    except httpx.HTTPError as e:
        _log(f"{prefix}✗ 网络错误 | {model_name} | {e}")
        record_request(cred, model_name, "anthropic", status="error", err=str(e), stream=True,
                       elapsed_ms=(time.time() - t0) * 1000 if t0 else 0)
        error_evt = {"type": "error", "error": {"message": str(e)[:500], "type": "api_error", "code": 502}}
        yield f"event: error\ndata: {json.dumps(error_evt, ensure_ascii=False)}\n\n".encode("utf-8")
        return

    finish_events = converter.finish()
    if finish_events:
        yield finish_events.encode("utf-8")

    elapsed = time.time() - t0 if t0 else 0
    _log(f"{prefix}◀ ANTHROPIC {model_name} | {elapsed:.1f}s | stream done")
    record_request(cred, model_name, "anthropic", usage, elapsed_ms=elapsed * 1000,
                   status="ok", finish=finish_reason, stream=True)


@app.post("/v1/messages/count_tokens")
async def count_tokens(request: Request,
                       authorization: Optional[str] = Header(default=None),
                       x_api_key: Optional[str] = Header(default=None, alias="X-Api-Key")):
    """Anthropic token 计数端点（stub）。

    Claude Code 可能在发送消息前调用此端点。
    返回一个简单估算值，不做实际 token 计数。
    """
    _check_auth(authorization, x_api_key)
    return {"input_tokens": 0}


# ---------------------------------------------------------------------------
# 启动
# ---------------------------------------------------------------------------

def preflight() -> bool:
    pool = CONFIG.get("pool")
    sys.stderr.write("==== 预检 ====\n")
    sys.stderr.write(f"平台      : {sys.platform}\n")
    sys.stderr.write(f"Python    : {sys.version.split()[0]}\n")
    sys.stderr.write(f"后端      : {BACKEND} (直连，原生 function calling)\n")
    if auth_dirs():
        sys.stderr.write(f"已查目录  : {', '.join(str(d) for d in auth_dirs())}\n")
    ok = True
    if pool is not None:
        n = pool.count()
        sys.stderr.write(f"账号模式  : 多账号轮询（{n} 个）\n")
        try:
            for acc in pool.list_accounts():
                if "error" in acc:
                    sys.stderr.write(f"  - {acc.get('file')} 读取失败: {acc['error']}\n")
                    continue
                exp = "是(将自动刷新)" if acc.get("token_expired") else "否"
                sys.stderr.write(f"  - {acc.get('nickname')} / {acc.get('enterpriseName')} "
                                 f"(uid={acc.get('uid')}) token过期={exp}\n")
        except Exception as e:
            sys.stderr.write(f"[警告] 读取账号失败：{e}\n")
            ok = False
    else:
        af = find_auth_file()
        sys.stderr.write(f"登录文件  : {af or '(未找到)'}\n")
        if af is None:
            sys.stderr.write("\n[警告] 未找到登录文件。请在桌面端完成登录（CodeBuddy/WorkBuddy）。\n")
            ok = False
        else:
            try:
                cm = CredentialManager(af)
                info = cm.summary()
                sys.stderr.write(f"账号      : {info.get('nickname')} / {info.get('enterpriseName')}\n")
                sys.stderr.write(f"token过期 : {'是(将自动刷新)' if info['token_expired'] else '否'}\n")
            except Exception as e:
                sys.stderr.write(f"[警告] 读取凭据失败：{e}\n")
                ok = False
    sys.stderr.write("================\n")
    return ok


def main():
    ap = argparse.ArgumentParser(description="CodeBuddy -> OpenAI 兼容转换器（直连后端）")
    # 默认只监听回环：对外统一走 panel 的 3008 反代，避免 API 端口被直接暴露。
    ap.add_argument("--host", default=os.environ.get("API_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("API_PORT", "3009")))
    ap.add_argument("--api-key", default=os.environ.get("CODEBUDDY2OPENAI_KEY", ""),
                    help="可选：要求客户端携带的 API key（默认不校验）")
    ap.add_argument("--api-key-file", default=os.environ.get("CODEBUDDY2OPENAI_KEY_FILE"),
                    help="可选：API key 文件路径。运行时读取该文件首行作为 key，可热更新 key 不重启。"
                         "文件不存在或为空则不校验。优先级低于 --api-key。")
    ap.add_argument("--log", default=None, metavar="PATH",
                    help="开启日志并写到该文件（如 --log converter.log 或 --log /tmp/cb.log）。"
                         "不传则不记日志。")
    ap.add_argument("--desensitize", action="store_true",
                    help="启用脱敏：对 system 消息里的合规模板敏感词（DoS/exploit/credential 等）"
                         "插入零宽空格，缓解被后端内容审核误拦。默认关闭。")
    ap.add_argument("--no-compact", action="store_true",
                    help="配合 --desensitize 使用：跳过 system/harness 压缩，仅做零宽脱敏。"
                         "保留原始 system prompt 完整内容（如 Claude Code 的行为指令），"
                         "但审核误拦风险略高于默认压缩模式。")
    ap.add_argument("--skip-check", action="store_true", help="跳过启动预检")
    args = ap.parse_args()

    CONFIG["api_key"] = args.api_key
    CONFIG["api_key_file"] = args.api_key_file
    CONFIG["desensitize"] = args.desensitize
    CONFIG["no_compact"] = args.no_compact
    # --log 直接指定文件路径即开启；不传则不记
    CONFIG["log_path"] = args.log if args.log else os.environ.get("CODEBUDDY2OPENAI_LOG")

    # 凭据初始化：优先建多账号池（auth 目录下存在 *.info 即视为多账号），否则单凭据兼容
    auth_dir = auth_dirs()[0] if auth_dirs() else None
    pool, cred = None, None
    if auth_dir is not None and auth_dir.is_dir() and any(auth_dir.glob("*.info")):
        pool = AccountPool(auth_dir)
        sys.stderr.write(f"多账号模式：扫描到 {pool.count()} 个账号（目录 {auth_dir}）\n")
    else:
        af = find_auth_file()
        cred = CredentialManager(af) if af else None
    CONFIG["pool"] = pool
    CONFIG["cred"] = cred

    if not args.skip_check:
        preflight()

    sys.stderr.write(f"\n✅ 监听 http://{args.host}:{args.port}（直连后端，原生 function calling）\n")
    sys.stderr.write("   GET  /v1/models\n")
    sys.stderr.write("   POST /v1/chat/completions   (原生 tools/tool_calls，支持流式)\n")
    sys.stderr.write("   POST /v1/responses          (Responses API，Codex CLI 兼容)\n")
    sys.stderr.write("   POST /v1/messages           (Anthropic API，Claude Code / CC Switch 兼容)\n")
    sys.stderr.write("   GET  /health\n")
    if args.api_key:
        sys.stderr.write("   鉴权已启用（API key 已设置）\n")
    if CONFIG["log_path"]:
        sys.stderr.write(f"   日志      : {CONFIG['log_path']}\n")
    if args.desensitize:
        mode = "零宽脱敏 + 保留全文" if args.no_compact else "零宽脱敏 + 压缩摘要"
        sys.stderr.write(f"   脱敏      : 已启用（{mode}）\n")
    sys.stderr.write("按 Ctrl+C 退出。\n\n")

    # 启动时写一条标记
    _log(f"==== converter 启动 ====")

    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
