#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""workbuddy2api 管理面板（唯一对外入口，默认 3008）。

功能：
  - 直接导入用户导出的 WorkBuddy/CodeBuddy 凭据 JSON（xyw110 格式）-> 转 .info 落 auth 目录
  - 列出 / 删除账号（auth 目录下所有 .info）
  - 设置 / 读取全局 API key（写入 auth 目录 .api_key，converter 通过 --api-key-file 热读取）
  - 列出模型 + 探测每个模型可用性（走 converter 入口）
  - 检查每个账号“剩余额度/可用性”：直接拿 token 打后端发最小请求，判断可用性与限流，
    叠加 token 过期状态 + 本地面板累计统计（腾讯后端无公开配额接口，故为探测式）
  - 查看 converter 轮询状态（账号数 / 多账号模式 / 各账号摘要）
  - 反向代理 /v1/* 与 /health 到内网的 converter，使 3008 成为唯一对外入口

复用 converter 的 CredentialManager（自动刷新 token + 回写）、AccountPool、后端常量。
"""
import os
import json
import time
import glob
import threading
import concurrent.futures
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

from converter import (
    BACKEND, DEFAULT_MODELS, CredentialManager,
    USER_AGENT, CLIENT_PRODUCT, CLIENT_IDE_NAME, CLIENT_REQUESTED_WITH, DEFAULT_DOMAIN,
    domain_backend, is_intl_domain,
)

AUTH_DIR = Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"))
KEY_FILE = AUTH_DIR / ".api_key"
CONVERTER_BASE = os.environ.get("CONVERTER_BASE", "http://127.0.0.1:3009")

# 对外 API 地址。面板自身就是 API 入口（把 /v1/* 反代给内网 converter），
# 所以默认同源；只有走独立域名 / 反代时才需要设 PUBLIC_API_BASE。
PUBLIC_API_BASE = os.environ.get("PUBLIC_API_BASE", "").strip().rstrip("/")
API_PORT = os.environ.get("API_PORT", "3009").strip()

# 需要原样透传给 converter 的请求头（鉴权 + 内容协商 + 会话粘性）。
_PROXY_REQ_HEADERS = (
    "authorization", "x-api-key", "content-type", "accept",
    "x-session-id", "x-conversation-id", "conversation-id", "x-thread-id",
    "user-agent", "accept-encoding",
)
# 需要回传给客户端的响应头（保留 SSE 与重试提示）。
_PROXY_RESP_HEADERS = ("content-type", "cache-control", "x-accel-buffering", "retry-after")
# converter 写的真实转发日志（含「哪个账号被调用」），面板据此做统计与日志页
REQLOG_PATH = AUTH_DIR / ".request_log.jsonl"

app = FastAPI(title="workbuddy2api 管理面板", version="1.0")

# 本地面板累计统计（按 .info 文件名）
STATS_LOCK = threading.Lock()
STATS: dict = {}


# ---------------------------------------------------------------------------
# 网关真实流量：请求日志读取 + 按账号聚合
# ---------------------------------------------------------------------------

def _read_reqlog(limit: int = 20000) -> list:
    """读取请求日志，返回「新 → 旧」的列表。"""
    rows = []
    try:
        if REQLOG_PATH.exists():
            lines = REQLOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
            for ln in lines[-limit:]:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    item = json.loads(ln)
                except Exception:
                    continue
                if isinstance(item, dict):
                    item.setdefault("source", "codebuddy")
                    rows.append(item)
    except Exception:
        pass

    rows.sort(key=lambda row: int(row.get("ts") or 0), reverse=True)
    return rows[:limit]


_GW_CACHE = {"ts": 0.0, "data": None}


def gateway_stats(ttl: float = 3.0) -> dict:
    """按账号聚合真实转发流量（带短缓存，避免高频轮询重复解析日志）。"""
    now = time.time()
    if _GW_CACHE["data"] is not None and now - _GW_CACHE["ts"] < ttl:
        return _GW_CACHE["data"]
    rows = _read_reqlog()
    today = time.strftime("%Y-%m-%d")
    per: dict = {}
    for r in rows:
        acct = r.get("acct") or "-"
        st = per.setdefault(acct, {"calls": 0, "ok": 0, "fail": 0, "today": 0, "pt": 0,
                                   "ct": 0, "rt": 0, "last_ts": 0, "last_model": "",
                                   "last_status": ""})
        st["calls"] += 1
        st["pt"] += int(r.get("pt") or 0)
        st["ct"] += int(r.get("ct") or 0)
        st["rt"] += int(r.get("rt") or 0)
        st["ok" if r.get("status") == "ok" else "fail"] += 1
        ts = int(r.get("ts") or 0)
        if ts > st["last_ts"]:
            st["last_ts"] = ts
            st["last_model"] = r.get("model") or ""
            st["last_status"] = r.get("status") or ""
        if ts:
            try:
                if time.strftime("%Y-%m-%d", time.localtime(ts / 1000)) == today:
                    st["today"] += 1
            except Exception:
                pass
    total = {k: sum(s[k] for s in per.values())
             for k in ("calls", "ok", "fail", "today", "pt", "ct", "rt")}
    data = {"accounts": per, "total": total, "window": len(rows)}
    _GW_CACHE["ts"], _GW_CACHE["data"] = now, data
    return data


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def list_info_files():
    return sorted(AUTH_DIR.glob("*.info"))


def is_disabled(path: Path) -> bool:
    return path.name.endswith(".disabled")


def list_all_account_files() -> list:
    """面板展示用：既含启用(.info)也含禁用(.disabled)。"""
    return sorted(AUTH_DIR.glob("*.info")) + sorted(AUTH_DIR.glob("*.disabled"))


def info_summary(p: Path) -> dict:
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        return {"file": p.name, "error": str(e), "disabled": is_disabled(p)}
    auth = d.get("auth") or {}
    acct = d.get("account") or {}
    exp = auth.get("expiresAt", 0)
    expired = time.time() * 1000 >= (exp - 60000)
    with STATS_LOCK:
        stats = STATS.get(p.name, {})
    return {
            "file": p.name,
            "uid": acct.get("uid"),
            "nickname": acct.get("nickname"),
            "enterpriseName": acct.get("enterpriseName"),
            "domain": auth.get("domain"),
            "proxy": d.get("proxy") or "",
            "backend": d.get("backend") or domain_backend(auth.get("domain") or ""),
            "token_expires_at": exp,
            "token_expired": expired,
            "disabled": is_disabled(p),
            "stats": stats,
        }


def current_key() -> str:
    if KEY_FILE.exists():
        return KEY_FILE.read_text(encoding="utf-8").strip()
    return ""


def convert_to_info(item: dict):
    """把 xyw110 导出 JSON（下划线命名）转成 converter 的 .info（驼峰 auth/account）。
    也兼容粘贴完整 .info 形状（含 auth/account 两段）的 JSON。
    不返回 token 明文到日志（这里只是构造 dict）。"""
    # 兼容 .info 原始形状：摊平成下划线字段再走同一逻辑
    if isinstance(item.get("auth"), dict):
        src_auth = item["auth"]
        src_acct = item.get("account") or {}
        item = {
            "access_token": src_auth.get("accessToken", ""),
            "refresh_token": src_auth.get("refreshToken", ""),
            "expires_at": src_auth.get("expiresAt"),
            "domain": src_auth.get("domain"),
            "uid": src_acct.get("uid"),
            "nickname": src_acct.get("nickname"),
            "email": src_acct.get("email"),
            "enterprise_id": src_acct.get("enterpriseId"),
            "enterprise_name": src_acct.get("enterpriseName"),
            "proxy": item.get("proxy"),
            "backend": item.get("backend"),
            "headers": item.get("headers"),
        }
    exp = item.get("expires_at") or item.get("expiresAt")
    if exp is None:
        exp_ms = int((time.time() + 86400 * 30) * 1000)
    else:
        exp_ms = int(exp)
        if exp_ms < 1e12:
            exp_ms = exp_ms * 1000
    out = {
        "auth": {
            "accessToken": item.get("access_token", ""),
            "refreshToken": item.get("refresh_token", ""),
            "expiresAt": exp_ms,
            "domain": item.get("domain") or "www.codebuddy.cn",
            "lastRefreshTime": int(time.time() * 1000),
        },
        "account": {
            "uid": item.get("uid") or "",
            "nickname": item.get("nickname") or item.get("email") or "workbuddy-user",
            "enterpriseId": item.get("enterprise_id") or "",
            "enterpriseName": item.get("enterprise_name") or "",
        },
    }
    # 可选的每账号网络覆盖：proxy（如 http://127.0.0.1:7890）/ backend / headers
    proxy = str(item.get("proxy") or "").strip()
    backend = str(item.get("backend") or "").strip()
    if proxy:
        out["proxy"] = proxy
    if backend:
        out["backend"] = backend
    if isinstance(item.get("headers"), dict) and item["headers"]:
        out["headers"] = item["headers"]
    ident = item.get("email") or item.get("uid") or ("acct" + str(int(time.time())))
    fname = "".join(c if c.isalnum() else "_" for c in str(ident)) + ".info"
    return fname, out


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@app.get("/api/status")
def api_status():
    conv = None
    try:
        with httpx.Client(timeout=5) as c:
            r = c.get(CONVERTER_BASE + "/health")
            conv = r.json()
    except Exception as e:
        conv = {"error": str(e)}
    return {
        "auth_dir": str(AUTH_DIR),
        "account_count": len(list_info_files()),
        "key_set": bool(current_key()),
        "converter": conv,
    }


@app.get("/api/accounts")
def api_accounts():
    files = list_all_account_files()
    accts = [info_summary(p) for p in files]
    gw = gateway_stats()
    for a in accts:                      # 挂上真实转发流量（哪个账号被调了多少次）
        a["gw"] = gw["accounts"].get(a.get("file") or "", None)
    return {"accounts": accts, "enabled_count": len([a for a in accts if not a.get("disabled")]),
            "disabled_count": len([a for a in accts if a.get("disabled")]),
            "gateway_total": gw["total"]}


@app.get("/api/logs")
def api_logs(limit: int = 200, acct: str = "", only_err: bool = False, model: str = ""):
    """请求日志页数据源：真实转发记录。"""
    rows = _read_reqlog()
    if acct:
        wanted = str(acct)
        rows = [r for r in rows if wanted in {
            str(r.get("acct") or ""), str(r.get("nick") or ""), str(r.get("label") or "")
        }]
    if model:
        rows = [r for r in rows if model in (r.get("model") or "")]
    if only_err:
        rows = [r for r in rows if (r.get("status") or "ok") != "ok"]
    limit = max(1, min(int(limit or 200), 2000))
    gw = gateway_stats()
    options = {}
    for row in _read_reqlog():
        value = str(row.get("acct") or "-")
        label = str(row.get("nick") or row.get("label") or value)
        source = "CodeBuddy"
        options.setdefault(value, {"value": value, "label": label, "source": source})
    return {"logs": rows[:limit], "total_matched": len(rows),
            "gateway_total": gw["total"],
            "accounts": sorted(gw["accounts"].keys()),
            "account_options": sorted(options.values(), key=lambda x: x["label"].lower())}


@app.post("/api/logs/clear")
def api_logs_clear():
    """清空请求日志（不影响账号）。"""
    try:
        if REQLOG_PATH.exists():
            REQLOG_PATH.write_text("", encoding="utf-8")
        _GW_CACHE["ts"], _GW_CACHE["data"] = 0.0, None
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"清空失败: {e}")


# ---------------------------------------------------------------------------
# 反向代理：把 /v1/* 与 /health 透传给内网的 converter
#
# 面板（3008）是唯一对外入口；API 进程只监听回环。这样客户端和浏览器
# 用同一个地址，不必再暴露第二个端口。流式 SSE 走 StreamingResponse 透传。
# ---------------------------------------------------------------------------

_PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]


def _proxy_target(path: str) -> str:
    return CONVERTER_BASE.rstrip("/") + path


@app.api_route("/v1/{rest:path}", methods=_PROXY_METHODS, include_in_schema=False)
async def proxy_v1(rest: str, request: Request):
    return await _proxy(request, "/v1/" + rest)


@app.api_route("/health", methods=_PROXY_METHODS, include_in_schema=False)
async def proxy_health(request: Request):
    return await _proxy(request, "/health")


async def _proxy(request: Request, path: str):
    headers = {k: v for k, v in request.headers.items() if k.lower() in _PROXY_REQ_HEADERS}
    # 上游按压缩体发送，httpx 自动解压，这里要避免重复声明编码。
    headers.pop("accept-encoding", None)
    body = await request.body()
    client = httpx.AsyncClient(timeout=httpx.Timeout(600.0, connect=10.0))
    try:
        upstream = await client.send(
            client.build_request(request.method, _proxy_target(path),
                                 headers=headers, content=body),
            stream=True,
        )
    except httpx.HTTPError as exc:
        await client.aclose()
        raise HTTPException(status_code=502, detail={"error": {
            "message": f"上游 API 不可达（{CONVERTER_BASE}）：{exc}",
            "type": "upstream_unreachable",
        }})

    resp_headers = {k: v for k, v in upstream.headers.items()
                    if k.lower() in _PROXY_RESP_HEADERS}
    media_type = upstream.headers.get("content-type", "application/json")

    async def relay():
        try:
            async for chunk in upstream.aiter_raw():
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    return StreamingResponse(relay(), status_code=upstream.status_code,
                             headers=resp_headers, media_type=media_type)


def _resolve_cred_file(raw: str) -> Path:
    # 目录穿越防护：只允许一个 basename，.info 或 .disabled 后缀
    safe = os.path.basename(raw)
    for suffix in (".info", ".disabled"):
        if safe.endswith(suffix):
            return AUTH_DIR / safe
    raise HTTPException(status_code=400, detail="非法文件名")


@app.post("/api/accounts/import")
async def api_import(request: Request):
    data = await request.json()
    raw = data.get("json") or data.get("content") or ""
    if not raw:
        raise HTTPException(status_code=400, detail="缺少 json 字段")
    try:
        obj = json.loads(raw)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"JSON 解析失败: {e}")
    items = obj if isinstance(obj, list) else [obj]
    # 批量级覆盖：导入弹窗里统一选择的代理 / 版本域名（单条里显式给了则优先）
    batch_proxy = str(data.get("proxy") or "").strip()
    batch_domain = str(data.get("domain") or "").strip()
    written = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if batch_proxy and not (item.get("proxy") or "").strip():
            item["proxy"] = batch_proxy
        if batch_domain:
            src = item.get("auth") if isinstance(item.get("auth"), dict) else None
            cur = (src or item).get("domain")
            if not (cur or "").strip():
                item["domain"] = batch_domain
        res = convert_to_info(item)
        if res is None:
            continue
        fname, content = res
        (AUTH_DIR / fname).write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
        written.append(fname)
    return {"written": written, "count": len(written)}


@app.post("/api/accounts/{filename}/settings")
async def api_account_settings(filename: str, request: Request):
    """修改已导入账号的网络字段：proxy / backend / headers（传空字符串 = 清除）。"""
    p = _resolve_cred_file(filename)
    if not p.exists():
        raise HTTPException(status_code=404, detail="账号不存在")
    data = await request.json()
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取账号失败: {e}")
    for key in ("proxy", "backend"):
        if key in data:
            v = str(data.get(key) or "").strip()
            if v:
                doc[key] = v
            else:
                doc.pop(key, None)
    if "headers" in data:
        h = data.get("headers")
        if isinstance(h, dict) and h:
            doc["headers"] = h
        else:
            doc.pop("headers", None)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, p)
    return {"ok": True, "file": p.name,
            "proxy": doc.get("proxy") or "", "backend": doc.get("backend") or ""}


@app.delete("/api/accounts/{filename}")
def api_delete(filename: str):
    # 防目录穿越
    safe = os.path.basename(filename)
    p = AUTH_DIR / safe
    if not p.exists() or not (safe.endswith(".info") or safe.endswith(".disabled")):
        raise HTTPException(status_code=404, detail="账号不存在")
    p.unlink()
    with STATS_LOCK:
        STATS.pop(safe, None)
    return {"ok": True, "deleted": safe}


@app.post("/api/accounts/{filename}/toggle")
def api_toggle(filename: str):
    """启用/禁用账号：通过重命名 .info <-> .disabled 实现。
    converter._scan() 只认 *.info，因此把文件改成 .disabled 后会自动从轮询池剔除
    （目录 mtime 变化触发热加载），无需重启。"""
    p = _resolve_cred_file(filename)
    if not p.exists():
        raise HTTPException(status_code=404, detail="账号不存在")
    if p.name.endswith(".disabled"):
        new = AUTH_DIR / p.name[: -len(".disabled")]  # 还原为 xxx.info
        p.rename(new)
        with STATS_LOCK:
            STATS.pop(p.name, None)
        return {"ok": True, "file": new.name, "disabled": False}
    else:
        new = Path(str(p) + ".disabled")
        p.rename(new)
        with STATS_LOCK:
            STATS.pop(p.name, None)
        return {"ok": True, "file": new.name, "disabled": True}


@app.get("/api/usage")
def api_usage():
    """查询每个账号的真实额度/用量（打到 CodeBuddy billing 计量接口）。

    POST /billing/meter/get-user-resource -> data.Response.Data:
       Accounts[]: CapacitySize(总额) CapacityRemain(剩余) CycleCapacityUsed(本期已用)
                 CycleCapacitySize(本期额度) PackageName CycleStartTime CycleEndTime
    """
    results = {}
    for p in list_info_files():  # 只查启用账号
        fname = p.name
        try:
            cm = CredentialManager(p)
            headers = cm.get_headers()  # 自动刷新 token
        except Exception as e:
            results[fname] = {"error": f"凭据加载/刷新失败: {e}"}
            continue
        billing_base = cm.backend if cm.is_intl else BACKEND
        try:
            kw = {"timeout": 20}
            if cm.proxy:
                kw["proxy"] = cm.proxy
            with httpx.Client(**kw) as c:
                r = c.post(billing_base + "/billing/meter/get-user-resource", headers=headers, json={})
            if r.status_code != 200:
                results[fname] = {"error": f"HTTP {r.status_code}: {r.text[:200]}"}
                continue
            body = r.json()
            data = (body.get("data") or {}).get("Response", {}).get("Data", {})
            accounts = data.get("Accounts") or []
            pkgs = []
            for a in accounts:
                try:
                    pkgs.append({
                        "name": a.get("PackageName"),
                        "total": a.get("CapacitySize"),
                        "remain": a.get("CapacityRemain"),
                        "cycle_used": a.get("CycleCapacityUsed"),
                        "cycle_total": a.get("CycleCapacitySize"),
                        "cycle_start": a.get("CycleStartTime"),
                        "cycle_end": a.get("CycleEndTime"),
                        "unit": a.get("CapacityUnit"),
                    })
                except Exception:
                    continue
            # 汇总
            results[fname] = {
                "ok": True,
                "nickname": json.loads(p.read_text()).get("account", {}).get("nickname", ""),
                "pkgs": pkgs,
                "total_count": data.get("TotalCount"),
                "total_dosage": data.get("TotalDosage"),
                "grand_remain": sum((x.get("remain") or 0) for x in pkgs),
                "grand_total": sum((x.get("total") or 0) for x in pkgs),
            }
        except Exception as e:
            results[fname] = {"error": f"{type(e).__name__}: {e}"}
    return {"results": results}


@app.get("/api/key")
def api_get_key():
    k = current_key()
    if not k:
        return {"set": False}
    masked = (k[:4] + "****" + k[-4:]) if len(k) > 8 else "****"
    return {"set": True, "masked": masked}


@app.post("/api/key")
async def api_set_key(request: Request):
    data = await request.json()
    k = (data.get("key") or "").strip()
    KEY_FILE.write_text(k + "\n", encoding="utf-8")
    return {"ok": True, "set": bool(k)}


@app.get("/api/models")
def api_models():
    try:
        with httpx.Client(timeout=5) as c:
            r = c.get(CONVERTER_BASE + "/v1/models")
            if r.status_code == 200:
                ids = [m["id"] for m in r.json().get("data", [])]
                return {"models": ids, "source": "converter"}
    except Exception:
        pass
    return {"models": stored_models() or DEFAULT_MODELS, "source": "static"}


def _probe_model(model: str) -> dict:
    """经 converter 发最小流式请求判真实可用性。

    注意：converter 对上游 4xx 返回 HTTP 200 + SSE 流内 data:{"error":...} 事件，
    因此除状态码外必须扫描流内 error 事件，否则伪造模型名也会被判可用。
    """
    payload = {"model": model, "messages": [{"role": "user", "content": "hi"}],
               "max_tokens": 1, "stream": True}
    key = current_key()
    headers = {"Authorization": "Bearer " + key} if key else {}
    try:
        with httpx.Client(timeout=30) as c:
            with c.stream("POST", CONVERTER_BASE + "/v1/chat/completions",
                          json=payload, headers=headers) as r:
                if r.status_code != 200:
                    try:
                        body = r.read().decode("utf-8", "replace")
                    except Exception:
                        body = ""
                    return {"ok": False, "status": r.status_code, "preview": body[:160]}
                scanned = 0
                for line in r.iter_lines():
                    if not line:
                        continue
                    if '"error"' in line:
                        return {"ok": False, "status": 200, "preview": line[:160]}
                    scanned += 1
                    if scanned >= 20:
                        break
                return {"ok": True, "status": 200, "preview": ""}
    except Exception as e:
        return {"ok": False, "status": 0, "preview": str(e)[:160]}


@app.post("/api/models/probe")
async def api_probe_models(request: Request):
    data = await request.json()
    models = data.get("models") or DEFAULT_MODELS
    results = {}
    for m in models:
        results[m] = _probe_model(m)
    return {"results": results}


# ---------------------------------------------------------------------------
# 模型清单管理（持久化到 auth 卷 models.json；纯手动，无自动同步）
# ---------------------------------------------------------------------------

def models_file() -> Path:
    return AUTH_DIR / "models.json"


def stored_models() -> list | None:
    try:
        data = json.loads(models_file().read_text(encoding="utf-8"))
    except Exception:
        return None
    ms = data.get("models") if isinstance(data, dict) else data
    if isinstance(ms, list) and ms:
        out, seen = [], set()
        for m in ms:
            m = str(m).strip()
            if m and m not in seen:
                seen.add(m)
                out.append(m)
        return out or None
    return None


def write_models(models: list) -> list:
    seen, out = set(), []
    for m in models:
        m = str(m).strip()
        if m and m not in seen:
            seen.add(m)
            out.append(m)
    if not out:
        raise HTTPException(status_code=400, detail="models 不能为空")
    p = models_file()
    tmp = p.with_name(".models.json.tmp")
    tmp.write_text(json.dumps({"models": out, "updatedAt": int(time.time() * 1000)},
                              ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return out


@app.post("/api/models/set")
async def api_models_set(request: Request):
    """整体覆盖模型清单。删除=传删除后的列表；添加=传追加后的列表。写 models.json，converter 即时生效。"""
    data = await request.json()
    models = data.get("models")
    if not isinstance(models, list):
        raise HTTPException(status_code=400, detail="models 必须是列表")
    out = write_models(models)
    return {"ok": True, "models": out, "count": len(out)}


# 「发现新模型」候选池：上游无模型列表接口，新模型只能靠候选名探测发现
DISCOVERY_CANDIDATES = [
    "glm-5.3", "glm-5.2", "glm-5.1", "glm-5v-turbo", "glm-5.2-agent", "glm-5-air",
    "glm-5.3v", "glm-5.3-agent", "glm-5.2-turbo",
    "kimi-k3", "kimi-k3.5", "kimi-k4", "kimi-k2.7", "kimi-k2.6", "kimi-k2.5",
    "kimi-k2.8", "kimi-k3-think", "kimi-k3-vision",
    "deepseek-v4-pro", "deepseek-v4-flash", "deepseek-v5", "deepseek-v4.5",
    "deepseek-v4-agent", "deepseek-r2",
    "minimax-m3-pay", "minimax-m3", "minimax-m4", "minimax-m3.5",
    "hy4-preview", "hy3-preview", "hy3-preview-agent", "hy4.5-preview",
    "hunyuan-t1-vision", "hunyuan-4", "hunyuan-turbos-latest", "hy4-agent",
    "qwen4-max", "qwen3.8-flash", "qwen3-max",
    "ernie-5.0", "step-3", "doubao-seed-2.0",
    "gpt-5", "gpt-5.2", "claude-sonnet-4-5", "gemini-3-pro", "auto",
]


@app.post("/api/models/discover")
async def api_models_discover(request: Request):
    """手动拉取：对候选池逐个发最小请求（max_tokens=1），返回真实可用候选，不改清单。"""
    try:
        data = await request.json()
    except Exception:
        data = {}
    extra = [str(m).strip() for m in (data.get("candidates") or []) if str(m).strip()]
    pool = []
    for m in (stored_models() or DEFAULT_MODELS) + DISCOVERY_CANDIDATES + extra:
        if m not in pool:
            pool.append(m)

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        # _probe_model 返回 dict（{"ok","status","preview"}），直接按模型名配对即可
        results = dict(zip(pool, ex.map(_probe_model, pool)))
    stored = stored_models() or []
    new_ok = [m for m, r in results.items() if r["ok"] and m not in stored]
    return {"ok": True, "tested": len(pool), "results": results,
            "stored": stored, "newOk": new_ok}


def probe_one_account(path: Path) -> dict:
    base = info_summary(path)
    try:
        cm = CredentialManager(path)
        headers = cm.get_headers()  # 自动刷新（若过期）
    except Exception as e:
        base["probe_ok"] = False
        base["probe_error"] = f"凭据加载/刷新失败: {e}"
        return base
    body = {"model": "auto", "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 1, "stream": True, "stream_options": {"include_usage": True}}
    if cm.is_intl and (not body["messages"] or body["messages"][0].get("role") != "system"):
        body["messages"].insert(0, {"role": "system", "content": "You are a helpful assistant."})
    try:
        kw = {"timeout": 40}
        if cm.proxy:
            kw["proxy"] = cm.proxy
        with httpx.Client(**kw) as c:
            r = c.post(cm.backend + "/v2/chat/completions", headers=headers, json=body)
        ok = r.status_code == 200
        preview = r.text[:160]
        base["probe_ok"] = ok
        base["probe_status"] = r.status_code
        base["probe_preview"] = preview
        with STATS_LOCK:
            s = STATS.setdefault(path.name, {"calls": 0, "ok": 0, "fail": 0})
            s["calls"] += 1
            if ok:
                s["ok"] += 1
                s["last_ok_ts"] = int(time.time())
            else:
                s["fail"] += 1
                s["last_err"] = preview
    except Exception as e:
        base["probe_ok"] = False
        base["probe_error"] = str(e)[:200]
        with STATS_LOCK:
            s = STATS.setdefault(path.name, {"calls": 0, "ok": 0, "fail": 0})
            s["calls"] += 1
            s["fail"] += 1
            s["last_err"] = str(e)[:200]
    return base


@app.post("/api/quota/probe")
def api_probe_quota():
    results = [probe_one_account(p) for p in list_info_files()]
    return {"results": results,
            "note": "腾讯 CodeBuddy 后端未提供公开配额接口，此结果为探测式可用性（直接以账号 token 打后端发送最小请求），并不代表精确剩余额度。"}


@app.get("/api/token-stats")
def api_token_stats():
    """近 30 天 token 用量（读 converter 落盘的 .token_stats.jsonl，按天聚合）。"""
    from collections import defaultdict
    path = AUTH_DIR / ".token_stats.jsonl"
    days = defaultdict(lambda: {"calls": 0, "pt": 0, "ct": 0})
    models = defaultdict(lambda: {"calls": 0, "tokens": 0})
    if path.exists():
        now = time.time()
        with path.open(encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if now - r.get("ts", 0) > 86400 * 31:
                    continue
                key = time.strftime("%m-%d", time.localtime(r.get("ts", 0)))
                d = days[key]
                d["calls"] += 1
                d["pt"] += r.get("pt", 0) or 0
                d["ct"] += r.get("ct", 0) or 0
                m = models[r.get("model") or "?"]
                m["calls"] += 1
                m["tokens"] += (r.get("pt", 0) or 0) + (r.get("ct", 0) or 0)
    out = []
    for i in range(29, -1, -1):
        key = time.strftime("%m-%d", time.localtime(time.time() - i * 86400))
        v = days.get(key, {"calls": 0, "pt": 0, "ct": 0})
        out.append({"date": key, "calls": v["calls"], "pt": v["pt"],
                    "ct": v["ct"], "tokens": v["pt"] + v["ct"]})
    top = sorted(models.items(), key=lambda kv: -kv[1]["tokens"])[:6]
    return {"days": out,
            "total_tokens": sum(x["tokens"] for x in out),
            "total_calls": sum(x["calls"] for x in out),
            "models": [{"model": k, "calls": v["calls"], "tokens": v["tokens"]} for k, v in top]}


@app.get("/api/poll")
def api_poll():
    try:
        with httpx.Client(timeout=5) as c:
            r = c.get(CONVERTER_BASE + "/health")
            j = r.json()
            return {"account_count": j.get("account_count"),
                    "multi_account": j.get("multi_account"),
                    "accounts": j.get("accounts")}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# 前端
# ---------------------------------------------------------------------------

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>WorkBuddy 控制台</title>
<style>
/* ============================================================
   Midnight v2 — 曜石深色控制台（附白天模式）
   近黑蓝底 / 发丝线 / 单一冰蓝强调 / 卡片化账号与模型
   ============================================================ */
:root{
  --bg:#0a0a0c;
  --bg-glow:linear-gradient(180deg, rgba(255,255,255,.028), transparent 340px);
  --panel:rgba(255,255,255,.024);
  --panel-solid:#131316;
  --panel2:rgba(255,255,255,.055);
  --card-hi:rgba(255,255,255,.04);
  --line:rgba(255,255,255,.07);
  --line2:rgba(255,255,255,.16);
  --fg:#eeeeef;
  --fg2:#bbbec6;
  --muted:#84878f;
  --faint:#54565c;
  --acc:#c9cdd6;
  --acc2:#e4e6ea;
  --acc-ink:#121316;
  --acc-dim:rgba(255,255,255,.07);
  --acc-line:rgba(255,255,255,.26);
  --ok:#3fd68f;      --ok-dim:rgba(63,214,143,.10);
  --warn:#e5b45f;    --warn-dim:rgba(229,180,95,.10);
  --danger:#ef6f5e;  --danger-dim:rgba(239,111,94,.10);
  --term-bg:#08080a;
  --skel-a:rgba(255,255,255,.035);
  --skel-b:rgba(255,255,255,.085);
  --shadow:0 30px 80px rgba(0,0,0,.6);
  --hover:rgba(255,255,255,.035);
  --mono:'JetBrains Mono','Cascadia Code',ui-monospace,Consolas,monospace;
  --sans:-apple-system,BlinkMacSystemFont,'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif;
  --r:14px;
  --sbw:220px;
}
[data-theme="light"]{
  --bg:#eef0f4;
  --bg-glow:radial-gradient(1000px 420px at 75% -10%, rgba(61,99,231,.06), transparent 62%);
  --panel:#ffffff;
  --panel-solid:#ffffff;
  --panel2:#f2f4f8;
  --card-hi:#ffffff;
  --line:rgba(18,24,40,.09);
  --line2:rgba(18,24,40,.2);
  --fg:#141824;
  --fg2:#3b4256;
  --muted:#6a7284;
  --faint:#98a0b2;
  --acc:#3559e0;
  --acc2:#2a49c0;
  --acc-ink:#ffffff;
  --acc-dim:rgba(53,89,224,.09);
  --acc-line:rgba(53,89,224,.4);
  --ok:#0f9d63;      --ok-dim:rgba(15,157,99,.09);
  --warn:#b07a14;    --warn-dim:rgba(176,122,20,.1);
  --danger:#d0442f;  --danger-dim:rgba(208,68,47,.08);
  --term-bg:#f7f8fb;
  --skel-a:rgba(20,28,50,.05);
  --skel-b:rgba(20,28,50,.11);
  --shadow:0 30px 80px rgba(30,40,70,.18);
  --hover:rgba(20,28,50,.045);
}
*{box-sizing:border-box}
html,body{height:100%}
body{
  margin:0;background:var(--bg-glow),var(--bg);background-attachment:fixed;
  color:var(--fg);font-family:var(--sans);font-size:13px;line-height:1.6;
  -webkit-font-smoothing:antialiased;display:flex;overflow:hidden;
}
[data-theme="light"] body{background:var(--bg-glow),var(--bg)}
::selection{background:rgba(124,154,255,.3)}
::-webkit-scrollbar{width:8px;height:8px}
::-webkit-scrollbar-thumb{background:var(--panel2);border-radius:8px}
::-webkit-scrollbar-thumb:hover{background:var(--line2)}
::-webkit-scrollbar-track{background:transparent}
a{color:var(--acc);text-decoration:none}
button{font-family:inherit;font-size:13px;cursor:pointer;color:var(--fg);
  background:var(--panel2);border:1px solid var(--line);border-radius:9px;
  padding:6.5px 14px;transition:all .16s ease}
button:hover{border-color:var(--line2);background:var(--hover)}
button:focus-visible,input:focus-visible,textarea:focus-visible,select:focus-visible{
  outline:2px solid var(--acc-line);outline-offset:1px}
button:disabled{opacity:.4;cursor:not-allowed}
button.primary{background:var(--acc);border-color:transparent;color:var(--acc-ink);font-weight:600}
button.primary:hover{background:var(--acc2)}
button.danger{background:transparent;border-color:var(--danger-dim);color:var(--danger)}
button.danger:hover{background:var(--danger-dim);border-color:var(--danger)}
button.sm{padding:3.5px 10px;font-size:12px;border-radius:8px}
button.ghost{background:transparent}
.icon-btn{display:inline-flex;align-items:center;justify-content:center;width:31px;height:31px;padding:0;border-radius:9px;color:var(--muted)}
.icon-btn:hover{color:var(--fg)}
.icon-btn svg{width:15px;height:15px}
.mono{font-family:var(--mono);font-variant-numeric:tabular-nums}
.muted{color:var(--muted)}
.faint{color:var(--faint)}
.grow{flex:1}
.mt8{margin-top:8px}.mt12{margin-top:12px}.mt16{margin-top:16px}

/* ---------- 侧边栏 ---------- */
aside{
  width:var(--sbw);flex:0 0 var(--sbw);display:flex;flex-direction:column;
  border-right:1px solid var(--line);
}
.brand{display:flex;align-items:center;gap:11px;padding:22px 20px 18px}
.brand .mark{width:28px;height:28px;border-radius:9px;flex:0 0 28px;
  background:linear-gradient(140deg,#484b53,#24262b);
  border:1px solid rgba(255,255,255,.12);
  display:flex;align-items:center;justify-content:center;
  color:#f2f3f5;font-weight:700;font-size:13px;font-family:var(--mono)}
[data-theme="light"] .brand .mark{color:#fff}
.brand .t{font-weight:700;font-size:13.5px;letter-spacing:.5px}
.brand .s{font-size:10px;color:var(--faint);letter-spacing:1px;margin-top:1px}
nav{padding:4px 12px;flex:1}
.nav-item{display:flex;align-items:center;gap:11px;padding:9px 12px;border-radius:10px;
  color:var(--muted);cursor:pointer;margin-bottom:2px;transition:all .16s;
  font-size:13px;user-select:none;position:relative}
.nav-item svg{width:15.5px;height:15.5px;flex:0 0 15.5px}
.nav-item:hover{color:var(--fg2);background:var(--hover)}
.nav-item.active{color:var(--fg);background:var(--acc-dim)}
.nav-item.active::before{content:'';position:absolute;left:-12px;top:20%;bottom:20%;width:2.5px;
  border-radius:3px;background:var(--acc)}
.nav-item.active svg{color:var(--acc)}
.sb-foot{padding:16px;border-top:1px solid var(--line)}
.sb-status{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--muted)}
.sb-ver{font-size:10px;color:var(--faint);margin-top:7px;letter-spacing:.8px}

.lamp{width:7px;height:7px;border-radius:50%;background:var(--faint);display:inline-block;flex:0 0 7px}
.lamp.ok{background:var(--ok);box-shadow:0 0 9px var(--ok)}
.lamp.bad{background:var(--danger);box-shadow:0 0 9px var(--danger)}
.lamp.mid{background:var(--warn)}
.lamp.spin{background:var(--warn);animation:pulse 1.1s ease-in-out infinite}
@keyframes pulse{50%{opacity:.25}}

/* ---------- 主区 ---------- */
main{flex:1;min-width:0;display:flex;flex-direction:column}
header#topbar{
  height:58px;flex:0 0 58px;display:flex;align-items:center;gap:14px;
  padding:0 28px;border-bottom:1px solid var(--line);
}
#page-title{font-size:16px;font-weight:650;margin:0;letter-spacing:.2px}
.tb-right{margin-left:auto;display:flex;align-items:center;gap:10px}
#tb-hint{font-size:12px;color:var(--faint)}
#content{flex:1;overflow-y:auto;padding:26px 30px 56px}
.page{max-width:1220px;margin:0 auto}

/* ---------- 卡片 ---------- */
.card{
  background:linear-gradient(180deg,var(--card-hi),transparent 130%);
  background-color:var(--panel);
  border:1px solid var(--line);border-radius:var(--r);padding:20px;
  transition:border-color .18s,transform .18s,box-shadow .18s;
}
.card.clickable{cursor:pointer}
.card.clickable:hover{border-color:var(--line2);transform:translateY(-2px);
  box-shadow:0 12px 34px rgba(0,0,0,.35)}
[data-theme="light"] .card.clickable:hover{box-shadow:0 12px 30px rgba(30,40,70,.12)}
.card h3{margin:0 0 14px;font-size:10.5px;font-weight:650;letter-spacing:1.8px;color:var(--muted);
  text-transform:uppercase;display:flex;align-items:center;gap:8px}
.card h3 .badge{margin-left:auto;text-transform:none;letter-spacing:0}
.big-num{font-family:var(--mono);font-variant-numeric:tabular-nums;font-size:32px;font-weight:250;letter-spacing:-1px;line-height:1.2}
.grid{display:grid;gap:14px}
.cards4{grid-template-columns:repeat(4,1fr)}
@media(max-width:1080px){.cards4{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.cards4{grid-template-columns:1fr}}

/* ---------- 工具条 ---------- */
.toolbar{display:flex;align-items:center;gap:10px;margin-bottom:16px;flex-wrap:wrap}
input,select,textarea{
  font-family:inherit;font-size:13px;color:var(--fg);background:var(--panel);
  border:1px solid var(--line2);border-radius:9px;padding:6.5px 11px;
  transition:border-color .16s}
input::placeholder,textarea::placeholder{color:var(--faint)}
input:hover,select:hover,textarea:hover{border-color:var(--acc-line)}
select{cursor:pointer}
textarea.big{min-height:190px;resize:vertical;line-height:1.55}
.seg{display:inline-flex;background:var(--panel);border:1px solid var(--line);border-radius:9px;padding:2.5px;gap:2px}
.seg button{border:0;background:transparent;padding:4px 12px;font-size:12px;color:var(--muted);border-radius:7px}
.seg button:hover{color:var(--fg);background:var(--hover)}
.seg button.on{background:var(--acc-dim);color:var(--acc)}
.seg button b{font-weight:600;margin-left:3px;opacity:.75}

/* ---------- 徽章 ---------- */
.badge{display:inline-flex;align-items:center;gap:5px;padding:2px 9px;border-radius:20px;
  font-size:11px;font-family:var(--mono);white-space:nowrap}
.badge.ok{background:var(--ok-dim);color:var(--ok)}
.badge.warn{background:var(--warn-dim);color:var(--warn)}
.badge.bad{background:var(--danger-dim);color:var(--danger)}
.badge.mid{background:var(--panel2);color:var(--muted)}
.badge.acc{background:var(--acc-dim);color:var(--acc)}
.status-pill{display:inline-flex;align-items:center;gap:7px;font-size:12px;color:var(--fg2)}

/* ---------- 实体卡片（账号 / 模型）---------- */
.cards-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
@media(max-width:1150px){.cards-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:680px){.cards-grid{grid-template-columns:1fr}}
.entity{
  background:linear-gradient(180deg,var(--card-hi),transparent 140%);
  background-color:var(--panel);
  border:1px solid var(--line);border-radius:var(--r);padding:16px 18px;
  transition:border-color .18s,transform .18s,box-shadow .18s,opacity .18s;
  display:flex;flex-direction:column;gap:9px;min-width:0;
}
.entity:hover{border-color:var(--line2);transform:translateY(-2px);box-shadow:0 10px 30px rgba(0,0,0,.3)}
[data-theme="light"] .entity:hover{box-shadow:0 10px 26px rgba(30,40,70,.1)}
.entity.off{opacity:.55}
.entity.probing{border-color:var(--acc-line);box-shadow:0 0 0 1px var(--acc-line),0 8px 30px rgba(124,154,255,.12)}
.e-head{display:flex;align-items:center;gap:9px;min-width:0}
.e-title{font-weight:600;font-size:13.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
.e-sub{font-family:var(--mono);font-size:11px;color:var(--muted);display:flex;align-items:center;gap:6px;
  overflow:hidden;white-space:nowrap}
.e-sub .uid-cell{max-width:200px}
.e-line{display:flex;justify-content:space-between;gap:12px;font-size:12px;padding:1px 0}
.e-line .k{color:var(--faint);flex:0 0 auto}
.e-line .v{font-family:var(--mono);color:var(--fg2);text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.e-foot{display:flex;align-items:center;gap:8px;margin-top:2px;padding-top:11px;border-top:1px solid var(--line)}
.e-detail{font-family:var(--mono);font-size:11px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;
  white-space:nowrap;min-height:17px}
.e-detail.err{color:var(--danger)}

/* ---------- 表格（用量） ---------- */
.tbl-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:13px}
th{font-size:10.5px;font-weight:650;letter-spacing:1.4px;text-transform:uppercase;
  color:var(--faint);text-align:left;padding:9px 14px;border-bottom:1px solid var(--line2);white-space:nowrap}
td{padding:10px 14px;border-bottom:1px solid var(--line);vertical-align:middle;white-space:nowrap}
tbody tr{transition:background .12s}
tbody tr:hover{background:var(--hover)}
tbody tr:last-child td{border-bottom:0}
tr.warn-row td{background:var(--warn-dim)}
tr.row-exp td{background:transparent}
.pager{display:flex;align-items:center;gap:8px;padding:11px 14px;font-size:12px;color:var(--muted);border-top:1px solid var(--line)}
.pager .grow{flex:1}
.uid-cell{font-family:var(--mono);font-size:12px;display:inline-flex;align-items:center;gap:5px;max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;vertical-align:middle}
.copy-btn{border:0;background:transparent;color:var(--faint);padding:1px 3px;font-size:12px;line-height:1}
.copy-btn:hover{color:var(--acc);background:transparent}
.empty{padding:48px 20px;text-align:center;color:var(--muted)}
.empty .big{font-size:26px;color:var(--faint);margin-bottom:8px}
.flex{display:flex;align-items:center;gap:10px}

/* ---------- 开关 ---------- */
.switch{position:relative;display:inline-block;width:30px;height:17px;vertical-align:middle}
.switch input{opacity:0;width:0;height:0;position:absolute}
.switch .tr{position:absolute;inset:0;border-radius:20px;background:var(--panel2);transition:.18s;cursor:pointer}
.switch .tr::before{content:'';position:absolute;width:13px;height:13px;border-radius:50%;left:2px;top:2px;background:var(--muted);transition:.18s}
.switch input:checked + .tr{background:var(--acc-dim);box-shadow:inset 0 0 0 1px var(--acc-line)}
.switch input:checked + .tr::before{transform:translateX(12px);background:var(--acc)}
.switch input:focus-visible + .tr{outline:2px solid var(--acc-line);outline-offset:1px}

/* ---------- 进度条 ---------- */
.pbar{height:4px;border-radius:4px;background:var(--panel2);overflow:hidden;min-width:110px}
.pbar i{display:block;height:100%;border-radius:4px;transition:width .4s ease}
.c-acc{background:var(--acc)}
.c-warn{background:var(--warn)}
.c-bad{background:var(--danger)}

/* ---------- 弹窗 ---------- */
#overlay{position:fixed;inset:0;background:rgba(2,3,7,.62);backdrop-filter:blur(10px);
  display:none;align-items:flex-start;justify-content:center;z-index:60;padding:9vh 16px 16px}
[data-theme="light"] #overlay{background:rgba(40,50,80,.35)}
#overlay.show{display:flex}
.modal{background:var(--panel-solid);border:1px solid var(--line2);border-radius:16px;width:520px;max-width:100%;
  max-height:82vh;display:flex;flex-direction:column;box-shadow:var(--shadow);
  animation:mIn .17s ease}
@keyframes mIn{from{opacity:0;transform:translateY(10px) scale(.98)}to{opacity:1;transform:none}}
.m-head{display:flex;align-items:center;padding:18px 20px 0}
.m-title{font-size:14.5px;font-weight:650;flex:1}
.m-close{border:0;background:transparent;color:var(--faint);font-size:16px;padding:2px 8px}
.m-close:hover{color:var(--fg);background:transparent}
.m-body{padding:12px 20px 4px;overflow-y:auto}
.m-body label{display:block;font-size:12px;color:var(--muted);margin-bottom:7px}
.m-body input,.m-body textarea,.m-body select{width:100%}
.m-foot{display:flex;justify-content:flex-end;gap:9px;padding:14px 20px 18px}

/* ---------- Toast ---------- */
#toasts{position:fixed;right:20px;bottom:20px;z-index:80;display:flex;flex-direction:column;gap:8px}
.toast{background:var(--panel-solid);border:1px solid var(--line2);border-left:2px solid var(--acc);
  color:var(--fg);border-radius:10px;padding:9px 15px;font-size:12.5px;max-width:380px;
  box-shadow:var(--shadow);animation:tIn .18s ease}
.toast.ok{border-left-color:var(--ok)}
.toast.warn{border-left-color:var(--warn)}
.toast.err{border-left-color:var(--danger)}
.toast.out{opacity:0;transform:translateY(6px);transition:.28s}
@keyframes tIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}

/* ---------- 骨架屏 ---------- */
.skel{background:linear-gradient(90deg,var(--skel-a) 25%,var(--skel-b) 45%,var(--skel-a) 65%);
  background-size:220% 100%;animation:shk 1.3s linear infinite;border-radius:6px}
.skel-line{height:13px;margin:11px 12px}
.skel-card{height:150px;margin:0;border-radius:var(--r)}
@keyframes shk{to{background-position:-220% 0}}

/* ---------- 其它 ---------- */
.btn-spin{display:inline-block;width:11px;height:11px;border:1.5px solid transparent;
  border-top-color:currentColor;border-right-color:currentColor;border-radius:50%;
  animation:rot .7s linear infinite;vertical-align:-1.5px;margin-right:6px}
@keyframes rot{to{transform:rotate(360deg)}}
.kv{display:flex;justify-content:space-between;gap:14px;padding:3px 0;font-size:12.5px}
.kv .k{color:var(--muted)}
.kv .v{font-family:var(--mono);text-align:right}
.hint{font-size:12px;color:var(--faint)}
.badge-row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-left:auto}

/* ---------- 总览扩展区 ---------- */
.quota-wrap{display:flex;gap:24px;align-items:stretch}
.quota-sum{flex:0 0 240px;display:flex;flex-direction:column;gap:4px;
  border-right:1px solid var(--line);padding-right:24px}
.quota-sum .cap{font-size:11px;letter-spacing:1.6px;color:var(--muted);text-transform:uppercase;font-weight:650}
.quota-sum .big-num{font-size:44px;font-weight:200;letter-spacing:-2px;line-height:1.15;margin:4px 0 2px}
.quota-accts{flex:1;display:flex;flex-wrap:wrap;gap:12px;align-content:flex-start}
.quota-acct{flex:1 1 220px;max-width:310px;border:1px solid var(--line);border-radius:12px;
  padding:13px 16px;display:flex;flex-direction:column;gap:7px;background:var(--panel);
  transition:border-color .16s}
.quota-acct:hover{border-color:var(--line2)}
.quota-acct .row1{display:flex;align-items:center;gap:8px;min-width:0}
.quota-acct .row1 .nm{font-weight:600;font-size:12.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0;flex:1}
.quota-acct .row2{display:flex;align-items:baseline;gap:6px}
.quota-acct .row2 .num{font-family:var(--mono);font-size:23px;font-weight:300;font-variant-numeric:tabular-nums}
.quota-acct .row2 .of{font-family:var(--mono);font-size:11.5px;color:var(--muted)}
.ep-row{display:flex;align-items:center;gap:10px;padding:7.5px 0;border-bottom:1px solid var(--line)}
.ep-row:last-child{border-bottom:0}
.ep-tag{font-size:10px;font-family:var(--mono);font-weight:700;padding:1.5px 7px;border-radius:6px;background:var(--acc-dim);color:var(--acc);flex:0 0 auto;letter-spacing:.4px}
.ep-path{font-family:var(--mono);font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.code-block{position:relative;background:var(--term-bg);border:1px solid var(--line);border-radius:10px;
  padding:13px 44px 13px 15px;font-family:var(--mono);font-size:11.5px;line-height:1.75;
  color:var(--fg2);overflow-x:auto;white-space:pre;margin-top:10px}
.code-copy{position:absolute;top:7px;right:7px}
.mchip{display:inline-flex;align-items:center;font-family:var(--mono);font-size:11px;padding:2.5px 9px;
  border-radius:7px;background:var(--panel2);border:1px solid var(--line);color:var(--fg2);
  cursor:pointer;transition:all .14s;user-select:none}
.mchip:hover{border-color:var(--acc-line);color:var(--acc)}
.mchip.ok{border-color:transparent;background:var(--ok-dim);color:var(--ok)}
.mchip.bad{border-color:transparent;background:var(--danger-dim);color:var(--danger)}
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{display:inline-flex;align-items:center;gap:5px;padding:4px 8px;border:1px solid var(--line);
  border-radius:7px;color:var(--muted);font-size:11px;line-height:1.2;background:var(--panel)}
.chip.ok{border-color:var(--ok-dim);background:var(--ok-dim);color:var(--ok)}
.progress{height:6px;border-radius:6px;background:var(--panel2);overflow:hidden}
.progress i{display:block;height:100%;border-radius:inherit;background:var(--ok);transition:width .2s}
.acct-cols{display:grid;grid-template-columns:1fr 1fr;column-gap:28px;align-content:start}
@media(max-width:880px){.acct-cols{grid-template-columns:1fr}}
.acc-grid{display:grid;grid-template-columns:1fr 1.15fr;gap:22px;align-items:start}
@media(max-width:960px){.acc-grid{grid-template-columns:1fr}}
.acc-grid .code-block{margin-top:0}
.env-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px 30px}
@media(max-width:960px){.env-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.env-grid{grid-template-columns:1fr}}
.acct-scroll{max-height:260px;overflow-y:auto}
.acct-line{display:flex;align-items:center;gap:11px;padding:9px 8px;border-bottom:1px solid var(--line);cursor:pointer;border-radius:8px;transition:background .13s}
.acct-line:hover{background:var(--hover)}
.acct-cols .acct-line:last-child{border-bottom:0}
.acct-line .nm{font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0;flex:1}
.acct-line .meta{font-family:var(--mono);font-size:11.5px;color:var(--muted);white-space:nowrap}

/* ---------- Token 统计图 ---------- */
.tk-head{display:flex;align-items:flex-end;gap:22px;flex-wrap:wrap}
.tk-head .num{font-family:var(--mono);font-size:34px;font-weight:200;font-variant-numeric:tabular-nums;letter-spacing:-1px;line-height:1.1}
.tk-chart{display:flex;align-items:flex-end;gap:3px;height:132px;margin-top:16px}
.tk-chart .bar{flex:1 1 0;min-width:3px;border-radius:3px 3px 1px 1px;background:var(--panel2);
  min-height:2px;position:relative;cursor:default;transition:background .13s}
.tk-chart .bar:hover{background:var(--acc)}
.tk-chart .bar.today{background:var(--fg2)}
.tk-chart .bar.today:hover{background:var(--acc)}
.tk-axis{display:flex;justify-content:space-between;margin-top:7px;font-family:var(--mono);
  font-size:10.5px;color:var(--faint)}
.tk-empty{padding:34px 0 10px;text-align:center;color:var(--muted);font-size:12.5px}

@media(max-width:940px){
  :root{--sbw:60px}
  .brand .t,.brand .s,.nav-item .lb,.sb-status span,.sb-ver{display:none}
  .brand{justify-content:center;padding:18px 0 12px}
  .nav-item{justify-content:center;padding:11px 0}
  .sb-foot{display:flex;justify-content:center;padding:14px 8px}
  .nav-item.active::before{display:none}
}
@media(max-width:640px){
  #content{padding:16px 14px 44px}
  header#topbar{padding:0 16px}
  #tb-hint{display:none}
}
</style>
</head>
<body>

<aside>
  <div class="brand">
    <div class="mark">C</div>
    <div><div class="t">WB2API</div><div class="s">CONSOLE</div></div>
  </div>
  <nav id="nav"></nav>
  <div class="sb-foot">
    <div class="sb-status"><span class="lamp" id="sb-lamp"></span><span id="sb-state">检测中…</span></div>
    <div class="sb-ver">MIDNIGHT · V2</div>
  </div>
</aside>

<main>
  <header id="topbar">
    <h1 id="page-title">总览</h1>
    <div class="tb-right">
      <span id="tb-hint"></span>
      <button class="icon-btn" id="theme-btn" title="切换白天 / 深色模式"></button>
      <button class="icon-btn" id="refresh-btn" title="刷新当前页">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><path d="M21 3v6h-6"/></svg>
      </button>
    </div>
  </header>
  <div id="content"></div>
</main>

<div id="toasts"></div>

<div id="overlay">
  <div class="modal">
    <div class="m-head">
      <div class="m-title" id="m-title"></div>
      <button class="m-close" id="m-close" title="关闭">✕</button>
    </div>
    <div class="m-body" id="m-body"></div>
    <div class="m-foot" id="m-foot"></div>
  </div>
</div>

<script>
/* ============================================================
   基础工具
   ============================================================ */
const $  = s => document.querySelector(s);
const $$ = s => Array.from(document.querySelectorAll(s));
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmtNum = n => (n==null||isNaN(n)) ? '—' : Number(n).toLocaleString('zh-CN');

function relTime(ms){
  if(!ms) return '—';
  const d = new Date(ms);
  if(isNaN(d.getTime())) return '—';
  let s = (Date.now() - ms)/1000;
  const suffix = s >= 0 ? '前' : '后';
  s = Math.abs(s);
  let txt;
  if(s < 60) txt = Math.floor(s)+' 秒';
  else if(s < 3600) txt = Math.floor(s/60)+' 分钟';
  else if(s < 86400) txt = Math.floor(s/3600)+' 小时';
  else txt = Math.floor(s/86400)+' 天';
  return `<span class="mono" title="${esc(d.toLocaleString('zh-CN'))}">${txt}${suffix}</span>`;
}
function relTimeText(ms){
  const html = relTime(ms);
  const tmp = document.createElement('div');
  tmp.innerHTML = html;
  return tmp.textContent || '—';
}

async function copyText(t){
  try{ await navigator.clipboard.writeText(t); toast('已复制','ok'); }
  catch(e){ toast('复制失败：'+e,'err'); }
}
function copyBtn(text){
  const safe = String(text).replace(/\\/g,'\\\\').replace(/'/g,"\\'");
  return `<button class="copy-btn" title="复制" onclick="copyText('${safe}')">⧉</button>`;
}

function toast(msg, type='info'){
  const el = document.createElement('div');
  el.className = 'toast ' + (type||'');
  el.textContent = msg;
  $('#toasts').appendChild(el);
  setTimeout(()=>{ el.classList.add('out'); setTimeout(()=>el.remove(), 300); }, 3200);
}

function skeleton(rows=4){ return Array.from({length:rows},()=>'<div class="skel skel-line"></div>').join(''); }
function skeletonCards(n=6){ return `<div class="cards-grid">${Array.from({length:n},()=>'<div class="skel skel-card"></div>').join('')}</div>`; }

function errBlock(retry){
  return `<div class="hint" style="color:var(--danger)">⚠ 加载失败</div>
    <button class="ghost sm mt8" onclick="${retry}">重试</button>`;
}

/* ---------- 弹窗 ---------- */
let _mResolve = null;
function openModal(title, bodyHTML, footHTML){
  $('#m-title').textContent = title;
  $('#m-body').innerHTML = bodyHTML;
  $('#m-foot').innerHTML = footHTML || '';
  $('#overlay').classList.add('show');
}
function closeModal(){ $('#overlay').classList.remove('show'); }
function resolveModal(v){ if(_mResolve){ _mResolve(v); _mResolve=null; } }
$('#m-close').addEventListener('click', closeModal);
$('#overlay').addEventListener('click', e => { if(e.target.id==='overlay') closeModal(); });

function confirmDlg({title, body, confirmText='确认', danger=true}){
  return new Promise(resolve=>{
    openModal(title, body, `
      <button class="ghost" onclick="closeModal();resolveModal(false)">取消</button>
      <button class="${danger?'danger':'primary'}" onclick="closeModal();resolveModal(true)">${esc(confirmText)}</button>`);
    _mResolve = resolve;
  });
}

/* ---------- API（后端接口一律不动；慢接口单独放大超时） ---------- */
async function api(path, opts={}){
  const ctrl = new AbortController();
  const timer = setTimeout(()=>ctrl.abort(), opts.timeout || 10000);
  const init = {method: opts.method||'GET', headers:{'Content-Type':'application/json'}, signal: ctrl.signal};
  if(opts.body) init.body = JSON.stringify(opts.body);
  try{
    const r = await fetch(path, init);
    const j = await r.json().catch(()=>({}));
    if(!r.ok){
      const d=j&&j.detail; const msg=typeof d==='string' ? d : (d&&d.error&&d.error.message) || (d ? JSON.stringify(d) : ('HTTP '+r.status));
      throw new Error(msg);
    }
    return j;
  }catch(e){
    if(e.name === 'AbortError') throw new Error('请求超时');
    if(String(e.message).startsWith('Failed to fetch')) throw new Error('网络错误（后端不可达）');
    throw e;
  }finally{
    clearTimeout(timer);
  }
}

/* ============================================================
   主题（曜石深色 ↔ 白天）
   ============================================================ */
function applyTheme(t){
  document.documentElement.dataset.theme = t;
  localStorage.setItem('cb2api-theme', t);
  $('#theme-btn').innerHTML = t === 'light'
    ? '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>'
    : '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.9 4.9l1.4 1.4m11.4 11.4 1.4 1.4M2 12h2m16 0h2M4.9 19.1l1.4-1.4m11.4-11.4 1.4-1.4"/></svg>';
}
$('#theme-btn').addEventListener('click', ()=>{
  applyTheme(document.documentElement.dataset.theme === 'light' ? 'dark' : 'light');
});
applyTheme(localStorage.getItem('cb2api-theme') || 'dark');

/* ============================================================
   全局状态 + 事件流（事件仅驱动状态灯/toast，不再展示日志页）
   ============================================================ */
const S = {
  status:null, accounts:null, usage:null, key:null, models:null, poll:null,
  prevPoll:null, prevUp:null, pageKey:'overview',
};
const EVENTS = [];
const MAX_EVENTS = 300;
function pushEvent(level, msg){
  EVENTS.push({ts: Date.now(), level, msg});
  if(EVENTS.length > MAX_EVENTS) EVENTS.splice(0, EVENTS.length - MAX_EVENTS);
}

function digestPoll(j){
  if(j.error){
    if(S.prevUp !== false){ pushEvent('ERROR', 'converter 轮询接口异常：' + j.error); toast('converter 轮询异常：'+j.error,'err'); }
    S.prevUp = false;
    return;
  }
  if(S.prevUp !== true){ pushEvent('OK', 'converter 连接正常'); }
  S.prevUp = true;
  if(!j.accounts){ S.prevPoll = j; return; }
  const prev = (S.prevPoll && S.prevPoll.accounts) || [];
  const map = Object.fromEntries(prev.map(a=>[a.file, a]));
  for(const a of j.accounts){
    const bad = a.error || a.token_expired;
    const p = map[a.file];
    const pbad = p && (p.error || p.token_expired);
    if(p && bad && !pbad){ pushEvent('WARN', `账号 ${a.file} 变为异常`); toast(`账号 ${a.file} 变为异常（${a.error||'token 过期'}）`,'warn'); }
    if(p && !bad && pbad){ pushEvent('OK', `账号 ${a.file} 恢复正常`); toast(`账号 ${a.file} 恢复正常`,'ok'); }
  }
  S.prevPoll = j;
}
function digestStatus(st){
  const up = !!(st && st.converter && !st.converter.error && st.converter.status === 'ok');
  if(S.prevUp === null){ S.prevUp = up; }
  if(up !== S.prevUp){
    pushEvent(up ? 'OK' : 'ERROR', up ? 'converter 上线' : 'converter 离线');
    S.prevUp = up;
  }
  renderHealth(up);
}
function renderHealth(up, label){
  const txt = label || (up ? 'converter 在线' : 'converter 异常');
  const lamp = $('#sb-lamp'), el = $('#sb-state');
  if(lamp) lamp.className = 'lamp ' + (up ? 'ok' : 'bad');
  if(el) el.textContent = txt;
}
async function refreshHealthLight(){
  try{
    digestStatus(await api('/api/status'));
  }catch(e){
    renderHealth(false, '状态未知');
  }
}

/* ---------- 轮询纪律 ---------- */
const pageTimers = new Set();
function pollEvery(fn, ms){
  const id = setInterval(()=>{ if(!document.hidden) fn(); }, ms);
  pageTimers.add(id);
  return id;
}
function clearPageTimers(){ for(const id of pageTimers) clearInterval(id); pageTimers.clear(); }
function pollTick(){
  const p = PAGES[S.pageKey];
  if(!p) return;
  if(p.tick) p.tick();
  else if(p.load) p.load();
}
document.addEventListener('visibilitychange', ()=>{ if(!document.hidden) pollTick(); });

/* ============================================================
   图标
   ============================================================ */
const ICONS = {
  overview:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="3.5" y="3.5" width="7" height="7" rx="2"/><rect x="13.5" y="3.5" width="7" height="7" rx="2"/><rect x="3.5" y="13.5" width="7" height="7" rx="2"/><rect x="13.5" y="13.5" width="7" height="7" rx="2"/></svg>',
  accounts:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="8" r="3.4"/><path d="M3.6 20c.8-3.1 2.9-4.9 5.4-4.9s4.6 1.8 5.4 4.9"/><circle cx="17" cy="9.2" r="2.4"/><path d="M15.8 15.3c2.2.4 3.8 1.8 4.5 4.2"/></svg>',
  keys:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="7.5" cy="15.5" r="4"/><path d="M10.4 12.6 20 3"/><path d="m16 7 3 3"/><path d="m13 10 2.2 2.2"/></svg>',
  models:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l7.5 4.3v8.4L12 20l-7.5-4.3V7.3L12 3z"/><path d="M12 11.5V20"/><path d="M12 11.5 4.5 7.3"/><path d="m12 11.5 7.5-4.2"/></svg>',
  usage:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M5.5 20v-6"/><path d="M12 20V5.5"/><path d="M18.5 20v-9"/><path d="M3 20h18"/></svg>',
  logs:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M5 3.5h9.5L19 8v12.5H5z"/><path d="M14 3.5V8h5"/><path d="M8 12h8"/><path d="M8 15.5h8"/><path d="M8 8h3"/></svg>',
  tasks:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="3.5" width="16" height="17" rx="2.5"/><path d="m8 9 1.5 1.5L12 8"/><path d="M13.5 9H17"/><path d="m8 15 1.5 1.5L12 14"/><path d="M13.5 15H17"/></svg>',
};

/* ============================================================
   路由
   ============================================================ */
const NAV = [
  {key:'overview', label:'总览'},
  {key:'accounts', label:'账号'},
  {key:'logs',     label:'请求日志'},
  {key:'keys',     label:'API 密钥'},
  {key:'models',   label:'模型'},
  {key:'usage',    label:'用量与额度'},
];
function renderNav(active){
  $('#nav').innerHTML = NAV.map(n=>`
    <div class="nav-item ${n.key===active?'active':''}" onclick="location.hash='#/${n.key}'">
      ${ICONS[n.icon||n.key]||ICONS.tasks}<span class="lb">${n.label}</span>
    </div>`).join('');
}
const PAGES = {};
function route(){
  const key = (location.hash||'#/overview').replace(/^#\//,'').split('?')[0] || 'overview';
  if(!PAGES[key]){ location.hash = '#/overview'; return; }
  if(S.pageKey === key && PAGES[key].mounted){ if(PAGES[key].refresh) PAGES[key].refresh(); return; }
  clearPageTimers();
  if(PAGES[S.pageKey] && PAGES[S.pageKey].unmount) PAGES[S.pageKey].unmount();
  S.pageKey = key;
  renderNav(key);
  $('#page-title').textContent = PAGES[key].title;
  $('#tb-hint').textContent = '';
  $('#content').innerHTML = '';
  if(PAGES[key].mount) PAGES[key].mount();
}
$('#refresh-btn').addEventListener('click', ()=>{
  const p = PAGES[S.pageKey];
  if(p && p.refresh) p.refresh();
});
window.addEventListener('hashchange', route);

/* ============================================================
   总览
   ============================================================ */
PAGES.overview = {
  title:'总览',
  mounted:false,
  mount(){
    this.mounted = true;
    $('#content').innerHTML = `<div class="page">
      <div class="grid cards4" id="ov-cards">${skeleton(4)}</div>
      <div class="card mt12" id="ov-quota"><div class="quota-wrap"><div class="quota-sum"><div class="skel skel-line"></div><div class="skel skel-line"></div></div><div class="quota-accts"></div></div></div>
      <div class="card mt12" id="ov-tokens"><h3>Token 用量 · 近 30 天</h3>${skeleton(2)}</div>
      <div class="card mt12" id="ov-acct"><h3>账号健康</h3>${skeleton(3)}</div>
      <div class="card mt12" id="ov-access"><h3>接口接入</h3>${skeleton(3)}</div>
      <div class="card mt12" id="ov-models"><h3>模型速览</h3>${skeleton(2)}</div>
      <div class="card mt12" id="ov-env"><h3>运行环境</h3>${skeleton(2)}</div>
    </div>`;
    this.load();
    loadQuota();
    refreshHealthLight();
    pollEvery(()=>this.load(), 10000);
    pollEvery(()=>loadQuota(), 300000);   // 积分 5 分钟自动刷新
  },
  unmount(){ this.mounted=false; },
  refresh(){ this.load(); },
  async load(){
    const [st, ac, ky, md, tk] = await Promise.allSettled([
      api('/api/status'), api('/api/accounts'), api('/api/key'), api('/api/models'),
      api('/api/token-stats'),
    ]);
    const stBad = st.status==='rejected', acBad = ac.status==='rejected', kyBad = ky.status==='rejected';
    const stv = stBad?null:st.value, acv = acBad?null:ac.value, kyv = kyBad?null:ky.value;
    if(!stBad){ S.status = stv; digestStatus(stv); }
    if(!acBad){ S.accounts = acv; }
    if(!kyBad){ S.key = kyv; }
    if(md.status==='fulfilled'){ S.models = md.value; }
    this.renderCards({stBad, acBad, kyBad, stv, acv, kyv});
    renderAcctHealth(acv, acBad);
    renderAccess(kyv, kyBad);
    renderModelsGlance();
    renderEnv(stv, stBad);
    renderTokenStats(tk.status==='fulfilled' ? tk.value : null);
    if(QUOTA.data) renderQuota();   // 仅刷新"更新于"时间显示，不发请求
  },
  renderCards({stBad, acBad, kyBad, stv, acv, kyv}){
    const box = $('#ov-cards'); if(!box) return;
    const up = !!(stv && stv.converter && !stv.converter.error && stv.converter.status==='ok');
    const conv = (stv && stv.converter) || {};
    const mode = conv.mode || '—';
    const nAcct = (acv && acv.accounts) ? acv.accounts.length : (conv.account_count ?? '—');
    const stats = (acv && acv.accounts) ? acv.accounts.reduce((t,a)=>t+((a.stats&&a.stats.calls)||0),0) : 0;
    const dis = acv ? (acv.disabled_count||0) : 0;
    const mr = MODEL.results, mTotal = (S.models&&S.models.models)||[];
    const mDone = mTotal.filter(m=>mr[m]);
    const mOk = mDone.filter(m=>mr[m].ok).length;
    box.innerHTML = `
      <div class="card clickable" onclick="location.hash='#/usage'">
        <h3>服务状态</h3>
        ${stBad ? errBlock('PAGES.overview.load()') : `
        <div class="big-num" style="font-size:26px;color:${up?'var(--ok)':'var(--danger)'}">${up?'运行中':'异常'}</div>
        <div class="hint mt8 mono" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(String(mode))}">${esc(String(mode))}</div>
        <div class="hint">累计调用 <span class="mono">${fmtNum(stats)}</span> 次</div>`}
      </div>
      <div class="card clickable" onclick="location.hash='#/accounts'">
        <h3>账号 ${dis>0?'<span class="badge warn">禁用 '+dis+'</span>':''}</h3>
        ${acBad ? errBlock('PAGES.overview.load()') : `
        <div class="big-num">${fmtNum(nAcct)}</div>
        <div class="hint mt8">启用 ${acv.enabled_count??0} · 禁用 ${acv.disabled_count??0}</div>`}
      </div>
      <div class="card clickable" onclick="location.hash='#/keys'">
        <h3>API Key</h3>
        ${kyBad ? errBlock('PAGES.overview.load()') : `
        <div class="big-num" style="font-size:24px;padding-top:3px">${kyv&&kyv.set?'已配置':'未配置'}</div>
        <div class="hint mt8 mono">${kyv&&kyv.set ? esc(kyv.masked||'sk-****') : '不校验 Bearer'}</div>`}
      </div>
      <div class="card clickable" onclick="location.hash='#/models'">
        <h3>模型</h3>
        ${mTotal.length && mDone.length ? `
        <div class="big-num" style="font-size:26px;color:var(--ok)">${mOk}<span class="muted" style="font-size:16px"> / ${mTotal.length}</span></div>
        <div class="hint mt8">探测可用 · <span class="mono">${mTotal.length-mDone.length}</span> 个未探测</div>`
        : mTotal.length ? `
        <div class="big-num" style="font-size:26px">${mTotal.length}</div>
        <div class="hint mt8">个模型 · 未探测可用性</div>`
        : `
        <div class="big-num" style="font-size:24px;padding-top:3px">待拉取</div>
        <div class="hint mt8">前往模型页拉取清单</div>`}
      </div>`;
  },
};

/* ---------- 总览：账号健康 ---------- */
function renderAcctHealth(acv, bad){
  const box = $('#ov-acct'); if(!box) return;
  if(bad || !acv || !Array.isArray(acv.accounts)){
    box.innerHTML = `<h3>账号健康</h3>${errBlock('PAGES.overview.load()')}`;
    return;
  }
  const list = acv.accounts;
  if(!list.length){
    box.innerHTML = `<h3>账号健康</h3><div class="empty" style="padding:28px">还没有账号
      <div class="mt12"><button class="primary" onclick="openImport()">导入账号</button></div></div>`;
    return;
  }
  const rows = list.slice(0, 10).map(a=>{
    if(a.error){
      return `<div class="acct-line" onclick="location.hash='#/accounts'">
        <span class="lamp bad"></span><span class="nm mono">${esc(a.file)}</span>
        <span class="meta" style="color:var(--danger)">读取失败</span></div>`;
    }
    const stBadge = a.disabled ? '<span class="badge mid">禁用</span>'
      : a.token_expired ? '<span class="badge bad">过期</span>' : '';
    const exp = relTimeText(a.token_expires_at);
    const calls = a.stats ? `${(a.stats.calls||0)} 次` : '—';
    return `<div class="acct-line" onclick="location.hash='#/accounts'" title="${esc(a.file)}">
      <span class="lamp ${a.disabled?'mid':(a.token_expired?'bad':'ok')}"></span>
      <span class="nm">${esc(a.nickname||a.file)} ${stBadge}</span>
      <span class="meta">到期 ${esc(exp)}</span>
      <span class="meta">${calls}</span></div>`;
  }).join('');
  const more = list.length > 10 ? `<div class="hint mt8" style="text-align:center;cursor:pointer" onclick="location.hash='#/accounts'">还有 ${list.length-10} 个账号 · 查看全部 →</div>` : '';
  const okN = list.filter(a=>!a.disabled && !a.token_expired && !a.error).length;
  box.innerHTML = `<h3>账号健康 <span class="badge ok">${okN}/${list.length} 正常</span></h3>
    <div class="acct-scroll"><div class="acct-cols">${rows}</div></div>${more}`;
}

/* ---------- 总览：接口接入 ---------- */
/* 面板 3008 自身就是 API 入口（/v1/* 由后端反代给内网 converter），
   所以默认同源；只有配了 PUBLIC_API_BASE（独立域名）时才用注入值。 */
function baseURL(){
  if(window.__CB2_API_BASE__) return window.__CB2_API_BASE__;
  return location.origin;
}
function copyAttr(text){
  return String(text).replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/\n/g,'\\n');
}
function renderAccess(kyv, bad){
  const box = $('#ov-access'); if(!box) return;
  const base = baseURL();
  const keySet = !!(kyv && kyv.set);
  const sample = `curl ${base}/v1/chat/completions \\\n  -H "Content-Type: application/json" \\\n  -H "Authorization: Bearer $CB2API_KEY" \\\n  -d '{"model":"hy4-preview","messages":[{"role":"user","content":"你好"}]}'`;
  box.innerHTML = `<h3>接口接入 ${keySet?'<span class="badge acc">Key 已启用</span>':'<span class="badge warn">未设 Key</span>'}</h3>
    <div class="acc-grid">
      <div>
        <div class="ep-row"><span class="ep-tag">OPENAI</span><span class="ep-path">POST /v1/chat/completions</span></div>
        <div class="ep-row"><span class="ep-tag">ANTHROPIC</span><span class="ep-path">POST /v1/messages</span></div>
        <div class="ep-row"><span class="ep-tag">RESPONSES</span><span class="ep-path">POST /v1/responses</span></div>
        <div class="ep-row"><span class="ep-tag">MODELS</span><span class="ep-path">GET /v1/models</span></div>
        <div class="hint mt12">${keySet ? '客户端需携带已配置的 Key（Base URL 见 API 密钥页）。' : '当前未设置 Key，Bearer 可留空；建议配置一个。'}</div>
      </div>
      <div class="code-block"><button class="icon-btn code-copy" title="复制示例" onclick="copyText('${copyAttr(sample)}')">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg></button>${esc(sample)}</div>
    </div>`;
}

/* ---------- 总览：模型速览 ---------- */
function renderModelsGlance(){
  const box = $('#ov-models'); if(!box) return;
  const models = (S.models && S.models.models) || [];
  if(!models.length){
    box.innerHTML = `<h3>模型速览</h3>
      <div class="empty" style="padding:28px">尚未拉取模型清单
      <div class="mt12"><button class="primary" onclick="location.hash='#/models'">前往拉取</button></div></div>`;
    return;
  }
  const res = MODEL.results;
  const done = models.filter(m=>res[m]);
  const okN = done.filter(m=>res[m].ok).length;
  const chips = models.map(m=>{
    const r = res[m];
    const cls = r ? (r.ok ? 'ok' : 'bad') : '';
    return `<span class="mchip ${cls}" onclick="location.hash='#/models'" title="${r ? (r.ok?'探测可用':'探测失败') : '未探测'}">${esc(m)}</span>`;
  }).join('');
  const probeTxt = done.length
    ? `<span class="badge ok">可用 ${okN}</span> <span class="badge bad">不可用 ${done.length-okN}</span> <span class="badge mid">未探测 ${models.length-done.length}</span>`
    : `<span class="badge mid">未探测</span>`;
  box.innerHTML = `<h3>模型速览 <span class="badge acc">${models.length} 个</span></h3>
    <div class="flex" style="gap:8px;flex-wrap:wrap">
      <span class="hint">${S.models.source === 'converter' ? 'converter 实时清单' : '内置静态清单'}</span>${probeTxt}
      <div class="grow"></div><span class="hint" style="cursor:pointer" onclick="location.hash='#/models'">管理 →</span>
    </div>
    <div class="flex mt8" style="gap:7px;flex-wrap:wrap;align-items:flex-start">${chips}</div>`;
}

/* ---------- 总览：运行环境 ---------- */
function renderEnv(stv, bad){
  const box = $('#ov-env'); if(!box) return;
  if(bad || !stv || !stv.converter || stv.converter.error){
    box.innerHTML = `<h3>运行环境</h3>${errBlock('PAGES.overview.load()')}`;
    return;
  }
  const c = stv.converter;
  const kv = (k, v) => `<div class="kv"><span class="k">${k}</span><span class="v">${v}</span></div>`;
  box.innerHTML = `<h3>运行环境 <span class="badge ok">healthy</span></h3>
    <div class="env-grid">
      ${kv('转发模式', esc(String(c.mode||'—')))}
      ${kv('多账号轮询', c.multi_account ? '开启' : '关闭')}
      ${kv('在线账号', (c.account_count ?? '—') + ' 个')}
      ${kv('凭据目录', esc(stv.auth_dir||'—'))}
      ${kv('Python', esc(c.python||'—'))}
      ${kv('面板版本', 'Midnight v2')}
    </div>`;
}

/* ---------- 总览：积分余额（billing 查询较慢，独立异步加载） ---------- */
let QUOTA = {loading:false, ts:0, data:null, error:''};

async function loadQuota(force){
  if(QUOTA.loading) return;
  if(!force && QUOTA.data && Date.now()-QUOTA.ts < 120000){ renderQuota(); return; }  // 2 分钟缓存
  QUOTA.loading = true; QUOTA.error = '';
  renderQuota();
  try{
    S.usage = await api('/api/usage', {timeout:90000});
    QUOTA.data = S.usage.results || {};
    QUOTA.ts = Date.now();
  }catch(e){ QUOTA.error = e.message; }
  QUOTA.loading = false;
  renderQuota();
}

function renderQuota(){
  const box = $('#ov-quota'); if(!box) return;
  const refreshed = QUOTA.ts ? `更新于 ${relTimeText(QUOTA.ts)}` : '';
  const headBtns = `<span class="hint" style="font-size:11px">${refreshed}</span>
      <button class="ghost sm" ${QUOTA.loading?'disabled':''} onclick="loadQuota(true)">${QUOTA.loading?'<span class="btn-spin"></span>查询中':'刷新'}</button>`;
  const wrap = (sumHtml, acctsHtml) => `<div class="flex" style="margin-bottom:12px">
      <h3 style="margin:0">积分余额</h3><div class="grow"></div>${headBtns}</div>
    <div class="quota-wrap">${sumHtml}<div class="quota-accts">${acctsHtml}</div></div>`;
  if(QUOTA.loading && !QUOTA.data){
    box.innerHTML = wrap(`<div class="quota-sum"><div class="empty" style="padding:14px"><span class="btn-spin"></span>正在查询 billing…</div></div>`, '');
    return;
  }
  if(QUOTA.error && !QUOTA.data){
    box.innerHTML = wrap(`<div class="quota-sum"><div class="empty" style="padding:14px;color:var(--danger)">⚠ ${esc(QUOTA.error)}
      <div class="mt12"><button class="primary" onclick="loadQuota(true)">重试</button></div></div></div>`, '');
    return;
  }
  const results = QUOTA.data || {};
  const files = Object.keys(results);
  if(!files.length){
    box.innerHTML = wrap(`<div class="quota-sum"><div class="empty" style="padding:14px">暂无账号数据</div></div>`, '');
    return;
  }
  let gTotal=0, gRemain=0, errN=0;
  const unitSet = new Set();
  const acctCards = files.map(f=>{
    const r = results[f];
    if(r.error){
      errN++;
      return `<div class="quota-acct" style="opacity:.6">
        <div class="row1"><span class="nm" title="${esc(f)}">${esc(r.nickname||f)}</span>
          <span class="badge bad">查询失败</span></div>
        <div class="hint" style="font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(String(r.error))}">${esc(String(r.error).slice(0,60))}</div></div>`;
    }
    const remain = r.grand_remain||0, total = r.grand_total||0;
    gTotal += total; gRemain += remain;
    const pct = total>0 ? remain/total*100 : 0;
    const unit = (r.pkgs && r.pkgs[0] && r.pkgs[0].unit) || '';
    if(unit) unitSet.add(unit);
    return `<div class="quota-acct">
      <div class="row1"><span class="lamp ${pct<=0?'bad':(pct<20?'warn':'ok')}"></span>
        <span class="nm" title="${esc(f)}">${esc(r.nickname||f)}</span>
        <span class="badge ${pct>=20?'ok':(pct>0?'warn':'bad')}">剩 ${pct.toFixed(0)}%</span></div>
      <div class="row2"><span class="num">${fmtNum(remain)}</span><span class="of">/ ${fmtNum(total)}${unit?(' '+esc(unit)):''}</span></div>
      <div class="pbar ${barClass(pct)}"><i style="width:${pct.toFixed(1)}%"></i></div></div>`;
  }).join('');
  const unitTxt = unitSet.size === 1 ? (' ' + Array.from(unitSet)[0]) : '';
  const sumHtml = `<div class="quota-sum">
      <span class="cap">总剩余积分</span>
      <div class="big-num">${fmtNum(Math.max(0,gRemain))}</div>
      <div class="hint">总额度 ${fmtNum(gTotal)}${unitTxt}</div>
      <div class="hint">已用 ${fmtNum(Math.max(0,gTotal-gRemain))} · ${files.length-errN} 个账号${errN?` · <span style="color:var(--warn)">${errN} 个失败</span>`:''}</div>
      <button class="ghost sm mt8" style="align-self:flex-start" onclick="location.hash='#/usage'">用量明细 →</button>
    </div>`;
  box.innerHTML = wrap(sumHtml, acctCards);
}

/* ---------- 总览：Token 用量图（近 30 天） ---------- */
function fmtTok(n){
  if(n >= 1e9) return (n/1e9).toFixed(2)+' B';
  if(n >= 1e6) return (n/1e6).toFixed(2)+' M';
  if(n >= 1e3) return (n/1e3).toFixed(1)+' K';
  return fmtNum(n);
}
function renderTokenStats(tk){
  const box = $('#ov-tokens'); if(!box) return;
  if(!tk || !Array.isArray(tk.days)){
    box.innerHTML = `<h3>Token 用量 · 近 30 天</h3>
      <div class="empty" style="padding:22px">用量数据读取失败 <button class="ghost sm" onclick="PAGES.overview.load()">重试</button></div>`;
    return;
  }
  const days = tk.days;
  const max = Math.max(...days.map(d=>d.tokens), 1);
  const allZero = tk.total_tokens === 0;
  const todayKey = days.length ? days[days.length-1].date : '';
  const bars = days.map(d=>{
    const h = d.tokens > 0 ? Math.max(4, Math.round(d.tokens/max*100)) : 2;
    const cls = d.date === todayKey ? 'today' : (d.tokens === 0 ? '' : '');
    const tip = `${d.date} · ${fmtNum(d.tokens)} tokens · ${d.calls} 次调用（输入 ${fmtNum(d.pt)} / 输出 ${fmtNum(d.ct)}）`;
    return `<div class="bar ${cls}" style="height:${h}%" title="${esc(tip)}"></div>`;
  }).join('');
  const axis = `<div class="tk-axis"><span>${esc(days[0].date)}</span><span>${esc(days[Math.floor(days.length/2)].date)}</span><span>今天 ${esc(todayKey)}</span></div>`;
  const modelChips = (tk.models||[]).map(m=>
    `<span class="mchip" title="${esc(m.model)} · ${fmtNum(m.tokens)} tokens · ${m.calls} 次">${esc(m.model)} <b style="opacity:.65;margin-left:4px">${fmtTok(m.tokens)}</b></span>`
  ).join('');
  const emptyNote = allZero
    ? `<div class="tk-empty">还没有调用记录 · 统计从面板部署后开始积累，发起一次对话后这里就会出现柱子</div>`
    : '';
  box.innerHTML = `<h3>Token 用量 · 近 30 天
      <span class="badge mid" style="letter-spacing:0">${tk.total_calls} 次调用</span></h3>
    <div class="tk-head">
      <div><div class="hint">30 天合计</div><div class="num">${fmtTok(tk.total_tokens)}</div></div>
      <div><div class="hint">峰值（单日）</div><div class="num" style="font-size:22px">${fmtTok(max===1?0:max)}</div></div>
      <div style="flex:1;min-width:200px"><div class="hint" style="margin-bottom:6px">模型分布（按 token）</div>
        <div class="flex" style="gap:7px;flex-wrap:wrap">${modelChips || '<span class="hint">暂无</span>'}</div></div>
    </div>
    ${emptyNote}
    <div class="tk-chart">${bars}</div>${axis}`;
}

/* ============================================================
   账号（卡片式）
   ============================================================ */
let ACCT = {list:[], query:'', filter:'all', page:1, pageSize:9, error:''};

PAGES.accounts = {
  title:'账号',
  mounted:false,
  mount(){
    this.mounted = true;
    $('#content').innerHTML = `<div class="page">
      <div class="toolbar">
        <button class="primary" onclick="openImport()">＋ 导入账号</button>
        <input type="search" placeholder="搜索昵称 / 文件名 / uid…" style="width:220px"
               id="acct-search" oninput="ACCT.query=this.value;ACCT.page=1;renderAcctCards()">
        <div class="seg" id="acct-seg">
          <button data-f="all" class="on">全部 <b id="seg-all">0</b></button>
          <button data-f="enabled">启用 <b id="seg-en">0</b></button>
          <button data-f="disabled">禁用 <b id="seg-dis">0</b></button>
        </div>
        <div class="grow"></div>
        <select id="acct-size" onchange="ACCT.pageSize=+this.value;ACCT.page=1;renderAcctCards()">
          <option value="9">9 张/页</option><option value="18">18 张/页</option><option value="36">36 张/页</option>
        </select>
        <button class="ghost" onclick="PAGES.accounts.load()">刷新</button>
      </div>
      <div id="acct-body">${skeletonCards(6)}</div>
    </div>`;
    $$('#acct-seg button').forEach(b=>b.onclick=()=>{
      $$('#acct-seg button').forEach(x=>x.classList.remove('on'));
      b.classList.add('on'); ACCT.filter=b.dataset.f; ACCT.page=1; renderAcctCards();
    });
    this.load();
    refreshHealthLight();
  },
  unmount(){ this.mounted=false; },
  refresh(){ this.load(); },
  async load(){
    try{
      const j = await api('/api/accounts');
      S.accounts = j;
      ACCT.list = Array.isArray(j.accounts) ? j.accounts : [];
      ACCT.error = '';
      $('#seg-all').textContent = ACCT.list.length;
      $('#seg-en').textContent = j.enabled_count ?? 0;
      $('#seg-dis').textContent = j.disabled_count ?? 0;
    }catch(e){ ACCT.list=[]; ACCT.error = e.message; }
    renderAcctCards();
  },
};

function filteredAccts(){
  const q = ACCT.query.trim().toLowerCase();
  return ACCT.list.filter(a=>{
    if(ACCT.filter==='enabled' && a.disabled) return false;
    if(ACCT.filter==='disabled' && !a.disabled) return false;
    if(!q) return true;
    return (a.file||'').toLowerCase().includes(q) || (a.nickname||'').toLowerCase().includes(q) ||
           (a.uid||'').toLowerCase().includes(q);
  });
}
function acctStatus(a){
  if(a.disabled) return '<span class="badge mid"><span class="lamp mid"></span>已禁用</span>';
  if(a.token_expired) return '<span class="badge bad"><span class="lamp bad"></span>token 过期</span>';
  return '<span class="badge ok"><span class="lamp ok"></span>正常</span>';
}
function renderAcctCards(){
  const box = $('#acct-body'); if(!box) return;
  const list = filteredAccts();
  const total = list.length;
  const pages = Math.max(1, Math.ceil(total / ACCT.pageSize));
  if(ACCT.page > pages) ACCT.page = pages;
  const slice = list.slice((ACCT.page-1)*ACCT.pageSize, ACCT.page*ACCT.pageSize);
  if(ACCT.error){
    box.innerHTML = `<div class="empty">⚠ 账号列表加载失败：<span class="mono">${esc(ACCT.error)}</span>
      <div class="mt12"><button class="primary" onclick="PAGES.accounts.load()">重试</button></div></div>`;
    return;
  }
  if(!total){
    box.innerHTML = `<div class="empty"><div class="big">◇</div>${ACCT.query||ACCT.filter!=='all'?'没有匹配的账号':'还没有账号'}
      <div class="mt12"><button class="primary" onclick="openImport()">导入账号</button></div></div>`;
    return;
  }
  const cards = slice.map(a=>{
    if(a.error){
      return `<div class="entity off"><div class="e-head"><span class="e-title mono">${esc(a.file)}</span></div>
        <div class="e-detail err">读取失败：${esc(a.error)}</div></div>`;
    }
    const expSoon = a.token_expires_at && !a.token_expired && (a.token_expires_at - Date.now()) < 86400000;
    const stats = a.stats ? `${a.stats.calls||0} 次 · 成 ${a.stats.ok||0} · 败 ${a.stats.fail||0}` : '—';
    return `<div class="entity ${a.disabled?'off':''}">
      <div class="e-head">
        <span class="lamp ${a.disabled?'mid':(a.token_expired?'bad':'ok')}"></span>
        <span class="e-title" title="${esc(a.nickname||a.file)}">${esc(a.nickname||a.file)}</span>
        <span style="margin-left:auto">${acctStatus(a)}</span>
      </div>
      <div class="e-sub">${esc(a.file)}${copyBtn(a.file)}</div>
      <div class="e-sub">uid ${a.uid ? esc(a.uid)+copyBtn(a.uid) : '—'}</div>
      <div class="e-line"><span class="k">企业 / 域</span><span class="v">${esc(a.enterpriseName||a.domain||'—')}${(a.domain&&(a.domain.endsWith('.workbuddy.ai')||a.domain.endsWith('.codebuddy.ai')))?' <span class="badge" style="color:var(--acc2)">INTL</span>':''}</span></div>
      <div class="e-line"><span class="k">上游代理</span><span class="v mono">${a.proxy?esc(a.proxy):'直连'}</span></div>
      <div class="e-line"><span class="k">token 到期</span><span class="v" ${expSoon?'style="color:var(--warn)"':''}>${relTime(a.token_expires_at)}${expSoon?' · 即将到期':''}</span></div>
      <div class="e-line"><span class="k">网关调用</span><span class="v">${a.gw
        ? `今日 <b>${a.gw.today}</b> · 累计 <b>${a.gw.calls}</b> · 失败 ${a.gw.fail}${a.gw.last_ts?` · 最近 ${relTime(a.gw.last_ts)}`:''}`
        : '—'}</span></div>
      <div class="e-line"><span class="k">探测统计</span><span class="v">${stats}</span></div>
      <div class="e-foot">
        <label class="switch" title="${a.disabled?'启用':'禁用'}"><input type="checkbox" ${a.disabled?'':'checked'}
          onchange="toggleAcct('${esc(a.file)}')"><span class="tr"></span></label>
        <span class="hint">${a.disabled?'已禁用':'轮询中'}</span>
        <div class="grow"></div>
        <button class="ghost sm" onclick="openProxyModal('${esc(a.file)}')">代理</button>
        <button class="ghost sm" onclick="refreshToken('${esc(a.file)}')">续期</button>
        <button class="danger sm" onclick="delAcct('${esc(a.file)}')">删除</button>
      </div>
    </div>`;
  }).join('');
  box.innerHTML = `<div class="cards-grid">${cards}</div>
    <div class="pager">共 ${total} 个账号 · 第 ${ACCT.page} / ${pages} 页<div class="grow"></div>
      <button class="ghost sm" onclick="ACCT.page--;renderAcctCards()" ${ACCT.page<=1?'disabled':''}>‹</button>
      <button class="ghost sm" onclick="ACCT.page++;renderAcctCards()" ${ACCT.page>=pages?'disabled':''}>›</button>
    </div>`;
}

async function toggleAcct(file){
  const enabling = file.endsWith('.disabled');
  if(!await confirmDlg({
    title: enabling?'启用账号':'禁用账号',
    body:`确认${enabling?'启用':'禁用'}账号 <span class="mono">${esc(file)}</span> ？<br>
          <span class="hint mt8" style="display:inline-block">禁用后将从 converter 轮询池剔除，不再转发请求；可随时重新启用。</span>`,
    confirmText: enabling?'启用':'禁用', danger:!enabling,
  })) return;
  try{
    await api('/api/accounts/'+encodeURIComponent(file)+'/toggle', {method:'POST'});
    toast((enabling?'已启用 ':'已禁用 ')+file, 'ok');
    pushEvent('ACT', `${enabling?'启用':'禁用'}账号 ${file}`);
    PAGES.accounts.load();
  }catch(e){ toast('操作失败：'+e.message, 'err'); }
}

async function delAcct(file){
  if(!await confirmDlg({
    title:'删除账号',
    body:`确认删除账号 <span class="mono">${esc(file)}</span> ？<br>
          <span class="hint mt8" style="display:inline-block">凭据文件将被永久删除，不可恢复。</span>`,
    confirmText:'删除', danger:true,
  })) return;
  try{
    await api('/api/accounts/'+encodeURIComponent(file), {method:'DELETE'});
    toast('已删除 '+file, 'ok');
    pushEvent('ACT', '删除账号 '+file);
    PAGES.accounts.load();
  }catch(e){ toast('删除失败：'+e.message, 'err'); }
}

async function refreshToken(file){
  toast('正在触发 token 续期检查…','info');
  try{
    await api('/api/usage', {timeout:90000});
    toast('已触发续期检查：'+file,'ok');
    setTimeout(()=>PAGES.accounts.load(), 1200);
  }catch(e){ toast('续期触发失败：'+e.message,'err'); }
}

/* ---------- 代理预设（按需改成你自己的出网代理地址） ---------- */
const PROXY_PRESETS = [
  ['','直连（不走代理）'],
  ['__custom','自定义…'],
];
function proxySelectHtml(id, current){
  const known = PROXY_PRESETS.some(x=>x[0]===current);
  const opts = PROXY_PRESETS.map(([v,t])=>`<option value="${esc(v)}" ${v===(known?current:(current?'__custom':''))?'selected':''}>${esc(t)}</option>`).join('');
  return `<select id="${id}" style="width:100%" onchange="(function(s){const c=s.parentElement.querySelector('.px-custom');if(c)c.style.display=s.value==='__custom'?'block':'none'})(this)">
    ${opts}</select>
    <input class="px-custom mono" placeholder="http://host:port 或 socks5://host:port" style="width:100%;display:${current&&!known?'block':'none'};margin-top:6px" value="${known?'':esc(current||'')}">`;
}
function readProxyVal(box){
  if(!box) return '';
  const sel = box.querySelector('select'), custom = box.querySelector('.px-custom');
  if(!sel) return '';
  return sel.value==='__custom' ? (custom?custom.value.trim():'') : sel.value;
}

/* ---------- 导入 ---------- */
let IMPORT = {raw:'', parse:null};
function openImport(){
  IMPORT = {raw:'', parse:null};
  openModal('导入账号', `
    <label>粘贴导出的 WorkBuddy / CodeBuddy 凭据 JSON（单个对象或数组，支持 xyw110 导出格式或 .info 原始格式）</label>
    <textarea id="imp-json" class="mono big" placeholder='[{"access_token":"...","refresh_token":"...","uid":"...","nickname":"...","expires_at":1234567890}]'
      oninput="validateImport(this.value)"></textarea>
    <div class="mt12 flex" style="gap:12px">
      <div class="grow"><label>版本（JSON 未带 domain 时生效）</label>
        <select id="imp-domain" style="width:100%">
          <option value="">自动（国区 copilot.tencent.com）</option>
          <option value="www.codebuddy.ai">国际版 CodeBuddy（www.codebuddy.ai）</option>
          <option value="www.workbuddy.ai">国际版 WorkBuddy（www.workbuddy.ai）</option>
        </select></div>
      <div class="grow"><label>上游代理（本批统一）</label>${proxySelectHtml('imp-proxy','')}</div>
    </div>
    <div class="mt12 flex">
      <input type="file" id="imp-file" accept=".json,application/json" class="grow" onchange="readImportFile(this)">
      <button class="ghost" onclick="loadSample()">填充示例</button>
    </div>
    <div id="imp-valid" class="hint mt8"></div>`,
    `<button class="ghost" onclick="closeModal()">取消</button>
     <button class="primary" id="imp-go" disabled onclick="doImport()">确认导入</button>`);
  setTimeout(()=>{ const t=$('#imp-json'); if(t) t.focus(); }, 60);
}
function readImportFile(input){
  const f = input.files[0]; if(!f) return;
  const rd = new FileReader();
  rd.onload = () => validateImport(rd.result);
  rd.readAsText(f);
}
function loadSample(){
  validateImport(JSON.stringify([{access_token:'sk-sample-aaaa',refresh_token:'rt-sample-bbbb',uid:'u-sample',nickname:'示例账号',enterprise_id:'',expires_at:Date.now()+86400000}],null,2));
}
function validateImport(raw){
  IMPORT.raw = raw;
  const box = $('#imp-valid'); const go = $('#imp-go');
  if(!box) return;
  if(!raw.trim()){ box.innerHTML=''; go.disabled=true; return; }
  let parsed;
  try{ parsed = JSON.parse(raw); }
  catch(e){
    box.innerHTML = `<span class="badge bad">JSON 语法错误</span> <span>${esc(String(e.message))}</span>`;
    go.disabled = true; return;
  }
  const items = Array.isArray(parsed) ? parsed : [parsed];
  let ok=0, bad=[];
  items.forEach((it, i)=>{
    if(it && typeof it==='object' && (it.access_token||it.accessToken)) ok++;
    else bad.push(i+1);
  });
  IMPORT.parse = parsed;
  const note = bad.length ? ` · <span style="color:var(--danger)">格式错误 ${bad.length} 条（第 ${bad.join('、')} 条）</span>` : '';
  box.innerHTML = `识别到 <b class="mono">${items.length}</b> 条：有效 <span class="badge ok">${ok}</span>${note}`;
  go.disabled = (ok===0);
}
async function doImport(){
  const btn = $('#imp-go');
  btn.disabled = true; btn.innerHTML = '<span class="btn-spin"></span>导入中…';
  try{
    const proxy = readProxyVal($('#imp-proxy').parentElement);
    const domain = ($('#imp-domain')||{}).value || '';
    const j = await api('/api/accounts/import', {method:'POST', body:{json: IMPORT.raw, proxy, domain}});
    toast(`已导入 ${j.count} 个账号`, 'ok');
    pushEvent('ACT', `导入账号 ${j.count} 个`);
    closeModal();
    PAGES.accounts.load();
  }catch(e){
    toast('导入失败：'+e.message, 'err');
    btn.disabled = false; btn.textContent = '确认导入';
  }
}

/* ---------- 单账号代理/后端设置 ---------- */
function openProxyModal(file){
  const a = ACCT.list.find(x=>x.file===file); if(!a){ toast('账号不在列表','err'); return; }
  openModal('网络设置 · '+file, `
    <label>上游代理（改后立即生效，无需重启）</label>
    ${proxySelectHtml('px-sel', a.proxy||'')}
    <div class="hint mt8">当前后端：<span class="mono">${esc(a.backend||'-')}</span> · domain：<span class="mono">${esc(a.domain||'-')}</span></div>
    <div class="hint">代理仅影响该账号访问 CodeBuddy/WorkBuddy 后端的出网路径；直连 = 容器原样出网。</div>`,
    `<button class="ghost" onclick="closeModal()">取消</button>
     <button class="primary" onclick="saveProxySettings('${esc(file)}')">保存</button>`);
}
async function saveProxySettings(file){
  const proxy = readProxyVal($('#px-sel').parentElement);
  try{
    const j = await api('/api/accounts/'+encodeURIComponent(file)+'/settings', {method:'POST', body:{proxy}});
    toast('已保存：'+(j.proxy? '代理 '+j.proxy : '直连'), 'ok');
    pushEvent('ACT', `设置代理 ${file} → ${j.proxy||'直连'}`);
    closeModal(); PAGES.accounts.load();
  }catch(e){ toast('保存失败：'+e.message,'err'); }
}

/* ============================================================
   API 密钥
   ============================================================ */
/* ============================================================
   请求日志（converter 真实转发流量，含「哪个账号被调用」）
   ============================================================ */
let LOGS = {rows:[], limit:200, acct:'', onlyErr:false, auto:true, err:'', total:0};
function fmtTok(n){ n=+n||0; return n>=1000 ? (n/1000).toFixed(1)+'k' : String(n); }
function fmtMs(ms){ ms=+ms||0; return ms>=1000 ? (ms/1000).toFixed(1)+'s' : ms+'ms'; }

PAGES.logs = {
  title:'请求日志',
  mounted:false,
  mount(){
    this.mounted = true;
    $('#content').innerHTML = `<div class="page">
      <div class="toolbar">
        <select id="log-acct" style="min-width:190px"><option value="">全部账号</option></select>
        <label class="hint" style="display:flex;align-items:center;gap:6px">
          <input type="checkbox" id="log-err" onchange="LOGS.onlyErr=this.checked;PAGES.logs.load()"> 只看失败</label>
        <select id="log-limit" onchange="LOGS.limit=+this.value;PAGES.logs.load()">
          <option value="100">100 条</option><option value="200" selected>200 条</option>
          <option value="500">500 条</option><option value="1000">1000 条</option></select>
        <label class="hint" style="display:flex;align-items:center;gap:6px">
          <input type="checkbox" id="log-auto" checked onchange="PAGES.logs.setAuto(this.checked)"> 自动刷新</label>
        <div class="grow"></div>
        <span class="hint" id="log-sum"></span>
        <button class="ghost" onclick="PAGES.logs.load()">刷新</button>
        <button class="danger" onclick="clearLogs()">清空日志</button>
      </div>
      <div class="card" id="log-body">${skeleton(8)}</div>
    </div>`;
    this.load();
    this.setAuto(LOGS.auto);
    refreshHealthLight();
  },
  unmount(){ this.mounted=false; },
  refresh(){ this.load(); },
  setAuto(on){
    LOGS.auto = !!on;
    clearPageTimers();
    if(LOGS.auto) pollEvery(()=>{ if(PAGES.logs.mounted) PAGES.logs.load(); }, 5000);
  },
  async load(){
    try{
      const j = await api(`/api/logs?limit=${LOGS.limit}&acct=${encodeURIComponent(LOGS.acct)}&only_err=${LOGS.onlyErr?1:0}`);
      LOGS.rows = Array.isArray(j.logs) ? j.logs : [];
      LOGS.total = j.total_matched ?? LOGS.rows.length;
      const sel = $('#log-acct');
      if(sel){
        const cur = LOGS.acct;
        sel.innerHTML = `<option value="">全部账号</option>` +
          (j.account_options || (j.accounts||[]).map(a=>({value:a,label:a,source:''}))).map(a=>{
            const value=String(a.value||'');
            const label=String(a.label||value);
            const suffix=a.source?` · ${a.source}`:'';
            return `<option value="${esc(value)}" ${value===cur?'selected':''}>${esc(label+suffix)}</option>`;
          }).join('');
        sel.onchange = ()=>{ LOGS.acct = sel.value; PAGES.logs.load(); };
      }
      const g = j.gateway_total || {};
      const sum = $('#log-sum');
      if(sum) sum.textContent = `窗口内 ${LOGS.total} 条 · 累计调用 ${g.calls||0} 次（失败 ${g.fail||0}、今日 ${g.today||0}）`;
      LOGS.err = '';
    }catch(e){ LOGS.err = e.message; LOGS.rows = []; }
    renderLogs();
  },
};
function renderLogs(){
  const box = $('#log-body'); if(!box) return;
  if(LOGS.err){ box.innerHTML = `<div class="empty">⚠ 读取失败：<span class="mono">${esc(LOGS.err)}</span></div>`; return; }
  if(!LOGS.rows.length){ box.innerHTML = `<div class="empty">暂无请求记录</div>`; return; }
  const th = 'style="text-align:left;padding:7px 10px;color:var(--muted);font-weight:500;border-bottom:1px solid var(--line)"';
  const td = 'padding:6px 10px;border-bottom:1px solid var(--line)';
  const rows = LOGS.rows.map(r=>{
    const ok = (r.status||'ok')==='ok';
    const badge = ok ? `<span class="badge ok">ok</span>` : `<span class="badge bad">${esc(r.status||'err')}</span>`;
    const account = r.nick || r.label || r.acct || '-';
    const task = r.task || r.prompt || r.request || '';
    return `<tr>
      <td class="mono" style="${td}">${esc(relTime(r.ts))}</td>
      <td class="mono" style="${td}" title="${esc(r.acct||'')}">${esc(account)} <span class="hint">${esc(source)}</span></td>
      <td style="${td}">${esc(r.model||'-')}</td>
      <td style="${td}">${esc(r.proto||'-')}${r.stream?' <span class="hint">stream</span>':''}</td>
      <td style="${td};max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(task)}">${esc(task||'-')}</td>
      <td class="mono" style="${td};text-align:right">${fmtTok(r.pt)}</td>
      <td class="mono" style="${td};text-align:right">${fmtTok(r.ct)}${r.rt?` <span class="hint">+${fmtTok(r.rt)}</span>`:''}</td>
      <td class="mono" style="${td};text-align:right">${fmtMs(r.ms)}</td>
      <td style="${td}">${badge}</td>
    </tr>`;
  }).join('');
  box.innerHTML = `<table style="width:100%;border-collapse:collapse;font-size:12.5px">
    <thead><tr><th ${th}>时间</th><th ${th}>账号</th><th ${th}>模型</th><th ${th}>协议</th><th ${th}>任务 / 请求</th>
      <th ${th};text-align:right">输入</th><th ${th};text-align:right">输出</th>
      <th ${th};text-align:right">耗时</th><th ${th}>状态</th></tr></thead>
    <tbody>${rows}</tbody></table>`;
}
async function clearLogs(){
  if(!await confirmDlg({title:'清空请求日志', body:'将删除全部历史请求记录（不影响账号、额度与签到）。确认继续？',
                        confirmText:'清空', danger:true})) return;
  try{ await api('/api/logs/clear',{method:'POST'}); toast('请求日志已清空','ok'); PAGES.logs.load(); }
  catch(e){ toast('清空失败：'+e.message,'err'); }
}

PAGES.keys = {
  title:'API 密钥',
  mounted:false,
  mount(){
    this.mounted = true;
    $('#content').innerHTML = `<div class="page">
      <div class="toolbar">
        <button class="primary" onclick="openKeyModal()">＋ 新建 Key</button>
        <span class="hint">converter 校验客户端 Bearer；留空 = 不校验。运行时热更新，无需重启。</span>
      </div>
      <div class="grid cards4" id="key-cards">${skeleton(2)}</div>
    </div>`;
    this.load();
    refreshHealthLight();
  },
  unmount(){ this.mounted=false; },
  refresh(){ this.load(); },
  async load(){
    try{ S.key = await api('/api/key'); }catch(e){ S.key={error:e.message}; }
    renderKeyCards();
  },
};
function renderKeyCards(){
  const box = $('#key-cards'); if(!box) return;
  box.style.gridTemplateColumns = 'repeat(2,1fr)';
  const k = S.key;
  if(!k || k.error){
    box.innerHTML = `<div class="card"><h3>当前密钥</h3>${errBlock('PAGES.keys.load()')}</div>
      <div class="card"><h3>使用说明</h3>
      <div class="kv"><span class="k">鉴权方式</span><span class="v">Authorization: Bearer &lt;key&gt;</span></div>
      <div class="kv"><span class="k">生效方式</span><span class="v">热更新</span></div></div>`;
    return;
  }
  if(!k.set){
    box.innerHTML = `<div class="card"><h3>当前密钥</h3>
      <div class="status-pill"><span class="lamp mid"></span>未设置（不校验 Bearer）</div>
      <div class="hint mt12">点击上方「新建 Key」生成或填入一个 key。</div></div>
      <div class="card"><h3>使用说明</h3>
      <div class="kv"><span class="k">鉴权方式</span><span class="v">Bearer &lt;key&gt;</span></div>
      <div class="kv"><span class="k">生效方式</span><span class="v">热更新 · 无需重启</span></div>
      <div class="kv"><span class="k">Base URL</span><span class="v">${baseURL()}/v1</span></div></div>`;
    return;
  }
  box.innerHTML = `<div class="card"><h3>当前密钥</h3>
    <div class="big-num" style="font-size:22px;padding-top:2px">${esc(k.masked||'')}</div>
    <div class="hint mt8">已配置 · 完整 key 仅创建时显示一次</div>
    <div class="mt16"><button class="danger sm" onclick="clearKey()">清空密钥（停止校验）</button></div></div>
    <div class="card"><h3>使用说明</h3>
    <div class="kv"><span class="k">鉴权方式</span><span class="v">Bearer &lt;key&gt;</span></div>
    <div class="kv"><span class="k">生效方式</span><span class="v">热更新 · 无需重启</span></div>
    <div class="kv"><span class="k">Base URL</span><span class="v">${baseURL()}/v1</span></div></div>`;
}
async function clearKey(){
  if(!await confirmDlg({title:'清空 API Key',
    body:'清空后 converter 将不再校验客户端 Bearer，任何持有地址的人都可以调用。确认继续？',
    confirmText:'清空', danger:true})) return;
  try{ await api('/api/key', {method:'POST', body:{key:''}}); toast('已清空','ok'); pushEvent('ACT','清空 API Key'); PAGES.keys.load(); }
  catch(e){ toast(e.message,'err'); }
}
function openKeyModal(){
  openModal('新建 API 密钥', `
    <label>填入自定义 key，或点击生成随机 key</label>
    <input id="new-key" class="mono" placeholder="sk-...">
    <div class="mt12"><button class="ghost sm" onclick="genKey()">生成随机 key</button></div>
    <div class="hint mt12">保存后完整 key 仅显示一次，请立即复制保存。</div>`,
    `<button class="ghost" onclick="closeModal()">取消</button>
     <button class="primary" onclick="saveNewKey()">保存</button>`);
  setTimeout(()=>{ const i=$('#new-key'); if(i) i.focus(); }, 60);
}
function genKey(){
  const rnd = () => crypto.getRandomValues(new Uint32Array(4)).join('');
  $('#new-key').value = 'sk-' + rnd();
}
let NEWKEY = '';
function copyNewKey(){ copyText(NEWKEY); }
async function saveNewKey(){
  const k = $('#new-key').value.trim();
  if(!k){ toast('key 不能为空','warn'); return; }
  try{
    await api('/api/key', {method:'POST', body:{key:k}});
    NEWKEY = k;
    pushEvent('ACT', '更新 API Key');
    closeModal();
    openModal('Key 已保存（仅显示一次）', `
      <div class="hint" style="margin-bottom:10px">请立即复制并妥善保存：</div>
      <div class="flex"><span class="mono" style="font-size:13px;word-break:break-all">${esc(k)}</span>
      <button class="primary sm" style="flex:0 0 auto" onclick="copyNewKey()">复制</button></div>
      <div class="hint mt12" style="color:var(--warn)">关闭后只能看到脱敏尾号。</div>`,
      `<button class="primary" onclick="closeModal()">我已知晓，关闭</button>`);
    PAGES.keys.load();
  }catch(e){ toast('保存失败：'+e.message,'err'); }
}

/* ============================================================
   模型（卡片式：拉取 + 探测）
   ============================================================ */
let MODEL = {results:{}, probing:false, current:null, lastTs:0};

PAGES.models = {
  title:'模型',
  mounted:false,
  mount(){
    this.mounted = true;
    $('#content').innerHTML = `<div class="page">
      <div class="toolbar">
        <button class="primary" id="pull-btn" onclick="PAGES.models.load()">⟳ 拉取清单</button>
        <button id="discover-btn" onclick="discoverModels()">发现新模型</button>
        <button id="probe-btn" onclick="probeAllModels()">探测全部可用性</button>
        <input id="add-model-input" placeholder="手动添加模型名" style="width:150px;height:30px;padding:0 8px;border:1px solid var(--line2);border-radius:6px;background:transparent;color:inherit;font-family:var(--mono,monospace)">
        <div class="sep" style="width:1px;height:20px;background:var(--line2)"></div>
        <span class="hint" id="model-src">来源：—</span>
        <div class="grow"></div>
        <span class="badge-row" id="model-summary"></span>
      </div>
      <div id="model-body">${skeletonCards(6)}</div>
      <div class="hint mt12">「拉取清单」读取当前模型清单（持久化在 auth 卷 models.json，converter 即时生效）；「发现新模型」手动对候选池逐个发最小请求探测新模型，加入与否由你决定（无自动同步）；「探测」验证真实可用性。</div>
    </div>`;
    if(!S.models) this.load(); else renderModelCards();
    refreshHealthLight();
  },
  unmount(){ this.mounted=false; },
  refresh(){ this.load(); },
  async load(){
    const btn = $('#pull-btn');
    if(btn){ btn.disabled = true; btn.innerHTML = '<span class="btn-spin"></span>拉取中…'; }
    try{
      S.models = await api('/api/models');
      pushEvent('INFO', `拉取模型清单（来源 ${S.models.source||'—'}，共 ${(S.models.models||[]).length} 个）`);
    }catch(e){
      const box = $('#model-body');
      if(box) box.innerHTML = `<div class="empty">⚠ 模型列表拉取失败：${esc(e.message)}
        <div class="mt12"><button class="primary" onclick="PAGES.models.load()">重试</button></div></div>`;
      const src = $('#model-src'); if(src) src.textContent = '来源：—';
    }
    if(btn){ btn.disabled = false; btn.innerHTML = '⟳ 拉取模型'; }
    renderModelCards();
  },
};
function modelBadgeEl(m){
  const r = MODEL.results[m];
  if(MODEL.probing && MODEL.current === m)
    return '<span class="badge mid"><span class="btn-spin"></span>探测中</span>';
  if(!r) return '<span class="badge mid">未探测</span>';
  if(r.ok) return '<span class="badge ok">可用</span>';
  return '<span class="badge bad">不可用</span>';
}
function renderModelCards(){
  const box = $('#model-body'); if(!box) return;
  const models = (S.models && S.models.models) || [];
  if(!models.length){ box.innerHTML = '<div class="empty"><div class="big">◇</div>暂无模型 · 点击「拉取清单」或「发现新模型」</div>'; return; }
  const src = $('#model-src');
  if(src) src.textContent = '来源：' + (S.models.source === 'converter' ? 'converter（models.json / 内置回退）' : '内置静态清单');
  const res = MODEL.results;
  const done = models.filter(m=>res[m]);
  const okN = done.filter(m=>res[m].ok).length;
  const sum = $('#model-summary');
  if(sum) sum.innerHTML = done.length
    ? `<span class="badge ok">可用 ${okN}</span> <span class="badge bad">不可用 ${done.length-okN}</span> <span class="badge mid">未探测 ${models.length-done.length}</span>`
    : `<span class="badge mid">共 ${models.length} 个</span>`;
  const cards = models.map(m=>{
    const r = res[m];
    let detail = '', derr = '';
    if(r){
      if(r.ok){ detail = 'HTTP '+r.status; }
      else { detail = String(r.error || r.preview || ('HTTP '+(r.status||'-'))).slice(0,120); derr = 'err'; }
    }
    return `<div class="entity ${MODEL.probing && MODEL.current===m ? 'probing':''}">
      <div class="e-head">
        <span class="e-title mono" title="${esc(m)}">${esc(m)}</span>
        ${copyBtn(m)}
        <span style="margin-left:auto">${modelBadgeEl(m)}</span>
      </div>
      <div class="e-detail ${derr}" title="${esc(detail)}">${r ? esc(detail) : '尚未探测可用性'}</div>
      <div class="e-foot">
        <span class="hint">${r ? relTime(MODEL.lastTs) : '—'}</span>
        <div class="grow"></div>
        <button class="ghost sm" ${MODEL.probing?'disabled':''} onclick="probeOneModel('${esc(m)}')">探测</button>
        <button class="ghost sm" style="color:#e5484d" ${MODEL.probing?'disabled':''} onclick="delModel('${esc(m)}')">删除</button>
      </div>
    </div>`;
  }).join('');
  box.innerHTML = `<div class="cards-grid">${cards}</div>`;
}
async function probeOneModel(m){
  if(MODEL.probing) return;
  MODEL.results[m] = null; MODEL.probing = true; MODEL.current = m; MODEL.lastTs = Date.now();
  renderModelCards();
  try{
    const j = await api('/api/models/probe', {method:'POST', body:{models:[m]}, timeout:45000});
    MODEL.results[m] = (j.results && j.results[m]) || {ok:false, error:'无结果'};
  }catch(e){ MODEL.results[m] = {ok:false, error:e.message}; }
  MODEL.probing = false; MODEL.current = null;
  renderModelCards();
  const r = MODEL.results[m];
  toast(`模型 ${m} 探测${r && r.ok ? '通过' : '失败'}`, r && r.ok ? 'ok' : 'warn');
}
async function probeAllModels(){
  if(MODEL.probing) return;
  if(!S.models || !S.models.models.length) await PAGES.models.load();
  const models = (S.models && S.models.models) || [];
  if(!models.length){ toast('无模型可探测，请先拉取模型','warn'); return; }
  MODEL.probing = true; MODEL.results = {}; MODEL.lastTs = Date.now();
  const btn = $('#probe-btn');
  if(btn){ btn.disabled = true; btn.innerHTML = '<span class="btn-spin"></span>探测中…'; }
  renderModelCards();
  for(const m of models){
    if(S.pageKey !== 'models'){ MODEL.probing = false; MODEL.current = null; return; }
    MODEL.current = m;
    renderModelCards();
    try{
      const j = await api('/api/models/probe', {method:'POST', body:{models:[m]}, timeout:45000});
      MODEL.results[m] = (j.results && j.results[m]) || {ok:false, error:'无结果'};
    }catch(e){ MODEL.results[m] = {ok:false, error:e.message}; }
    renderModelCards();
  }
  const okN = models.filter(m=>MODEL.results[m] && MODEL.results[m].ok).length;
  pushEvent('OK', `模型可用性探测完成（${okN}/${models.length} 可用）`);
  toast(`探测完成：${okN}/${models.length} 可用`, okN ? 'ok' : 'warn');
  MODEL.probing = false; MODEL.current = null;
  if(btn){ btn.disabled = false; btn.innerHTML = '探测全部可用性'; }
  renderModelCards();
}

async function discoverModels(){
  const btn = $('#discover-btn');
  const oldBar = $('#discover-bar'); if(oldBar) oldBar.remove();
  if(btn){ btn.disabled = true; btn.innerHTML = '<span class="btn-spin"></span>发现中…'; }
  try{
    const j = await api('/api/models/discover', {method:'POST', body:{}, timeout:120000});
    MODEL.lastTs = Date.now();
    MODEL.results = Object.assign({}, MODEL.results, j.results||{});
    renderModelCards();
    const newOk = j.newOk||[];
    pushEvent('OK', '发现新模型完成：候选 '+j.tested+'，清单外可用 '+newOk.length);
    if(newOk.length){
      const box = $('#model-body');
      const bar = document.createElement('div');
      bar.className = 'card mt12';
      bar.id = 'discover-bar';
      bar.innerHTML = '<h3>发现 '+newOk.length+' 个清单外可用模型（点击加入，不会自动改动清单）</h3>' +
        '<div class="mt12" style="display:flex;flex-wrap:wrap;gap:8px">' +
        newOk.map(m=>'<button class="ghost sm mono" onclick="addModelName(\''+esc(m)+'\')">+ '+esc(m)+'</button>').join('') +
        '</div><div class="mt12"><button class="primary sm" onclick="addAllDiscovered()">全部加入</button></div>';
      box.parentNode.insertBefore(bar, box.nextSibling);
      toast('发现 '+newOk.length+' 个新可用模型，请选择加入','ok');
    } else {
      toast('发现完成：候选 '+j.tested+' 个，无清单外新模型','ok');
    }
  }catch(e){
    toast('发现失败：'+e.message,'err');
  }
  if(btn){ btn.disabled = false; btn.innerHTML = '发现新模型'; }
}
async function addModelName(m){
  try{
    const cur = (S.models && S.models.models) || [];
    if(cur.includes(m)){ toast(m+' 已在清单中','warn'); return; }
    const j = await api('/api/models/set', {method:'POST', body:{models: cur.concat([m])}});
    S.models = {models: j.models, source: 'converter'};
    MODEL.results = {};
    const bar = $('#discover-bar'); if(bar) bar.remove();
    renderModelCards();
    toast('已加入 '+m,'ok');
    pushEvent('OK','模型已加入清单：'+m);
  }catch(e){ toast('加入失败：'+e.message,'err'); }
}
async function addAllDiscovered(){
  try{
    const bar = $('#discover-bar'); if(!bar){ toast('无可加入项','warn'); return; }
    const cur = (S.models && S.models.models) || [];
    const names = Array.from(bar.querySelectorAll('button.mono')).map(b=>b.textContent.slice(2));
    const merged = cur.concat(names.filter(m=>!cur.includes(m)));
    const j = await api('/api/models/set', {method:'POST', body:{models: merged}});
    S.models = {models: j.models, source: 'converter'};
    MODEL.results = {};
    if(bar) bar.remove();
    renderModelCards();
    toast('已加入 '+names.length+' 个模型','ok');
    pushEvent('OK','批量加入模型：'+names.join(', '));
  }catch(e){ toast('加入失败：'+e.message,'err'); }
}
async function delModel(m){
  if(!confirm('确定从模型清单中删除「'+m+'」？\n（仅改 models.json，可随时重新添加）')) return;
  try{
    const cur = (S.models && S.models.models) || [];
    const j = await api('/api/models/set', {method:'POST', body:{models: cur.filter(x=>x!==m)}});
    S.models = {models: j.models, source: 'converter'};
    if(MODEL.results) delete MODEL.results[m];
    renderModelCards();
    toast('已删除 '+m,'ok');
    pushEvent('OK','模型已从清单删除：'+m);
  }catch(e){ toast('删除失败：'+e.message,'err'); }
}
async function addModelManual(){
  const inp = $('#add-model-input');
  const m = ((inp && inp.value) || '').trim();
  if(!m){ toast('请输入模型名','warn'); return; }
  await addModelName(m);
  if(inp) inp.value = '';
}

/* ============================================================
   用量与额度
   ============================================================ */
let USAGE = {acct:'all', agg:[], errs:[], open:null, page:1};

PAGES.usage = {
  title:'用量与额度',
  mounted:false,
  mount(){
    this.mounted = true;
    $('#content').innerHTML = `<div class="page">
      <div class="toolbar">
        <select id="usage-acct" onchange="USAGE.acct=this.value;buildUsage()"><option value="all">全部账号</option></select>
        <button class="primary" id="usage-btn" onclick="PAGES.usage.load()">查询额度</button>
        <span class="hint">来自 CodeBuddy billing 计量接口 · 查询会触发 token 续期</span>
        <div class="grow"></div>
        <span class="hint" id="usage-summary"></span>
      </div>
      <div class="card" style="padding:4px 0 0"><div class="tbl-wrap" id="usage-body">${skeleton(5)}</div></div>
      <div class="hint mt12">剩余 &gt;50% 冰蓝 / 20~50% 琥珀 / &lt;20% 红；7 天内到期行标黄。点击行展开账号明细。</div>
    </div>`;
    this.load();
    refreshHealthLight();
  },
  unmount(){ this.mounted=false; },
  refresh(){ this.load(); },
  async load(){
    const box = $('#usage-body'); const btn = $('#usage-btn');
    if(btn){ btn.disabled = true; btn.innerHTML = '<span class="btn-spin"></span>查询中…'; }
    if(box) box.innerHTML = '<div class="empty" style="padding:26px"><span class="btn-spin"></span>正在查询 billing 计量接口（账号较多时约需数十秒）…</div>';
    try{ S.usage = await api('/api/usage', {timeout:90000}); }
    catch(e){
      if(btn){ btn.disabled = false; btn.innerHTML = '查询额度'; }
      if(box) box.innerHTML = `<div class="empty">⚠ 查询失败：${esc(e.message)}
        <div class="mt12"><button class="primary" onclick="PAGES.usage.load()">重试</button></div></div>`;
      return;
    }
    if(btn){ btn.disabled = false; btn.innerHTML = '查询额度'; }
    const results = (S.usage && S.usage.results) || {};
    const sel = $('#usage-acct');
    if(sel){
      const cur = sel.value;
      sel.innerHTML = '<option value="all">全部账号</option>' +
        Object.keys(results).map(f=>`<option value="${esc(f)}">${esc(f)}${results[f].nickname?(' · '+esc(results[f].nickname)):''}</option>`).join('');
      sel.value = cur && (cur==='all'||results[cur]) ? cur : 'all';
      USAGE.acct = sel.value;
    }
    buildUsage();
    pushEvent('ACT', '查询用量额度');
  },
};
function parseCbTime(v){
  if(!v) return 0;
  if(typeof v==='number') return v<1e12 ? v*1000 : v;
  const n = Number(v);
  if(!isNaN(n)) return n<1e12 ? n*1000 : n;
  const d = new Date(String(v).replace(' ','T'));
  return isNaN(d)?0:d.getTime();
}
function remainPct(pkg){ return (pkg.total||0)>0 ? (pkg.remain||0)/(pkg.total)*100 : 0; }
function barClass(pct){ return pct>=50 ? 'c-acc' : pct>=20 ? 'c-warn' : 'c-bad'; }
function buildUsage(){
  const box = $('#usage-body'); if(!box) return;
  const results = (S.usage && S.usage.results) || {};
  const names = Object.keys(results).filter(f=>USAGE.acct==='all'||f===USAGE.acct);
  if(!names.length){ box.innerHTML = '<div class="empty">暂无账号数据</div>'; return; }
  USAGE.errs = names.filter(f=>results[f].error);
  const okF = names.filter(f=>!results[f].error);
  const agg = new Map();
  for(const f of okF){
    for(const p of (results[f].pkgs||[])){
      const used = (p.total||0) - (p.remain||0);
      const endMs = parseCbTime(p.cycle_end);
      if(!agg.has(p.name)) agg.set(p.name, {name:p.name, rows:[], total:0, used:0, nearestEnd:0});
      const g = agg.get(p.name);
      g.rows.push({acct:f, pkg:p, used});
      g.total += (p.total||0); g.used += used;
      if(endMs && (!g.nearestEnd || endMs < g.nearestEnd)) g.nearestEnd = endMs;
    }
  }
  const list = Array.from(agg.values());
  list.sort((a,b)=>(a.nearestEnd||Infinity)-(b.nearestEnd||Infinity));
  USAGE.agg = list; USAGE.page = 1;
  let total=0, used=0;
  list.forEach(g=>{ total+=g.total; used+=g.used; });
  const sum = $('#usage-summary');
  if(sum) sum.textContent = `${okF.length} 个账号 · ${list.length} 类额度包 · 剩余 ${fmtNum(Math.max(0,total-used))} / ${fmtNum(total)}`;
  renderUsage();
}
function renderUsage(){
  const box = $('#usage-body'); if(!box) return;
  const list = USAGE.agg;
  const now = Date.now();
  if(!list.length){
    box.innerHTML = `<div class="empty">${USAGE.acct==='all'?'':'该账号'}暂无额度包数据
      <div class="hint mt8">可点击「查询额度」重新读取 billing 计量接口</div></div>`;
    return;
  }
  let rows = list.map(g=>{
    const remain = Math.max(0, g.total-g.used);
    const pct = g.total>0 ? remain/g.total*100 : 0;
    const near = g.nearestEnd && (g.nearestEnd-now < 86400000*7) && g.nearestEnd>now;
    const endTxt = g.nearestEnd ? relTime(g.nearestEnd) : '—';
    return `<tr${near?' class="warn-row"':''} style="cursor:pointer" onclick="toggleUsageDetail('${esc(g.name)}')">
      <td class="mono">${esc(g.name)} <span class="badge mid">×${g.rows.length}</span></td>
      <td class="mono">${fmtNum(g.total)}</td>
      <td class="mono">${fmtNum(g.used)}</td>
      <td class="mono">${fmtNum(remain)}</td>
      <td style="min-width:150px"><div class="flex"><div class="pbar ${barClass(pct)} grow"><i style="width:${pct.toFixed(1)}%"></i></div>
          <span class="muted mono" style="font-size:11px">${pct.toFixed(0)}%</span></div></td>
      <td class="mono" style="font-size:12px">${endTxt}</td>
    </tr>`;
  }).join('');
  const errNote = (USAGE.errs && USAGE.errs.length)
    ? `<div style="padding:10px 14px;font-size:12px;color:var(--warn);border-bottom:1px solid var(--line)">⚠ ${USAGE.errs.length} 个账号查询失败（${USAGE.errs.map(esc).join('、')}），其余正常展示</div>`
    : '';
  const open = USAGE.open;
  let detailHtml = '';
  if(open){
    const g = list.find(x=>x.name===open);
    if(g){
      const per=20, pg=Math.max(1,Math.ceil(g.rows.length/per));
      if(USAGE.page>pg) USAGE.page=pg;
      const rows2 = g.rows.slice((USAGE.page-1)*per, USAGE.page*per).map(r=>{
        const pct = remainPct(r.pkg);
        return `<tr class="row-exp"><td class="muted">└ ${esc(r.acct)}</td>
          <td class="mono">${fmtNum(r.pkg.total)}</td>
          <td class="mono">${fmtNum(r.used)}</td>
          <td class="mono">${fmtNum(r.pkg.remain??0)}</td>
          <td><div class="flex"><div class="pbar ${barClass(pct)} grow"><i style="width:${pct.toFixed(1)}%"></i></div>
              <span class="muted mono" style="font-size:11px">${pct.toFixed(0)}%</span></div></td>
          <td class="muted mono" style="font-size:12px">${relTimeText(parseCbTime(r.pkg.cycle_end))}</td></tr>`;
      }).join('');
      detailHtml = `<tr class="row-exp"><td colspan="6" style="padding:0 10px 10px">
        <table><thead><tr><th>账号</th><th>总额度</th><th>已用</th><th>剩余</th><th>消耗</th><th>周期到期</th></tr></thead>
        <tbody>${rows2}</tbody></table>
        <div class="pager">第 ${USAGE.page} / ${pg} 页<div class="grow"></div>
          <button class="ghost sm" onclick="event.stopPropagation();USAGE.page--;renderUsage()" ${USAGE.page<=1?'disabled':''}>‹</button>
          <button class="ghost sm" onclick="event.stopPropagation();USAGE.page++;renderUsage()" ${USAGE.page>=pg?'disabled':''}>›</button>
          <button class="ghost sm" onclick="event.stopPropagation();USAGE.open=null;renderUsage()">收起</button></div>
      </td></tr>`;
    }
  }
  box.innerHTML = `${errNote}<table><thead><tr><th>额度包</th><th>总额度</th><th>已用</th><th>剩余</th><th>消耗</th><th>最近到期</th></tr></thead>
    <tbody>${rows}${detailHtml}</tbody></table>`;
}
function toggleUsageDetail(name){
  USAGE.open = (USAGE.open===name) ? null : name;
  USAGE.page = 1;
  renderUsage();
}

/* ============================================================
   启动：内联事件需访问顶层 const/let，统一暴露到 window
   ============================================================ */
Object.assign(window, { PAGES, S, ACCT, USAGE, MODEL, IMPORT, NEWKEY, EVENTS });

route();
if(!location.hash) location.hash = '#/overview';
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    # 外置 UI 优先（仓库里的 ui/index.html），其次容器内路径，最后回落到内置 INDEX_HTML。
    candidates = [
        Path(os.environ["PANEL_UI_FILE"]) if os.environ.get("PANEL_UI_FILE") else None,
        Path(__file__).resolve().parent.parent / "ui" / "index.html",
        Path("/app/ui/index.html"),
    ]
    # 把对外 API 地址注入前端，避免把端口 / IP 写死在 HTML 里。
    boot = "<script>window.__CB2_API_BASE__=%s;</script>" % json.dumps(PUBLIC_API_BASE)
    for ext in candidates:
        try:
            if ext and ext.exists():
                html = ext.read_text(encoding="utf-8")
                return HTMLResponse(html.replace("</body>", boot + "\n</body>", 1))
        except Exception:
            continue
    return HTMLResponse(INDEX_HTML.replace("</body>", boot + "\n</body>", 1))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PANEL_PORT", "3008"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
