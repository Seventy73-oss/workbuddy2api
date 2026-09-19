#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CodeBuddy 每日签到（国区 copilot.tencent.com）—— v2 2026-09-11

相对旧版：
  1. 同时扫描 *.info 与 *.info.disabled —— 账号「停用」（退出轮询池）后依然签到。
  2. 只处理国区账号；国际版（*.codebuddy.ai / *.workbuddy.ai）走另一套后端，这里直接跳过，
     避免每天刷 401 报错。
  3. access token 过期自动续期并回写（停用期间也保持 token 新鲜，重新启用即可用）。
     回写用临时文件 + os.replace：兼容 auth 目录里 root 属主、当前用户只读的 .info。
  4. 分时签到：每个账号按文件名做稳定散列，落在 [START_HOUR, END_HOUR) 窗口内的某一分钟；
     cron 每 10 分钟跑一次，到点才签，签过即跳过（状态文件去重，空闲时不发请求、不写日志）。
  5. 幂等：签到前先查 checkin-activity-status，已签则只记录状态。

用法：
  python3 daily_checkin.py [auth_dir] [--dry-run] [--check-only] [--now] [-v]
    --dry-run     只打印今天的安排时间，不发任何请求
    --check-only  只查签到状态，不签到
    --now         忽略时间窗，立即签到（手动补签）
    -v            多打一些细节（例如跳过国际版账号的原因）
环境变量：CHECKIN_START_HOUR=8  CHECKIN_END_HOUR=12  （本地时间，NAS 为 CST）
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import sys
import time
import zlib

import httpx

BASE = "https://copilot.tencent.com"
INTL_SUFFIXES = (".codebuddy.ai", ".workbuddy.ai")
STATE_NAME = ".checkin_state.json"


def backend_headers(info: dict) -> dict:
    a = info.get("auth") or {}
    ac = info.get("account") or {}
    return {
        "Authorization": "Bearer " + a.get("accessToken", ""),
        "X-User-Id": ac.get("uid", ""),
        "X-Enterprise-Id": ac.get("enterpriseId", ""),
        "X-Tenant-Id": ac.get("enterpriseId", ""),
        "X-Domain": a.get("domain", "www.codebuddy.cn"),
        "X-Product": "SaaS",
        "X-IDE-Name": "CodeBuddyIDE",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "CodeBuddyIDE",
        "Accept": "application/json",
    }


def is_cn(info: dict) -> bool:
    domain = ((info.get("auth") or {}).get("domain") or "www.codebuddy.cn")
    return not domain.endswith(INTL_SUFFIXES)


def is_disabled(path: str) -> bool:
    return path.endswith(".disabled")


def load_state(auth_dir: str) -> dict:
    try:
        with open(os.path.join(auth_dir, STATE_NAME), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(auth_dir: str, state: dict) -> None:
    try:
        tmp = os.path.join(auth_dir, STATE_NAME + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=1)
        os.replace(tmp, os.path.join(auth_dir, STATE_NAME))
    except Exception:
        pass  # 状态文件只是去重优化，失败不影响签到


def bump_attempts(state: dict, name: str, today: str, cap: int = 3) -> int:
    """记录当天签到尝试次数并返回本次序号；达到 cap 后当天不再重试（防刷屏）。"""
    key = name + "@try"
    cur = state.get(key)
    if not isinstance(cur, dict) or cur.get("date") != today:
        cur = {"date": today, "n": 0}
    cur["n"] = int(cur.get("n", 0)) + 1
    state[key] = cur
    if cur["n"] >= cap:
        state[name] = today
    return cur["n"]


def write_info(path: str, info: dict) -> bool:
    """临时文件 + rename 回写（目标可能是 root 属主的只读文件，但目录可写）。"""
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
        return True
    except Exception as e:
        print(f"    [!] 回写失败（不影响本次签到）: {e}")
        return False


def ensure_token(c: httpx.Client, path: str, info: dict) -> bool:
    """access token 过期/缺失时用 refreshToken 续期并回写。返回是否可用于请求。"""
    a = info.get("auth") or {}
    exp = a.get("expiresAt") or 0
    if a.get("accessToken") and exp and time.time() * 1000 < exp - 60_000:
        return True
    if not a.get("refreshToken"):
        return bool(a.get("accessToken"))
    h = backend_headers(info)
    h["X-Refresh-Token"] = a.get("refreshToken", "")
    h["X-Auth-Refresh-Source"] = "plugin"
    try:
        r = c.post(BASE + "/v2/plugin/auth/token/refresh", headers=h, json={})
        data = r.json()
    except Exception as e:
        print(f"    [!] 续期请求失败: {type(e).__name__} {e}")
        return bool(a.get("accessToken"))
    if data.get("code") != 0 or not data.get("data"):
        print(f"    [!] 续期未成功: code={data.get('code')} msg={data.get('msg')}")
        return bool(a.get("accessToken"))
    new = data["data"]
    a["accessToken"] = new.get("accessToken") or a.get("accessToken")
    a["refreshToken"] = new.get("refreshToken") or a.get("refreshToken")
    if new.get("expiresAt"):
        a["expiresAt"] = new["expiresAt"]
    elif new.get("expiresIn"):
        a["expiresAt"] = int(time.time() * 1000) + int(new["expiresIn"]) * 1000
    a["lastRefreshTime"] = int(time.time() * 1000)
    info["auth"] = a
    ok = write_info(path, info)
    print(f"    ↻ token 已续期{'并回写' if ok else '（回写失败）'}")
    return True


def slot_of(path: str, start_hour: int, end_hour: int) -> dt.datetime:
    """按文件名散列出今天的签到时刻（窗口内某一分钟，稳定可复现）。"""
    name = os.path.basename(path)
    window = max(1, (end_hour - start_hour) * 60)
    offset = zlib.crc32(name.encode("utf-8")) % window
    return dt.datetime.combine(dt.date.today(), dt.time(start_hour)) + dt.timedelta(minutes=offset)


def is_intl_skip(path) -> bool:  # 兼容旧调用
    return False


def main() -> None:
    ap = argparse.ArgumentParser(description="CodeBuddy 国区每日签到 v2")
    ap.add_argument("auth_dir", nargs="?", default="/data/auth")
    ap.add_argument("--dry-run", action="store_true", help="只打印安排，不发请求")
    ap.add_argument("--check-only", action="store_true", help="只查状态，不签到")
    ap.add_argument("--now", action="store_true", help="忽略时间窗，立即签到")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    start_hour = int(os.environ.get("CHECKIN_START_HOUR", "8"))
    end_hour = int(os.environ.get("CHECKIN_END_HOUR", "12"))
    auth_dir = args.auth_dir
    today = dt.date.today().isoformat()
    now = dt.datetime.now()
    state = load_state(auth_dir)

    files = sorted(glob.glob(os.path.join(auth_dir, "*.info"))) + \
        sorted(glob.glob(os.path.join(auth_dir, "*.info.disabled")))

    todo, skipped_intl, skipped_signed, skipped_wait = [], [], [], []
    for f in files:
        try:
            with open(f, encoding="utf-8") as fh:
                info = json.load(fh)
        except Exception as e:
            print(f"  {os.path.basename(f)}: 读取失败 {e}")
            continue
        if not is_cn(info):
            skipped_intl.append(f)
            continue
        name = os.path.basename(f)
        if state.get(name) == today and not args.now:
            skipped_signed.append(name)
            continue
        slot = slot_of(f, start_hour, end_hour)
        if not args.now and now < slot:
            skipped_wait.append((name, slot))
            continue
        todo.append((f, info, slot))

    if args.dry_run:
        print(f"==== 签到安排 {today}（窗口 {start_hour}:00-{end_hour}:00，现在 {now:%H:%M}）====")
        for f, info, slot in todo:
            print(f"  {os.path.basename(f):40s} 今天 {slot:%H:%M} 可签（当前已到点）")
        for name, slot in skipped_wait:
            print(f"  {name:40s} 今天 {slot:%H:%M}")
        for name in skipped_signed:
            print(f"  {name:40s} 今天已签过，跳过")
        for f in skipped_intl:
            print(f"  {os.path.basename(f):40s} 国际版账号，跳过（不在国区签到范围）")
        return

    if not todo:
        if args.verbose:
            print(f"[{now:%Y-%m-%d %H:%M}] 无待签账号"
                  f"（等时间窗 {len(skipped_wait)}，已签 {len(skipped_signed)}，国际版 {len(skipped_intl)}）")
        return

    print(f"==== CodeBuddy 每日签到 {today} {now:%H:%M} 待处理 {len(todo)} 个账号 ====")
    with httpx.Client(timeout=20) as c:
        for f, info, slot in todo:
            name = os.path.basename(f)
            nick = (info.get("account") or {}).get("nickname", "?")
            tag = "已停用·仅签到" if is_disabled(f) else "启用中"
            try:
                if not ensure_token(c, f, info):
                    print(f"  {name} [{nick}] 无可用 token，跳过")
                    continue
                h = backend_headers(info)
                st = c.post(BASE + "/billing/meter/checkin-activity-status", headers=h, json={}).json()
                d = st.get("data") or {}
                if d.get("today_checked_in"):
                    print(f"  {name} [{nick}] ({tag}) 已签 ✓ 连续{d.get('streak_days', 0)}天 "
                          f"累计{d.get('total_credits', 0)}积分")
                    state[name] = today
                    continue
                if not d.get("active"):
                    print(f"  {name} [{nick}] ({tag}) 活动未启用/无权益 → 今日不再重试")
                    state[name] = today   # 无权益：当天不必再查，避免每 10 分钟刷屏
                    continue
                if args.check_only:
                    print(f"  {name} [{nick}] ({tag}) 可签（今日未签，连续{d.get('streak_days', 0)}天）")
                    continue
                r = c.post(BASE + "/billing/meter/daily-checkin", headers=h, json={}).json()
                if r.get("code") == 0:
                    total = (r.get("data") or {}).get("total_credits", "?")
                    print(f"  {name} [{nick}] ({tag}) 签到成功! 今日+{d.get('daily_credit', 0)} 累计{total}")
                    state[name] = today
                else:
                    tries = bump_attempts(state, name, today, cap=3)
                    print(f"  {name} [{nick}] ({tag}) 签到未成功: code={r.get('code')} "
                          f"msg={r.get('msg')}（今日第 {tries}/3 次尝试）")
            except Exception as e:
                bump_attempts(state, name, today, cap=3)
                print(f"  {name} [{nick}] 异常 {type(e).__name__}: {e}")
    save_state(auth_dir, state)


if __name__ == "__main__":
    main()
