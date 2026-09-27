#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
USPS 运单在线查询（jsdom 版，drop-in 替代 usps_track.py）

与 usps_track.py 完全相同的对外接口，唯一区别是“JS 计算”引擎：
  - usps_track.py      : iv8（真 V8 + 自研 BOM/DOM），在 Python 进程内跑传感器。
  - usps_track_jsdom.py : 调用 Node 子进程里的 jsdom 跑传感器（见 usps_challenge_jsdom.js）。

流程：
  1. Node/jsdom 在线执行一次 Akamai 挑战（可选走同一 http 代理），拿到通行 Cookie
  2. 本模块把这些 Cookie 注入一个全新的 requests.Session（相同代理出口）
  3. 用该会话查询一个或多个运单

用法（与 usps_track.py 一致）:
    python usps_track_jsdom.py 9202090395221519986059
    python usps_track_jsdom.py 单号1 单号2
    python usps_track_jsdom.py --proxy http://user:pass@host:port 单号

依赖：
    - Node.js（node 在 PATH 上，或用环境变量 NODE_BIN 指定）
    - 已在本目录 `npm install`（package.json 里含 jsdom）
"""

import os
import re
import sys
import json
import shutil
import pathlib
import argparse
import subprocess

import requests
from requests.cookies import create_cookie

# ---------------------------------------------------------------- 常量（与 usps_track.py 保持一致）
BASE = "https://m.usps.com"
PAGE_URL = f"{BASE}/m/TrackConfirmAction"
UA = "Emb/And/1.0"
MAX_ATTEMPTS = 3
PASS_COOKIES = ("JSESSIONID", "NSC_psjhjo-n_443", "w3IsGuY1")

HERE = pathlib.Path(__file__).resolve().parent
NODE_SCRIPT = HERE / "usps_challenge_jsdom.js"
# 查询主机；所有 cookie 都以此域注入 session（单主机场景，作用域放宽不影响发送）
QUERY_HOST = "m.usps.com"


class AkamaiChallengeError(RuntimeError):
    pass


# ---------------------------------------------------------------- 文本/状态解析（与 usps_track.py 相同）
def html_to_text(html):
    text = re.sub(r"<script[\s\S]*?</script>", " ", html)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# 状态短语（含 USPS 特有的“Status Not Available”降级文案）
STATUS_RE = re.compile(
    r"(Delivered|Pre-Shipment|In Transit|Out for Delivery|Available for Pickup|"
    r"Alert|Return to Sender|Status Not Available|"
    r"Accepted|Departed|Arrived)[\s\S]{0,300}"
)

# “Status Not Available”整句以句号结尾，单独抓完整降级说明
SNA_RE = re.compile(r"Status Not Available[\s\S]*?other reasons\.")


# ---------------------------------------------------------------- 会话（与 usps_track.py 相同）
def make_session(proxy=None):
    """创建全新 requests 会话（Cookie 罐为空），可选固定代理。"""
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": UA,
        "Accept-Language": "zh-CN,en-US;q=0.8",
        # USPS 源站用它判定“来自 App”，缺失会返回 “service currently unavailable”。
        "X-Requested-With": "com.usps",
    })
    if proxy:
        import random
        if "{sid}" in proxy:
            proxy = proxy.replace("{sid}", str(random.randint(10**8, 10**9 - 1)))
        sess.proxies.update({"http": proxy, "https": proxy})
    if os.environ.get("SSL_VERIFY", "1").strip().lower() in ("0", "false", "no"):
        sess.verify = False
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
    return sess


# ---------------------------------------------------------------- Node/jsdom 挑战
def _node_bin():
    """定位 node 可执行文件。优先 NODE_BIN，其次 PATH。"""
    cand = os.environ.get("NODE_BIN") or "node"
    resolved = shutil.which(cand)
    if not resolved:
        raise AkamaiChallengeError(
            f"未找到 Node.js 可执行文件（{cand!r}）。请安装 Node 并确保在 PATH 上，"
            f"或用环境变量 NODE_BIN 指定绝对路径。")
    return resolved


def _run_node_challenge(proxy=None, verbose=True, timeout_ms=25000):
    """调用 Node 子进程执行一次 jsdom 挑战，返回解析后的结果字典。失败抛 AkamaiChallengeError。"""
    if not NODE_SCRIPT.exists():
        raise AkamaiChallengeError(f"缺少 Node 挑战脚本: {NODE_SCRIPT}")

    node = _node_bin()
    cmd = [node, str(NODE_SCRIPT), "--timeout", str(int(timeout_ms))]
    if proxy:
        cmd += ["--proxy", proxy]
    if verbose:
        cmd += ["--verbose"]
    if os.environ.get("SSL_VERIFY", "1").strip().lower() in ("0", "false", "no"):
        cmd += ["--insecure"]

    # 子进程总超时 = 挑战超时 + 余量（拉页面/启动 jsdom）
    proc_timeout = timeout_ms / 1000.0 + 20

    try:
        proc = subprocess.run(
            cmd, cwd=str(HERE), capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=proc_timeout,
        )
    except subprocess.TimeoutExpired:
        raise AkamaiChallengeError(f"Node 挑战子进程超时（>{proc_timeout:.0f}s）")

    if verbose and proc.stderr:
        # Node 脚本的过程日志走 stderr，透传给调用方便于观察
        sys.stderr.write(proc.stderr)

    # stdout 最后一行是结果 JSON
    line = ""
    for ln in reversed((proc.stdout or "").splitlines()):
        if ln.strip():
            line = ln.strip()
            break
    if not line:
        raise AkamaiChallengeError(
            f"Node 挑战无输出（returncode={proc.returncode}）。stderr尾: "
            f"{(proc.stderr or '')[-300:]}")
    try:
        data = json.loads(line)
    except Exception as e:
        raise AkamaiChallengeError(f"解析 Node 输出失败: {e} | 原文: {line[:300]}")
    return data


def _session_from_jar(jar, proxy=None):
    """把 Node 导出的 cookie 列表注入一个新的 requests.Session（相同代理出口）。"""
    sess = make_session(proxy)
    for c in jar:
        name = c.get("key")
        val = c.get("value")
        if not name:
            continue
        # 单主机查询：统一以 QUERY_HOST 注入，确保发往 m.usps.com 时都会带上
        ck = create_cookie(
            name=name, value=val, domain=QUERY_HOST,
            path=c.get("path") or "/",
        )
        sess.cookies.set_cookie(ck)
    return sess


def solve_challenge(proxy=None, verbose=True):
    """
    在线执行一次完整 Akamai 挑战（jsdom），返回通过验证的 requests.Session。
    对外签名与 usps_track.solve_challenge 完全一致，可直接替换。
    """
    def log(*a):
        if verbose:
            print(*a, flush=True)

    timeout_ms = int(os.environ.get("CHALLENGE_TIMEOUT_MS", "25000"))
    log("STEP jsdom 执行 Akamai 挑战 ...")
    data = _run_node_challenge(proxy=proxy, verbose=verbose, timeout_ms=timeout_ms)

    if not data.get("ok"):
        raise AkamaiChallengeError(
            f"挑战未通过（variant={data.get('variant')}, pageStatus={data.get('pageStatus')}, "
            f"missing={data.get('missing')}, error={data.get('error')}）")

    sess = _session_from_jar(data.get("jar", []), proxy=proxy)

    got = {k for k, _ in sess.cookies.items()}
    missing = [c for c in PASS_COOKIES if c not in got]
    if missing:
        raise AkamaiChallengeError(f"挑战未通过，缺少 Cookie: {missing}")

    log(f"挑战通过 ✓  变体={data.get('variant')} 耗时={data.get('elapsedMs')}ms  "
        f"会话 Cookie: {sorted(got)}")
    return sess


# ---------------------------------------------------------------- 查询（与 usps_track.py 相同）
def track(sess, label):
    headers = {
        "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": PAGE_URL,
    }
    r = sess.get(f"{PAGE_URL}?tLabels={label}", headers=headers, timeout=30)
    # USPS 响应声明 GB2312，需显式纠正
    r.encoding = "gb2312"
    text = html_to_text(r.text)

    sna = SNA_RE.search(text)
    if sna:
        status = sna.group(0).strip(" \t\r\n\x00\xa0")
    else:
        ms = STATUS_RE.search(text)
        status = ms.group(0).strip(" \t\r\n\x00\xa0") if ms else ""

    return {"tracking_number": label, "http_status": r.status_code,
            "content_type": r.headers.get("content-type", ""),
            "raw_len": len(r.content),
            "status": status, "raw_text": text}


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="USPS 运单在线查询（jsdom 在线执行 Akamai 挑战）")
    parser.add_argument("labels", nargs="*", help="一个或多个运单号")
    parser.add_argument("-q", "--quiet", action="store_true", help="只输出结果")
    parser.add_argument(
        "--proxy",
        help="代理地址，如 http://user:pass@host:port；用于更换出口 IP。"
             "注意：jsdom 版仅支持 http/https 代理（undici 限制），不支持 socks。")
    args = parser.parse_args(argv)

    labels = args.labels or ["9202090395221519986059"]
    verbose = not args.quiet
    proxy = args.proxy

    if verbose and proxy:
        print(f"使用代理: {proxy}", flush=True)

    sess = None
    last_err = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if verbose and attempt > 1:
            print(f"--- 第 {attempt} 次尝试 ---", flush=True)
        try:
            sess = solve_challenge(proxy=proxy, verbose=verbose)
            break
        except AkamaiChallengeError as e:
            last_err = e
            if verbose:
                print("挑战失败:", e, flush=True)
    if sess is None:
        print(f"重试 {MAX_ATTEMPTS} 次后仍失败: {last_err}")
        return 2

    print(flush=True)
    ok = True
    for label in labels:
        if not label.isdigit():
            print(f"警告: 运单号 {label} 含非数字字符，请核对", flush=True)
        elif len(label) != 22:
            print(f"警告: 运单号 {label} 为 {len(label)} 位，"
                  f"USPS 常见标签为 22 位，可能少写/多写了数字", flush=True)

        res = track(sess, label)
        print("=" * 60)
        print("运单号 :", res["tracking_number"])
        print("状态   :", res["status"] or "(未能解析状态)")
        if "Status Not Available" in res["status"] or not res["status"]:
            ok = False
            if verbose:
                print("HTTP:", res["http_status"], "ctype:", res["content_type"],
                      "len:", res["raw_len"])
                print("原始文本:", res["raw_text"][:400])
    print("=" * 60)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
