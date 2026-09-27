#!/usr/bin/env node
"use strict";
/*
 * USPS 移动端 Akamai 挑战求解器（jsdom 版）
 * ------------------------------------------------------------------
 * 作用：替代 iv8（真 V8 + 自研 BOM/DOM）那一套“JS 计算”，改用 jsdom 在 Node 里
 *      跑 Akamai 传感器脚本，拿到通行 Cookie（JSESSIONID / NSC_psjhjo-n_443 / w3IsGuY1）。
 *
 * 与 iv8 版的关键区别：
 *   - iv8 版：Python 用 requests 掌控网络，JS 每个请求经 bridge 回到 Python；手动 drain 事件循环。
 *   - 本版：jsdom 自带 undici 网络栈。通过 resources.dispatcher 注入自定义 undici
 *           dispatcher（可选 ProxyAgent 走代理），通过 cookieJar 共享 tough-cookie 罐。
 *           XHR / 子资源加载 / document.cookie 全部走这套，Set-Cookie 自动落入 cookieJar。
 *           事件循环是 Node 原生的，无需手动 drain，只需轮询 cookieJar 等通行 Cookie 出现。
 *
 * 输入（命令行参数或环境变量）：
 *   --proxy <url>        或  env PROXY          http 代理，如 http://user:pass@host:port
 *   --timeout <ms>       或  env CHALLENGE_TIMEOUT_MS   总超时，默认 25000
 *   --verbose            或  env VERBOSE=1      把过程日志打到 stderr
 *   --insecure           或  env SSL_VERIFY=0   跳过 TLS 校验（本地中间人调试用）
 *
 * 输出：stdout 打印一行 JSON：
 *   { ok, cookies:{name:value...}, jar:[{key,value,domain,path}...],
 *     missing:[...], variant, pageStatus, elapsedMs, error? }
 * 退出码：成功 0，失败 1。
 */

const { JSDOM, CookieJar, VirtualConsole } = require("jsdom");
const { Agent, ProxyAgent, request } = require("undici");

// ---------------------------------------------------------------- 常量
const BASE = "https://m.usps.com";
const PAGE_URL = `${BASE}/m/TrackConfirmAction`;
// USPS 官方 App 内嵌 WebView 的固定 UA（与 iv8 版一致，唯一被服务端宽松放行的标识）
const UA = "Emb/And/1.0";
const PASS_COOKIES = ["JSESSIONID", "NSC_psjhjo-n_443", "w3IsGuY1"];

// ---------------------------------------------------------------- 参数解析
function parseArgs(argv) {
  const out = { proxy: null, timeout: 25000, verbose: false, insecure: false };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--proxy") out.proxy = argv[++i];
    else if (a === "--timeout") out.timeout = parseInt(argv[++i], 10) || out.timeout;
    else if (a === "--verbose") out.verbose = true;
    else if (a === "--insecure") out.insecure = true;
  }
  if (!out.proxy && process.env.PROXY) out.proxy = process.env.PROXY;
  if (process.env.CHALLENGE_TIMEOUT_MS) {
    out.timeout = parseInt(process.env.CHALLENGE_TIMEOUT_MS, 10) || out.timeout;
  }
  if (process.env.VERBOSE && process.env.VERBOSE !== "0") out.verbose = true;
  const sv = (process.env.SSL_VERIFY || "1").trim().toLowerCase();
  if (["0", "false", "no"].includes(sv)) out.insecure = true;
  return out;
}

const ARGS = parseArgs(process.argv);
function log(...a) {
  if (ARGS.verbose) process.stderr.write(a.join(" ") + "\n");
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------- 代理串规范化
function normProxy(p) {
  if (!p) return null;
  p = String(p).trim();
  if (!p) return null;
  if (!/^(https?|socks5h?):\/\//i.test(p)) p = "http://" + p;
  return p;
}

// ---------------------------------------------------------------- 底层网络（undici dispatcher）
function buildDispatcher(proxy, insecure) {
  const connect = insecure ? { rejectUnauthorized: false } : {};
  if (proxy) {
    if (/^socks/i.test(proxy)) {
      // undici 原生不支持 socks，明确报错让上层降级/换 http 代理
      throw new Error("jsdom 版暂不支持 socks 代理（undici 限制），请用 http 代理");
    }
    log("[net] 使用代理:", maskProxy(proxy));
    return new ProxyAgent({ uri: proxy, connect, requestTls: connect });
  }
  return new Agent({ connect });
}

function maskProxy(p) {
  if (!p) return "none";
  try {
    return "***@" + (p.includes("@") ? p.split("@")[1] : p.split("://")[1]);
  } catch (e) {
    return "***";
  }
}

// 把一段 set-cookie（string 或 array）写入 cookieJar
function storeSetCookie(jar, setCookie, url) {
  if (!setCookie) return;
  const arr = Array.isArray(setCookie) ? setCookie : [setCookie];
  for (const c of arr) {
    try {
      jar.setCookieSync(c, url, { ignoreError: true });
    } catch (e) { /* ignore */ }
  }
}

// ---------------------------------------------------------------- 环境补丁（降低崩溃/被检测概率）
function patchEnv(window) {
  const nav = window.navigator;
  const define = (obj, key, val) => {
    try {
      Object.defineProperty(obj, key, { get: () => val, configurable: true });
    } catch (e) { /* ignore */ }
  };

  // 对齐 iv8 版的设备画像（Android WebView / 中文）
  define(nav, "platform", "Linux armv8l");
  define(nav, "language", "zh-CN");
  define(nav, "languages", ["zh-CN", "en-US"]);
  define(nav, "maxTouchPoints", 5);
  define(nav, "webdriver", false); // 反自动化探针

  // jsdom 未实现导航：location.assign/replace/reload 会往 virtualConsole 报 "Not implemented"。
  // 挑战成功路径其实只依赖 XHR 的 Set-Cookie，导航仅是装饰。这里静默成 no-op，避免噪声/被探针捕获异常。
  try {
    const loc = window.location;
    for (const m of ["assign", "replace", "reload"]) {
      try { Object.defineProperty(loc, m, { value: () => {}, configurable: true }); } catch (e) { /* ignore */ }
    }
  } catch (e) { /* ignore */ }

  // canvas 指纹探针：jsdom 无渲染后端，toDataURL 会抛 "Not implemented"。
  // 给个稳定的假值，避免 sensor 在 canvas 分支抛错中断。
  try {
    const proto = window.HTMLCanvasElement && window.HTMLCanvasElement.prototype;
    if (proto) {
      const FAKE_PNG =
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
      proto.toDataURL = function () { return FAKE_PNG; };
      const origGetContext = proto.getContext;
      proto.getContext = function (type) {
        try {
          const ctx = origGetContext ? origGetContext.apply(this, arguments) : null;
          if (ctx) return ctx;
        } catch (e) { /* fallthrough */ }
        // 最小 2d/webgl 假上下文，探针取不到会走 catch，不至于 fatal
        return null;
      };
    }
  } catch (e) { /* ignore */ }
}

// ---------------------------------------------------------------- 主流程
async function main() {
  const started = Date.now();
  const proxy = normProxy(ARGS.proxy);
  const dispatcher = buildDispatcher(proxy, ARGS.insecure);
  const cookieJar = new CookieJar();

  const result = {
    ok: false, cookies: {}, jar: [], missing: [...PASS_COOKIES],
    variant: null, pageStatus: 0, elapsedMs: 0, error: null,
  };

  try {
    // STEP 1：拉挑战页（走同一 dispatcher + 手动把 Set-Cookie 灌进共享罐）
    log("STEP 1/3 获取挑战页 ...");
    const r0 = await request(PAGE_URL, {
      dispatcher,
      method: "GET",
      headers: {
        "User-Agent": UA,
        "Accept-Language": "zh-CN,en-US;q=0.8",
        // USPS 源站据此判定“来自 App”，缺失会返回 service currently unavailable
        "X-Requested-With": "com.usps",
        Accept: "text/html,application/xhtml+xml,*/*",
      },
    });
    result.pageStatus = r0.statusCode;
    const istl = r0.headers["istl-response"];
    const html = await r0.body.text();
    storeSetCookie(cookieJar, r0.headers["set-cookie"], PAGE_URL);
    log("  01:", r0.statusCode, "ISTL:", istl, "len:", html.length);

    if (istl === undefined || istl === null) {
      throw new Error("挑战页缺少 ISTL-RESPONSE 标记 (status=" + r0.statusCode + ")");
    }
    result.variant = /src="\/AH9QIE\/[^"]+"/.test(html) ? "normal" : "inline";
    log("  变体:", result.variant);

    // STEP 2：构建 jsdom，注入自定义 dispatcher + 共享 cookieJar，执行页面脚本
    log("STEP 2/3 构建 jsdom 并执行挑战脚本 ...");
    const virtualConsole = new VirtualConsole();
    virtualConsole.on("jsdomError", (e) => {
      // 吞掉 "Not implemented: navigation"、canvas 等噪声；verbose 时才透出
      log("  [jsdomError]", (e && e.message) || e);
    });
    if (ARGS.verbose) {
      virtualConsole.on("error", (...a) => log("  [console.error]", ...a));
    }

    const dom = new JSDOM(html, {
      url: PAGE_URL,
      referrer: PAGE_URL,
      contentType: "text/html",
      runScripts: "dangerously",
      pretendToBeVisual: true, // 提供 requestAnimationFrame 等
      resources: { userAgent: UA, dispatcher },
      cookieJar,
      virtualConsole,
      beforeParse: patchEnv,
    });

    // STEP 3：推进事件循环 —— jsdom 用 Node 原生事件循环，这里只需轮询等通行 Cookie
    log("STEP 3/3 等待传感器提交并回种 Cookie ...");
    const deadline = started + ARGS.timeout;
    let missing = [...PASS_COOKIES];
    while (Date.now() < deadline) {
      await sleep(250);
      const got = new Set(cookieJar.serializeSync().cookies.map((c) => c.key));
      missing = PASS_COOKIES.filter((c) => !got.has(c));
      if (missing.length === 0) break;
    }

    // 收尾：导出所有 cookie
    const serialized = cookieJar.serializeSync().cookies || [];
    result.jar = serialized.map((c) => ({
      key: c.key, value: c.value, domain: c.domain, path: c.path || "/",
    }));
    for (const c of serialized) result.cookies[c.key] = c.value;
    result.missing = missing;
    result.ok = missing.length === 0;

    try { dom.window.close(); } catch (e) { /* ignore */ }
  } catch (e) {
    result.error = (e && e.message) || String(e);
  } finally {
    result.elapsedMs = Date.now() - started;
    try { await dispatcher.close(); } catch (e) { /* ignore */ }
  }

  process.stdout.write(JSON.stringify(result) + "\n");
  process.exit(result.ok ? 0 : 1);
}

main().catch((e) => {
  process.stdout.write(JSON.stringify({
    ok: false, cookies: {}, jar: [], missing: PASS_COOKIES,
    variant: null, pageStatus: 0, elapsedMs: 0,
    error: (e && e.message) || String(e),
  }) + "\n");
  process.exit(1);
});
