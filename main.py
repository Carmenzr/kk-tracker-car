import asyncio
import json
import re
import aiohttp
import logging
import os
import random  # 随机运行时长
import itertools  # 代理轮询

# 在线 Akamai 挑战 + 移动端查询能力（单一数据源）。
# 挑战引擎可切换（二者对外接口完全一致）：
#   CHALLENGE_ENGINE=iv8   （默认）真 V8 + 自研 BOM/DOM，进程内计算 -> usps_track.py
#   CHALLENGE_ENGINE=jsdom  Node 子进程里的 jsdom 计算            -> usps_track_jsdom.py
_CHALLENGE_ENGINE = os.environ.get("CHALLENGE_ENGINE", "iv8").strip().lower()
if _CHALLENGE_ENGINE == "jsdom":
    from usps_track_jsdom import (
        solve_challenge,
        PAGE_URL,
        html_to_text,
        SNA_RE,
        PASS_COOKIES,
        AkamaiChallengeError,
    )
else:
    from usps_track import (
        solve_challenge,
        PAGE_URL,
        html_to_text,
        SNA_RE,
        PASS_COOKIES,
        AkamaiChallengeError,
    )

# 配置日志
logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
)

# 打印当前使用的挑战引擎，便于确认是否切换成功
logging.info("挑战引擎(CHALLENGE_ENGINE): %s -> %s",
             _CHALLENGE_ENGINE,
             "usps_track_jsdom" if _CHALLENGE_ENGINE == "jsdom" else "usps_track")

# 单号服务器地址：真实地址一律从环境变量注入（本地导出 env / GitHub 用 secret），
# 源码里只留本地占位符，避免内网地址明文进公共仓库。
# 用 or 而非 get 默认值：空串(未设的 secret)也回落占位符，不会被覆盖成空。
#   本地生产运行： $env:DANHAO_HOST="真实:端口"; $env:DANHAO_MYSQL_HOST="真实:端口"
#   GitHub：仓库 Secrets 添加 DANHAO_HOST / DANHAO_MYSQL_HOST
danhao_server_host_mysql = os.environ.get('DANHAO_MYSQL_HOST') or '127.0.0.1:8082'
danhao_server_host = os.environ.get('DANHAO_HOST') or '127.0.0.1:8082'

# ---------------------------------------------------------------- 移动端解析
# USPS 移动端查询结果结构：一次可传多个 tLabels（逗号分隔），返回一个
# <ul class="package-list">，每个运单一个 <li>：
#   <li class="ui-border-dotted-bottom">
#       ... <div class="tracking-number hidden">运单号</div> ...
#       <div class="package-note">
#           <h3> Delivered: </h3>                -> 状态标题
#           <span> 地点/时间 或 状态描述 </span>   -> 状态描述（含日期）
#       </div>
#   </li>
LI_SPLIT_RE = re.compile(r'<li class="ui-border-dotted-bottom">')
TRACKNUM_RE = re.compile(r'<div class="tracking-number hidden">\s*(\d+)\s*</div>')
HEADING_RE = re.compile(r'trackingNumberHeading">\s*(\d+)\s*</a>')
PKG_NOTE_RE = re.compile(r'<div class="package-note">([\s\S]*?)</div>', re.I)
H3_RE = re.compile(r'<h3>([\s\S]*?)</h3>', re.I)
SPAN_RE = re.compile(r'<span>([\s\S]*?)</span>', re.I)
# 严格日期短语，用于抽取 timestamp（"September 22, 2026"）
DATE_RE = re.compile(
    r"(?:January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+\d{1,2},\s+\d{4}")

# 一次挑战失败时的重试次数
CHALLENGE_MAX_ATTEMPTS = 3
# 单次请求批量查询的运单数上限（USPS 移动端支持一次多个 tLabels）
BATCH_SIZE = 34


def _clean_text(s):
    """清洗移动端 HTML 片段：去 &nbsp;、HTML 注释、标签，折叠空白。"""
    s = s.replace("&nbsp;", " ").replace("\xa0", " ")
    s = re.sub(r"<!--[\s\S]*?-->", " ", s)   # 去 HTML 注释
    s = re.sub(r"<[^>]+>", " ", s)           # 去标签
    s = re.sub(r"\s+", " ", s)
    return s.strip(" \t\r\n\x00")


class USPSLegacyTracker:
    def __init__(self, num_type='mysql', workers: int = 3,
                 use_proxy: bool = False, proxy: str = None,
                 batch_size: int = BATCH_SIZE):
        self.workers = workers
        self.num_type = num_type
        self.batch_size = batch_size  # 一个请求查多少个单号

        self.processed_numbers = set()  # 记录正在处理或已处理的单号
        self.max_cache_size = 5000      # 防止内存溢出

        # 每个 worker 各自持有一份独立会话（requests.Session），不共享。
        # 会话由该 worker 自己执行 Akamai 挑战获得，失效时也只重解自己那一份。

        # --- 代理新增 ---
        self.use_proxy = use_proxy       # 代理总开关
        self.challenge_proxy = proxy     # 未开代理池时使用的固定代理
        self.proxy_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "proxies.txt")
        if self.use_proxy:
            self.proxies = self._load_proxies()
        else:
            self.proxies = []
            logging.info("🚫 代理开关已关闭，所有请求走直连。")
        self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
        self.proxy_lock = asyncio.Lock()
        # ----------------

    # ================= 代理池 =================
    def _load_proxies(self):
        """从本地文件加载 http 代理列表 (格式: ip:port, 每行一个)"""
        proxies = []
        try:
            if os.path.exists(self.proxy_file):
                with open(self.proxy_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith('http://') or line.startswith('https://'):
                            proxies.append(line)
                        else:
                            proxies.append('http://' + line)
            if proxies:
                logging.info(f"✅ 已加载 {len(proxies)} 个代理。")
            else:
                logging.warning("⚠️ 代理文件为空或不存在，将不使用代理。")
        except Exception as e:
            logging.error(f"读取代理文件失败: {e}")
        return proxies

    async def _get_next_proxy(self):
        """轮询获取下一个代理"""
        if not self.proxy_cycle:
            return self.challenge_proxy
        async with self.proxy_lock:
            return next(self.proxy_cycle)

    # ================= 服务端交互 =================
    async def 检查库存(self):
        url = f'http://{danhao_server_host_mysql}/get_mysql_check_num'
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                usps_num = await response.text()
                return usps_num

    async def 上报本机IP(self):
        """启动时获取本机公网 IP 并上报到服务端 /now_ip 接口"""
        public_ip = None
        ip_query_urls = [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://icanhazip.com",
        ]
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # 1. 获取本机公网 IP
                for q_url in ip_query_urls:
                    try:
                        async with session.get(q_url) as resp:
                            if resp.status == 200:
                                public_ip = (await resp.text()).strip()
                                if public_ip:
                                    break
                    except Exception as e:
                        logging.warning(f"通过 {q_url} 获取公网IP失败: {e}")
                        continue

                if not public_ip:
                    logging.warning("⚠️ 未能获取到本机公网IP，跳过上报。")
                    return

                # 2. 上报到服务端
                report_url = f'http://{danhao_server_host_mysql}/now_ip'
                try:
                    async with session.get(report_url, params={"ip": public_ip}) as resp:
                        resp.raise_for_status()
                        logging.info(f"✅ 已上报本机IP [{public_ip}] 到 {report_url}")
                except Exception as e:
                    logging.error(f"上报本机IP失败: {e}")
        except Exception as e:
            logging.error(f"上报本机IP流程异常: {e}")

    async def 获取单号(self, num=10, num_type="mysql"):
        if num_type == 'mysql':
            url = f'http://{danhao_server_host_mysql}/get_mysql_usps_num?num={num}'
        else:
            url = f'http://{danhao_server_host}/get_big_usps_num?num={num}'

        timeout = aiohttp.ClientTimeout(total=30)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    usps_num = await response.text()

            danhao = usps_num.split(',')
            danhao_ls = []
            for i in danhao:
                if "|" in i:
                    danhao_ls.append(i.split("|")[0])
            return danhao_ls

        except Exception as e:
            print(f"❌ 异步获取单号失败: {e}")
            return []

    async def 提交到缓存(self, data):
        if self.num_type == 'mysql':
            url = f'http://{danhao_server_host_mysql}/set_mysql_usps_num_res'
        else:
            url = f'http://{danhao_server_host}/set_big_usps_num_res'

        timeout = aiohttp.ClientTimeout(total=5)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                try:
                    async with session.post(
                            url,
                            json=data,
                            ssl=False,
                            proxy=""
                    ) as r:
                        r.raise_for_status()
                        return True
                except Exception as e:
                    print(f"提交缓存错误 ({url}): {e}")
                    await asyncio.sleep(1)

    async def 原始请求提交到缓存(self, actions):
        try:
            cache_ls = []
            for n, action in enumerate(actions):
                cache_ls.append({"num": action[0], "res": action})
            await self.提交到缓存(cache_ls)
        except Exception as e:
            print(f"原始请求提交失败: {e}")

    # ================= Akamai 会话管理 =================
    def _solve_sync(self, proxy):
        """同步执行 Akamai 挑战，内部重试若干次。返回通过验证的 requests.Session。"""
        last = None
        for _ in range(CHALLENGE_MAX_ATTEMPTS):
            try:
                return solve_challenge(proxy=proxy, verbose=False)
            except AkamaiChallengeError as e:
                last = e
                logging.warning(f"挑战失败，重试中: {e}")
        raise last if last else AkamaiChallengeError("挑战失败")

    async def _solve_session(self, worker_name=""):
        """执行一次 Akamai 挑战，返回一份全新的专属会话（在线程池执行阻塞逻辑）。"""
        loop = asyncio.get_running_loop()
        proxy = await self._get_next_proxy() if self.use_proxy else self.challenge_proxy
        # 日志脱敏：隐藏账密，避免在(公开)Actions 日志里泄露代理凭据
        logging.info(f"🔓 [{worker_name}] 执行 Akamai 挑战获取会话... (proxy={_mask_proxy(proxy)})")
        sess = await loop.run_in_executor(None, self._solve_sync, proxy)
        logging.info(f"✅ [{worker_name}] 会话就绪。")
        return sess

    async def _ensure_session(self, session, worker_name=""):
        """会话为空或 Cookie 失效则重解挑战，否则原样复用。"""
        if session is not None:
            got = {k for k, _ in session.cookies.items()}
            if all(c in got for c in PASS_COOKIES):
                return session
        return await self._solve_session(worker_name)

    def _track_batch_sync(self, sess, labels):
        """同步批量查询：一个请求传多个 tLabels，返回 (http_status, 原始HTML)。"""
        headers = {
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": PAGE_URL,
        }
        tlabels = ",".join(labels)
        r = sess.get(f"{PAGE_URL}?tLabels={tlabels}", headers=headers, timeout=30)
        # USPS 响应声明 GB2312，需显式纠正
        r.encoding = "gb2312"
        return r.status_code, r.text

    async def _fetch_batch(self, session, labels, max_retries: int = 3,
                           worker_name=""):
        """用该 worker 自己的会话批量查询一组运单；响应异常（被拦截/会话失效）则
        重解自己那份会话后重试。返回 (最新会话, [[单号,状态,描述,日期], ...])。"""
        loop = asyncio.get_running_loop()
        last_err = None
        for attempt in range(max_retries):
            try:
                session = await self._ensure_session(session, worker_name)
                status_code, html = await loop.run_in_executor(
                    None, self._track_batch_sync, session, labels)

                # 正常的查询结果一定包含 package-list 容器
                if status_code == 200 and 'package-list' in html:
                    actions = self._parse_batch_html(html)
                    got = {a[0] for a in actions}
                    missing = [n for n in labels if n not in got]
                    if missing:
                        logging.warning(
                            f"⚠️ [{worker_name}] 本批请求 {len(labels)} 个，成功解析 "
                            f"{len(actions)} 个，缺失 {len(missing)} 个（单号无效或超出"
                            f"批量上限被截断，可调小 batch_size）")
                    return session, actions

                logging.warning(
                    f"⚠️ [{worker_name}] 批量响应异常(status={status_code}, "
                    f"len={len(html)})，重解会话后重试")
                session = await self._solve_session(worker_name)

            except Exception as e:
                last_err = e
                logging.warning(
                    f"[{worker_name}] 批量请求异常 (第{attempt+1}次, {len(labels)}个): "
                    f"{type(e).__name__}: {e}")
                try:
                    session = await self._solve_session(worker_name)
                except Exception as e2:
                    last_err = e2
                    await asyncio.sleep(2)

        logging.error(f"❌ [{worker_name}] 批量查询失败，放弃 {len(labels)} 个单号: {last_err}")
        return session, []

    # ================= 结果解析（对齐官方 API 的 4 字段结构） =================
    def _extract_fields(self, tracking_number, li_html):
        """从单个 <li> 片段解析出 [单号, 状态, 描述, 日期]，解析不到返回 None。"""
        status_title = ''
        status_desc = ''

        m_note = PKG_NOTE_RE.search(li_html)
        if m_note:
            block = m_note.group(1)
            m_h3 = H3_RE.search(block)
            m_span = SPAN_RE.search(block)
            if m_h3:
                status_title = _clean_text(m_h3.group(1)).rstrip(':').strip()
            if m_span:
                status_desc = _clean_text(m_span.group(1))

        text_all = html_to_text(li_html)
        # 降级文案（Status Not Available）
        if not status_title:
            sna = SNA_RE.search(text_all)
            if sna:
                status_title = 'Status Not Available'
                status_desc = _clean_text(sna.group(0))

        if not status_title and not status_desc:
            logging.warning(f"[未解析] {tracking_number} 未找到状态")
            return None

        # 抽取日期：优先描述中，其次全文
        md = DATE_RE.search(status_desc) or DATE_RE.search(text_all)
        timestamp = md.group(0) if md else ''

        return [tracking_number, status_title, status_desc, timestamp]

    def _parse_batch_html(self, html):
        """把 package-list 的 HTML 按 <li> 拆分，逐个解析，返回 [[单号,状态,描述,日期], ...]。"""
        actions = []
        parts = LI_SPLIT_RE.split(html)
        for chunk in parts[1:]:  # 第 0 段是 <ul> 之前的内容，跳过
            m = TRACKNUM_RE.search(chunk) or HEADING_RE.search(chunk)
            if not m:
                continue
            num = m.group(1)
            fields = self._extract_fields(num, chunk)
            if fields:
                actions.append(fields)
        return actions

    # ================= 新架构核心 =================
    async def _producer(self, task_queue: asyncio.Queue, result_queue: asyncio.Queue):
        logging.info("📡 生产者启动，进入持续监听模式...")
        try:
            while True:
                try:
                    # 一次多取一些，够喂满所有 worker（每个 worker 一个批次并行查询）
                    fetch_n = self.batch_size * max(self.workers, 1)
                    numbers = await self.获取单号(fetch_n, self.num_type)

                    if not numbers:
                        logging.info(f"目前数据库无新单号，等待 10 秒...")
                        await asyncio.sleep(10)
                        continue

                    # --- 去重核心逻辑 ---
                    new_numbers = []
                    for tn in numbers:
                        if tn not in self.processed_numbers:
                            self.processed_numbers.add(tn)  # 标记为已处理
                            new_numbers.append(tn)
                    # ------------------

                    # 按 batch_size 分批入队，每个批次由一个 worker 用一个请求查询
                    for i in range(0, len(new_numbers), self.batch_size):
                        await task_queue.put(new_numbers[i:i + self.batch_size])

                    if new_numbers:
                        logging.info(
                            f"📥 本轮获取 {len(numbers)} 个，新单号 {len(new_numbers)} 个，"
                            f"分 {(len(new_numbers) + self.batch_size - 1) // self.batch_size} 批入队")

                    # 定期清理去重集合，防止内存无限增长
                    if len(self.processed_numbers) > 10000:
                        logging.info("🧹 清理去重缓存...")
                        self.processed_numbers.clear()

                    # 防压控逻辑（按批次计）
                    while task_queue.qsize() > 50:
                        await asyncio.sleep(10)

                except Exception as e:
                    logging.error(f"生产者单次循环异常: {e}")
                    await asyncio.sleep(10)
        except Exception as e:
            logging.exception("🚨 致命错误：生产者进程完全崩溃！错误详情：")

    async def _worker(self, task_queue: asyncio.Queue, result_queue: asyncio.Queue,
                      worker_name="Worker"):
        """搬砖工：持有自己独立的会话；拿一批单号 -> 一个请求批量查 -> 逐个扔给提交者。
        会话首次使用时由本 worker 自己执行 Akamai 挑战获得，之后复用。"""
        session = None  # 本 worker 专属会话，与其他 worker 不共享
        try:
            while True:
                batch = await task_queue.get()

                if batch is None:
                    task_queue.task_done()
                    break

                try:
                    session, actions = await self._fetch_batch(
                        session, batch, worker_name=worker_name)
                    for action in actions:
                        await result_queue.put(action)
                finally:
                    task_queue.task_done()
        except Exception as e:
            logging.exception(f"🚨 致命错误：{worker_name} 进程完全崩溃！错误详情：")

    async def _submitter(self, result_queue: asyncio.Queue):
        """专门负责将结果交给缓存函数（result_queue 里已是解析好的 [单号,状态,描述,日期]）"""
        total_submitted = 0
        batch = []
        WAIT_TIMEOUT = 8.0

        try:
            while True:
                try:
                    # 带有超时的阻塞等待
                    result = await asyncio.wait_for(result_queue.get(), timeout=WAIT_TIMEOUT)

                    # 优雅关机信号处理
                    if result is None:
                        if batch:
                            await self.原始请求提交到缓存(batch)
                            logging.info(f"✅ 收到停止信号，最后一批 {len(batch)} 条数据已提交。")
                        result_queue.task_done()
                        break

                    total_submitted += 1
                    print(f"[序号: {total_submitted}] {result[0]},{result}")
                    batch.append(result)

                    result_queue.task_done()

                    # 攒够 30 个正常提交
                    if len(batch) >= 30:
                        await self.原始请求提交到缓存(batch)
                        logging.info(f"✅ 成功满载批量提交 {len(batch)} 条数据！当前总计提交: {total_submitted}")
                        batch.clear()

                except asyncio.TimeoutError:
                    # 超时逻辑：如果没等到新结果，但手里还有没提交的数据，直接提交！
                    if batch:
                        await self.原始请求提交到缓存(batch)
                        logging.info(f"⏳ 触发闲时刷新：提交攒留的 {len(batch)} 条数据！")
                        batch.clear()
        except Exception as e:
            logging.exception("🚨 致命错误：Submitter 进程完全崩溃！错误详情：")

    async def run_system(self):
        """主控调度"""
        task_queue = asyncio.Queue()
        result_queue = asyncio.Queue()

        # 0. 启动时上报本机公网 IP
        await self.上报本机IP()

        # 1. 启动结果提交协程
        submitter_task = asyncio.create_task(self._submitter(result_queue), name="Submitter")

        # 2. 启动 N 个查询工人协程；每个 worker 首次拿到批次时自行执行挑战、
        #    持有各自独立的会话（互不共享）。
        workers = []
        for i in range(self.workers):
            name = f"Worker-{i}"
            worker = asyncio.create_task(
                self._worker(task_queue, result_queue, worker_name=name), name=name)
            workers.append(worker)

        # 3. 启动单号获取
        producer_task = asyncio.create_task(
            self._producer(task_queue, result_queue), name="Producer")

        # 运行时长：默认 60 秒，可用环境变量 RUN_SECONDS 覆盖（无人值守场景）
        run_seconds = int(os.environ.get("RUN_SECONDS", "60"))
        logging.info(f"🚀 系统就绪！共启动 {self.workers} 个并发进程，将运行约 {run_seconds} 秒后优雅退出...")

        # 将所有关键任务放进一个列表
        all_tasks = workers + [submitter_task, producer_task]

        # 4. 主程序挂起：等待 [运行超时] 或 [任意任务异常退出]
        done, pending = await asyncio.wait(
                all_tasks,
                timeout=run_seconds,
                return_when=asyncio.FIRST_EXCEPTION
        )

        # 检查是否有任务因异常提前退出
        crashed = False
        for task in done:
            if task.exception():
                crashed = True
                logging.error(f"🚨 检测到核心任务 [{task.get_name()}] 崩溃！")
                try:
                    task.result()
                except Exception as e:
                    logging.exception("崩溃详情：")

        if crashed:
            logging.error("因核心任务崩溃，开始优雅收尾...")
        else:
            logging.info(f"⏰ 已运行 {run_seconds} 秒，开始优雅退出流程...")

        # ============ 优雅退出流程 ============
        # 5. 先停掉生产者
        producer_task.cancel()
        try:
            await producer_task
        except asyncio.CancelledError:
            pass

        # 6. 等待已在队列中的单号被处理完
        try:
            logging.info(f"⏳ 等待剩余 {task_queue.qsize()} 个单号处理完成（在途请求收尾）...")
            await asyncio.wait_for(task_queue.join(), timeout=120)
            logging.info("✅ 队列中所有单号均已处理完成。")
        except asyncio.TimeoutError:
            logging.warning("⚠️ 等待队列处理超时（120秒），强制进入下一步收尾。")

        # 7. 通知所有 worker 退出
        for _ in workers:
            await task_queue.put(None)
        await asyncio.gather(*workers, return_exceptions=True)

        # 8. 通知 submitter 提交最后一批后退出
        await result_queue.put(None)
        try:
            await asyncio.wait_for(submitter_task, timeout=30)
        except asyncio.TimeoutError:
            logging.warning("⚠️ 等待提交者收尾超时，强制取消。")
            submitter_task.cancel()

        # 9. 清理仍在挂起的任务
        for p in pending:
            if not p.done():
                p.cancel()

        logging.info("👋 程序已优雅退出。")


def _env_bool(name, default=False):
    """把环境变量解析成布尔值。"""
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _norm_proxy(p):
    """规范化代理串：无协议头则补 http://；空则返回 None。"""
    if not p:
        return None
    p = p.strip()
    if not p:
        return None
    if not p.startswith(("http://", "https://", "socks5://", "socks5h://")):
        p = "http://" + p
    return p


def _mask_proxy(p):
    """脱敏：隐藏账密，只留 host:port。"""
    if not p:
        return "none"
    try:
        return "***@" + (p.split("@", 1)[1] if "@" in p else p.split("://", 1)[-1])
    except Exception:
        return "***"


# ================= 使用示例 =================
if __name__ == "__main__":
    # 运行参数优先读环境变量（供 GitHub Actions 等无人值守场景使用），
    # 不设置任何环境变量时，行为与本地默认一致（big / 3 workers / batch 15 / 直连）。
    num_type = os.environ.get("NUM_TYPE", "big")
    use_proxy = _env_bool("USE_PROXY", False)  # True=走代理池(读取proxies.txt), False=用下面的固定代理/直连
    workers = int(os.environ.get("WORKERS", "3"))
    batch_size = int(os.environ.get("BATCH_SIZE", "15"))
    # 固定代理：设了 PROXY 环境变量就整个会话走它(挑战+查询同一出口)，不设则直连。
    proxy = _norm_proxy(os.environ.get("PROXY"))
    logging.info(f"代理配置: use_proxy={use_proxy} proxy={_mask_proxy(proxy)}")

    # workers=并发批次数; batch_size=单个请求查多少个单号（USPS 移动端支持一次多个）
    tracker = USPSLegacyTracker(num_type=num_type, workers=workers,
                                use_proxy=use_proxy, proxy=proxy, batch_size=batch_size)

    try:
        asyncio.run(tracker.run_system())
    except KeyboardInterrupt:
        print("\n🛑 手动停止程序。")
