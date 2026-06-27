use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use rand::Rng;
use regex::Regex;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::Mutex;

// ================= 配置常量 =================
const DANHAO_SERVER_HOST_MYSQL: &str = "43.128.111.219:8082";
const DANHAO_SERVER_HOST: &str = "kungfu.bj.cn:8082";

const BASE_URL: &str = "https://apis.usps.com/tracking/v3/tracking/{}?expand=DETAIL";
const AUTH_URL: &str = "https://apis.usps.com/oauth2/v3/token";
const TOKEN_FILE: &str = "usps_token_cache.json";
const PROXY_FILE: &str = "proxies.txt";

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

// ================= Token 状态 =================
#[derive(Default)]
struct TokenState {
    token: Option<String>,
    expiry: f64,
}

// ================= 主结构体 =================
struct Tracker {
    client_id: String,
    client_secret: String,
    num_type: String,
    workers: usize,

    direct_client: Client,
    proxy_clients: Vec<Client>,
    proxy_idx: AtomicUsize,

    token: Mutex<TokenState>,
    processed_numbers: Mutex<HashSet<String>>,
}

impl Tracker {
    fn new(
        client_id: String,
        client_secret: String,
        num_type: String,
        workers: usize,
        use_proxy: bool,
    ) -> Arc<Self> {
        let direct_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("构建直连客户端失败");

        let proxy_clients = if use_proxy {
            let proxies = Self::load_proxies();
            proxies
                .iter()
                .filter_map(|p| {
                    reqwest::Proxy::all(p)
                        .ok()
                        .and_then(|proxy| {
                            Client::builder()
                                .timeout(Duration::from_secs(30))
                                .proxy(proxy)
                                .build()
                                .ok()
                        })
                })
                .collect()
        } else {
            log::info!("🚫 代理开关已关闭，所有请求走直连。");
            Vec::new()
        };

        let tracker = Arc::new(Tracker {
            client_id,
            client_secret,
            num_type,
            workers,
            direct_client,
            proxy_clients,
            proxy_idx: AtomicUsize::new(0),
            token: Mutex::new(TokenState::default()),
            processed_numbers: Mutex::new(HashSet::new()),
        });

        tracker.load_local_token_blocking();
        tracker
    }

    fn load_proxies() -> Vec<String> {
        let mut proxies = Vec::new();
        match std::fs::read_to_string(PROXY_FILE) {
            Ok(content) => {
                for line in content.lines() {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    if line.starts_with("http://") || line.starts_with("https://") {
                        proxies.push(line.to_string());
                    } else {
                        proxies.push(format!("http://{}", line));
                    }
                }
                if proxies.is_empty() {
                    log::warn!("⚠️ 代理文件为空或不存在，将不使用代理。");
                } else {
                    log::info!("✅ 已加载 {} 个代理。", proxies.len());
                }
            }
            Err(_) => log::warn!("⚠️ 代理文件为空或不存在，将不使用代理。"),
        }
        proxies
    }

    /// 轮询取下一个用于查询的 client（无代理则返回直连）
    fn next_fetch_client(&self) -> (&Client, Option<usize>) {
        if self.proxy_clients.is_empty() {
            (&self.direct_client, None)
        } else {
            let idx = self.proxy_idx.fetch_add(1, Ordering::Relaxed) % self.proxy_clients.len();
            (&self.proxy_clients[idx], Some(idx))
        }
    }

    fn load_local_token_blocking(&self) {
        if let Ok(content) = std::fs::read_to_string(TOKEN_FILE) {
            if let Ok(data) = serde_json::from_str::<Value>(&content) {
                let token = data.get("token").and_then(|v| v.as_str()).map(String::from);
                let expiry = data.get("expiry").and_then(|v| v.as_f64()).unwrap_or(0.0);
                if let Ok(mut guard) = self.token.try_lock() {
                    guard.token = token;
                    guard.expiry = expiry;
                    if now_ts() < expiry - 60.0 {
                        log::info!("✅ 已成功从本地加载未过期的 USPS Token。");
                    } else {
                        log::info!("⚠️ 本地 Token 已过期，将重新获取。");
                    }
                }
            }
        }
    }

    fn save_local_token(token: &str, expiry: f64) {
        let data = json!({ "token": token, "expiry": expiry });
        if let Err(e) = std::fs::write(TOKEN_FILE, data.to_string()) {
            log::error!("保存 Token 到本地文件失败: {}", e);
        }
    }

    // ================= 上报本机 IP =================
    async fn report_ip(&self) {
        let ip_query_urls = [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://icanhazip.com",
        ];
        let mut public_ip: Option<String> = None;
        for q_url in ip_query_urls {
            match self
                .direct_client
                .get(q_url)
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) if resp.status().as_u16() == 200 => {
                    if let Ok(text) = resp.text().await {
                        let ip = text.trim().to_string();
                        if !ip.is_empty() {
                            public_ip = Some(ip);
                            break;
                        }
                    }
                }
                Ok(_) => continue,
                Err(e) => {
                    log::warn!("通过 {} 获取公网IP失败: {}", q_url, e);
                    continue;
                }
            }
        }

        let public_ip = match public_ip {
            Some(ip) => ip,
            None => {
                log::warn!("⚠️ 未能获取到本机公网IP，跳过上报。");
                return;
            }
        };

        let report_url = format!("http://{}/now_ip", DANHAO_SERVER_HOST_MYSQL);
        match self
            .direct_client
            .get(&report_url)
            .query(&[("ip", &public_ip)])
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                log::info!("✅ 已上报本机IP [{}] 到 {}", public_ip, report_url);
            }
            Ok(resp) => log::error!("上报本机IP失败: HTTP {}", resp.status()),
            Err(e) => log::error!("上报本机IP失败: {}", e),
        }
    }

    // ================= 获取单号 =================
    async fn get_numbers(&self, num: usize) -> Vec<String> {
        let url = if self.num_type == "mysql" {
            format!(
                "http://{}/get_mysql_usps_num?num={}",
                DANHAO_SERVER_HOST_MYSQL, num
            )
        } else {
            format!("http://{}/get_big_usps_num?num={}", DANHAO_SERVER_HOST, num)
        };

        match self
            .direct_client
            .get(&url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
        {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp) => match resp.text().await {
                    Ok(text) => text
                        .split(',')
                        .filter(|i| i.contains('|'))
                        .map(|i| i.split('|').next().unwrap_or("").to_string())
                        .collect(),
                    Err(e) => {
                        println!("❌ 异步获取单号失败: {}", e);
                        Vec::new()
                    }
                },
                Err(e) => {
                    println!("❌ 异步获取单号失败: {}", e);
                    Vec::new()
                }
            },
            Err(e) => {
                println!("❌ 异步获取单号失败: {}", e);
                Vec::new()
            }
        }
    }

    // ================= 提交到缓存 =================
    async fn submit_to_cache(&self, data: &Value) {
        let url = if self.num_type == "mysql" {
            format!("http://{}/set_mysql_usps_num_res", DANHAO_SERVER_HOST_MYSQL)
        } else {
            format!("http://{}/set_big_usps_num_res", DANHAO_SERVER_HOST)
        };

        loop {
            match self
                .direct_client
                .post(&url)
                .json(data)
                .timeout(Duration::from_secs(5))
                .send()
                .await
            {
                Ok(resp) => match resp.error_for_status() {
                    Ok(_) => return,
                    Err(e) => {
                        println!("提交缓存错误 ({}): {}", url, e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                },
                Err(e) => {
                    println!("提交缓存错误 ({}): {}", url, e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// 把一批 action（每个为 [num, title, desc, timestamp]）打包提交
    async fn submit_actions(&self, actions: &[Vec<String>]) {
        let cache_ls: Vec<Value> = actions
            .iter()
            .map(|action| json!({ "num": action.get(0).cloned().unwrap_or_default(), "res": action }))
            .collect();
        self.submit_to_cache(&Value::Array(cache_ls)).await;
    }

    // ================= 获取/刷新 Token =================
    async fn get_valid_token(&self) -> Result<String, String> {
        let mut guard = self.token.lock().await;
        if let Some(t) = &guard.token {
            if now_ts() < guard.expiry - 60.0 {
                return Ok(t.clone());
            }
        }

        log::info!("🔄 请求/刷新 USPS Token 中...");
        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", self.client_id.as_str()),
            ("client_secret", self.client_secret.as_str()),
            ("scope", "tracking"),
        ];

        let resp = self
            .direct_client
            .post(AUTH_URL)
            .form(&params)
            .send()
            .await
            .map_err(|e| format!("OAuth2 请求失败: {}", e))?;

        if resp.status().as_u16() == 200 {
            let data: Value = resp
                .json()
                .await
                .map_err(|e| format!("OAuth2 响应解析失败: {}", e))?;
            let token = data
                .get("access_token")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let expires_in = data
                .get("expires_in")
                .and_then(|v| v.as_f64())
                .unwrap_or(3600.0);
            let expiry = now_ts() + expires_in;

            guard.token = Some(token.clone());
            guard.expiry = expiry;
            Self::save_local_token(&token, expiry);
            log::info!("✅ Token 获取成功并已保存到本地。");
            Ok(token)
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(format!("OAuth2 认证失败: {}", text))
        }
    }

    // ================= 单个单号查询 =================
    async fn fetch_single(&self, tracking_number: &str) -> Value {
        let url = BASE_URL.replace("{}", tracking_number);
        let max_retries = 3;
        let mut attempt: i32 = 0;
        let mut proxy_switches = 0usize;
        let max_proxy_switches = if self.proxy_clients.is_empty() {
            0
        } else {
            self.proxy_clients.len().max(10)
        };

        while attempt < max_retries {
            let token = match self.get_valid_token().await {
                Ok(t) => t,
                Err(e) => {
                    log::warn!("获取 Token 失败 {}: {}", tracking_number, e);
                    tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
                    attempt += 1;
                    continue;
                }
            };

            let (client, idx) = self.next_fetch_client();
            let result = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", token))
                .header("Accept", "application/json")
                .send()
                .await;

            match result {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if status == 200 {
                        match resp.json::<Value>().await {
                            Ok(v) => return v,
                            Err(e) => {
                                log::warn!("解析 200 响应失败 {}: {}", tracking_number, e);
                                attempt += 1;
                                continue;
                            }
                        }
                    } else if status == 404 {
                        return json!({
                            "trackingNumber": tracking_number,
                            "error": "Not Found",
                            "status": 404
                        });
                    } else if matches!(status, 429 | 500 | 502 | 503 | 504) {
                        let retry_after = resp
                            .headers()
                            .get("Retry-After")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(2u64.pow(attempt as u32));
                        tokio::time::sleep(Duration::from_secs(retry_after)).await;
                        attempt += 1;
                    } else {
                        let text = resp.text().await.unwrap_or_default();
                        return json!({
                            "trackingNumber": tracking_number,
                            "error": text,
                            "status": status
                        });
                    }
                }
                Err(e) => {
                    // 代理类/连接类错误：立即换下一个代理重试（不消耗 attempt）
                    if !self.proxy_clients.is_empty() && (e.is_connect() || e.is_timeout()) {
                        proxy_switches += 1;
                        log::warn!(
                            "代理失效 {} (proxy_idx={:?}): {}, 立即换下一个",
                            tracking_number,
                            idx,
                            e
                        );
                        if proxy_switches >= max_proxy_switches {
                            log::error!(
                                "❌ {} 连续 {} 个代理均失效, 放弃",
                                tracking_number,
                                proxy_switches
                            );
                            return json!({
                                "trackingNumber": tracking_number,
                                "error": "All proxies unavailable"
                            });
                        }
                        continue;
                    }
                    log::warn!(
                        "请求异常 {} (第{}次): {}",
                        tracking_number,
                        attempt + 1,
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
                    attempt += 1;
                }
            }
        }

        json!({
            "trackingNumber": tracking_number,
            "error": "Max retries exceeded"
        })
    }
}

// ================= 时间转换 =================
fn to_standard_time(date_str: &str) -> Option<String> {
    let s = date_str.replace('Z', "+00:00");
    if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
        return Some(dt.format("%B %d, %Y").to_string());
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") {
        return Some(ndt.format("%B %d, %Y").to_string());
    }
    if let Ok(nd) = NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
        return Some(nd.format("%B %d, %Y").to_string());
    }
    None
}

/// 判断 error 字段是否表示“有错误”，返回内部 error 对象
fn extract_error(j: &Value) -> Option<Value> {
    match j.get("error") {
        None => None,
        Some(Value::String(s)) => match serde_json::from_str::<Value>(s) {
            Ok(v) => match v.get("error") {
                Some(e) if !is_empty_obj(e) => Some(e.clone()),
                _ => None,
            },
            Err(_) => Some(json!({ "message": s })),
        },
        Some(other) => Some(json!({ "message": other.to_string() })),
    }
}

fn is_empty_obj(v: &Value) -> bool {
    match v {
        Value::Object(m) => m.is_empty(),
        Value::Null => true,
        _ => false,
    }
}

/// 解析单号 JSON，返回一个 action [num, title, desc, timestamp]，无效则 None
fn parse_danhao_json(j: &Value, month_re: &Regex) -> Option<Vec<String>> {
    let tracking_number = j
        .get("trackingNumber")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let status_title: String;
    let status_desc: String;
    let mut timestamp = String::new();

    if let Some(err) = extract_error(j) {
        let message = err.get("message").and_then(|m| m.as_str()).unwrap_or("");
        if message.contains("Tracking is not available") {
            status_title = "Tracking is not available".to_string();
            status_desc = message.to_string();
        } else {
            status_title = "Unknow".to_string();
            status_desc = "Unknow".to_string();
            println!("[Unknow] {} 原始返回: {}", tracking_number, j);
        }
    } else {
        status_title = j
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        status_desc = j
            .get("statusSummary")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mail_intake = j
            .get("mailPieceIntakeDate")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if let Some(events) = j.get("trackingEvents").and_then(|v| v.as_array()) {
            if let Some(ev) = events.first() {
                if let Some(g) = ev.get("GMTTimestamp").and_then(|v| v.as_str()) {
                    if !g.is_empty() {
                        if let Some(t) = to_standard_time(g) {
                            timestamp = t;
                        }
                    }
                }
                if let Some(e) = ev.get("eventTimestamp").and_then(|v| v.as_str()) {
                    if !e.is_empty() {
                        if let Some(t) = to_standard_time(e) {
                            timestamp = t;
                        }
                    }
                }
            }
        }
        if timestamp.is_empty() && !mail_intake.is_empty() {
            if let Some(t) = to_standard_time(mail_intake) {
                timestamp = t;
            }
        }
        if timestamp.is_empty() {
            if let Some(m) = month_re.find(&status_desc) {
                timestamp = m.as_str().to_string();
            }
        }
    }

    if status_title == "Unknow" {
        return None;
    }
    if !status_desc.is_empty() {
        Some(vec![tracking_number, status_title, status_desc, timestamp])
    } else {
        None
    }
}

// ================= 生产者 =================
async fn producer(
    tracker: Arc<Tracker>,
    task_tx: async_channel::Sender<String>,
    pending: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
) {
    log::info!("📡 生产者启动，进入持续监听模式...");
    while !shutdown.load(Ordering::Relaxed) {
        let numbers = tracker.get_numbers(34).await;

        if numbers.is_empty() {
            log::info!("目前数据库无新单号，等待 10 秒...");
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }

        let mut new_count = 0;
        {
            let mut seen = tracker.processed_numbers.lock().await;
            for tn in &numbers {
                if !seen.contains(tn) {
                    if task_tx.send(tn.clone()).await.is_err() {
                        return;
                    }
                    seen.insert(tn.clone());
                    pending.fetch_add(1, Ordering::SeqCst);
                    new_count += 1;
                }
            }
            if seen.len() > 10000 {
                log::info!("🧹 清理去重缓存...");
                seen.clear();
            }
        }

        if new_count > 0 {
            log::info!("📥 本轮获取 {} 个，其中新单号 {} 个", numbers.len(), new_count);
        }

        // 防压控：队列积压超过 500 则等待
        while task_tx.len() > 500 && !shutdown.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
}

// ================= 工人 =================
async fn worker(
    tracker: Arc<Tracker>,
    task_rx: async_channel::Receiver<String>,
    result_tx: async_channel::Sender<Value>,
    pending: Arc<AtomicUsize>,
) {
    while let Ok(tracking_number) = task_rx.recv().await {
        let result = tracker.fetch_single(&tracking_number).await;
        let status_403 = result.get("status").and_then(|v| v.as_u64()) == Some(403);
        if status_403 {
            log::warn!("⚠️ 单号 {} 返回 403,已跳过提交", tracking_number);
            pending.fetch_sub(1, Ordering::SeqCst);
            continue;
        }
        let _ = result_tx.send(result).await;
        pending.fetch_sub(1, Ordering::SeqCst);
    }
}

// ================= 提交者 =================
async fn submitter(
    tracker: Arc<Tracker>,
    result_rx: async_channel::Receiver<Value>,
    month_re: Regex,
) {
    let mut total_submitted = 0usize;
    let mut batch: Vec<Vec<String>> = Vec::new();
    let wait_timeout = Duration::from_secs(8);

    loop {
        match tokio::time::timeout(wait_timeout, result_rx.recv()).await {
            Ok(Ok(result)) => {
                if let Some(res) = parse_danhao_json(&result, &month_re) {
                    println!("[序号: {}] {:?}", total_submitted + 1, res);
                    batch.push(res);
                }
                total_submitted += 1;

                if batch.len() >= 30 {
                    tracker.submit_actions(&batch).await;
                    log::info!(
                        "✅ 成功满载批量提交 {} 条数据！当前总计提交: {}",
                        batch.len(),
                        total_submitted
                    );
                    batch.clear();
                }
            }
            Ok(Err(_)) => {
                // 通道关闭：提交剩余数据并退出
                if !batch.is_empty() {
                    tracker.submit_actions(&batch).await;
                    log::info!("✅ 收到停止信号，最后一批 {} 条数据已提交。", batch.len());
                    batch.clear();
                }
                break;
            }
            Err(_) => {
                // 超时：闲时刷新
                if !batch.is_empty() {
                    tracker.submit_actions(&batch).await;
                    log::info!("⏳ 触发闲时刷新：提交攒留的 {} 条数据！", batch.len());
                    batch.clear();
                }
            }
        }
    }
}

// ================= 主控调度 =================
async fn run_system(tracker: Arc<Tracker>) {
    let month_re = Regex::new(
        r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
    )
    .unwrap();

    let (task_tx, task_rx) = async_channel::unbounded::<String>();
    let (result_tx, result_rx) = async_channel::unbounded::<Value>();
    let pending = Arc::new(AtomicUsize::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    // 0. 上报本机公网 IP
    tracker.report_ip().await;

    // 1. 提交者
    let submitter_handle = tokio::spawn(submitter(
        Arc::clone(&tracker),
        result_rx.clone(),
        month_re.clone(),
    ));

    // 2. 工人
    let mut worker_handles = Vec::new();
    for _ in 0..tracker.workers {
        worker_handles.push(tokio::spawn(worker(
            Arc::clone(&tracker),
            task_rx.clone(),
            result_tx.clone(),
            Arc::clone(&pending),
        )));
    }
    // 主任务不再持有 worker 用的接收/发送端副本，便于通道正确关闭
    drop(task_rx);
    drop(result_tx);

    // 3. 生产者
    let producer_handle = tokio::spawn(producer(
        Arc::clone(&tracker),
        task_tx.clone(),
        Arc::clone(&pending),
        Arc::clone(&shutdown),
    ));

    // 运行时长：由环境变量 RUN_SECONDS 控制。
    // 0 表示不限时长，一直运行直到进程被外部终止（配合 GitHub Actions 的 job 超时收尾）。
    // 未设置时默认随机 3-5 分钟（保留原行为）。
    let run_seconds: u64 = match std::env::var("RUN_SECONDS").ok().and_then(|v| v.parse::<u64>().ok()) {
        Some(0) => 0,
        Some(n) => n,
        None => rand::thread_rng().gen_range(180..=300),
    };
    log::info!(
        "🚀 系统就绪！共启动 {} 个并发进程，运行时长：{}...",
        tracker.workers,
        if run_seconds == 0 {
            "不限时（持续运行直到被外部终止）".to_string()
        } else {
            format!("约 {} 秒（{:.1} 分钟）后优雅退出", run_seconds, run_seconds as f64 / 60.0)
        }
    );

    if run_seconds == 0 {
        // 不限时模式：永久挂起，依赖外部信号/超时来终止进程
        std::future::pending::<()>().await;
    } else {
        tokio::time::sleep(Duration::from_secs(run_seconds)).await;
    }
    log::info!("⏰ 运行结束，开始优雅退出流程...");

    // ============ 优雅退出 ============
    // 5. 停掉生产者
    shutdown.store(true, Ordering::Relaxed);
    producer_handle.abort();
    let _ = producer_handle.await;

    // 6. 等待在途单号处理完（最多 120 秒）
    log::info!("⏳ 等待剩余 {} 个单号处理完成（在途请求收尾）...", pending.load(Ordering::SeqCst));
    let drain = async {
        while pending.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    match tokio::time::timeout(Duration::from_secs(120), drain).await {
        Ok(_) => log::info!("✅ 队列中所有单号均已处理完成。"),
        Err(_) => log::warn!("⚠️ 等待队列处理超时（120秒），强制进入下一步收尾。"),
    }

    // 7. 关闭任务通道，通知工人退出
    task_tx.close();
    drop(task_tx);
    for h in worker_handles {
        let _ = h.await;
    }

    // 8. 工人全部结束后，result_tx 副本已释放，提交者会读到通道关闭并提交最后一批
    match tokio::time::timeout(Duration::from_secs(30), submitter_handle).await {
        Ok(_) => {}
        Err(_) => log::warn!("⚠️ 等待提交者收尾超时。"),
    }

    log::info!("👋 程序已优雅退出。");
}

// ================= 入口 =================
#[tokio::main]
async fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "{} - {} - {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S,%3f"),
                record.level(),
                record.args()
            )
        })
        .init();

    // 密钥从环境变量读取（GitHub Actions 用 Secrets 注入），缺失则直接报错退出。
    let client_id = match std::env::var("USPS_CLIENT_ID") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            log::error!("❌ 缺少环境变量 USPS_CLIENT_ID，请在 GitHub Secrets 或本地环境中配置后再运行。");
            std::process::exit(1);
        }
    };
    let client_secret = match std::env::var("USPS_CLIENT_SECRET") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            log::error!("❌ 缺少环境变量 USPS_CLIENT_SECRET，请在 GitHub Secrets 或本地环境中配置后再运行。");
            std::process::exit(1);
        }
    };
    let num_type = std::env::var("NUM_TYPE").unwrap_or_else(|_| "big".to_string());
    let workers = std::env::var("WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1);
    let use_proxy = std::env::var("USE_PROXY")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let tracker = Tracker::new(client_id, client_secret, num_type, workers, use_proxy);
    run_system(tracker).await;
}
