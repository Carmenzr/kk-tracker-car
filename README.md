# USPS Tracker (binary release)

仅包含预编译的 Linux 二进制和 GitHub Actions 工作流。源码不在本仓库。

二进制 `bin/usps-tracker-rs` 为 `x86_64-unknown-linux-musl` 静态链接，无外部依赖。
每次运行随机 3-5 分钟后自动优雅退出，由 Actions「链式接力」不断换新机器（新 IP）持续运行。

## 配置步骤

1. **Secrets**（Settings → Secrets and variables → Actions → New repository secret）：
   - `USPS_CLIENT_ID` — USPS OAuth Client ID
   - `USPS_CLIENT_SECRET` — USPS OAuth Client Secret
   - `PAT` — 个人访问令牌（用于自触发下一轮，见下）

2. **创建 PAT**（链式接力必需）：
   - 头像 → Settings → Developer settings → Personal access tokens
   - Fine-grained token：选中本仓库，赋予 **Actions: Read and write** 权限
   - （或经典 token，勾选 `workflow` scope）
   - 生成后把值存为上面的 `PAT` secret

3. **启用 Actions**：仓库 Actions 标签页 → 启用 workflow → 手动 Run workflow 一次启动接力链。

## 说明

- `schedule: 0 * * * *` 为兜底，万一接力断了每小时自动重新接上。
- 公开仓库 Actions 时长无限。
