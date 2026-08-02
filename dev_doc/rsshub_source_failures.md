# RSSHub 来源失败排查记录

## 记录信息

- 记录时间：2026-08-01
- 项目：`ashares_data_collect`
- RSSHub：本地 Docker 容器 `ashares-rsshub`
- RSSHub 地址：`http://127.0.0.1:11200`
- Redis：`ashares-rsshub-redis`
- OpenSearch：`news-2026`
- 项目配置：`config.yaml` 中 `news.rsshub_base: http://127.0.0.1:11200`
- 本次状态：3 个来源失败，但未阻断其他来源；新闻管线采用源级隔离继续执行。

## 1. 东方财富个股研报

### 来源

- source id：`em_report_stock`
- RSSHub 路由：`/eastmoney/report/stock`
- 目标数据：东方财富个股研报/股票研究报告
- 项目注册位置：`sources.yaml`
- 归属任务：`news_regulator`
- 归属频道：`report`

### 当前结果

- 本地访问：HTTP 503
- 同组已成功的路由：
  - `/eastmoney/report/strategyreport`
  - `/eastmoney/report/industry`
  - `/eastmoney/report/macresearch`

### RSSHub 日志

```text
Error in /eastmoney/report/stock: SyntaxError: Unterminated string in JSON at position 59795 (line 1 column 59796)
```

### 初步判断

RSSHub 从东方财富获取的响应不是完整 JSON，可能原因：

1. 东方财富接口响应被截断；
2. 返回内容包含非预期字符或错误页；
3. 东方财富接口字段/返回格式发生变化；
4. 请求频率或 IP 风控导致返回异常内容。

目前没有证据表明需要用户登录。

### 复现命令

```powershell
Invoke-WebRequest -UseBasicParsing `
  -Uri "http://127.0.0.1:11200/eastmoney/report/stock"
```

### 后续排查

- 查看 RSSHub 完整日志和异常堆栈；
- 抓取 RSSHub 实际访问的东方财富 URL；
- 保存目标接口原始响应长度、Content-Type 和前 500/后 500 字节；
- 对比 strategy/industry/macro 三个正常路由的请求和返回格式；
- 检查是否需要降低请求频率或增加重试退避；
- 确认 RSSHub 上游路由是否已更新。

## 2. 界面新闻

### 来源

- source id：`jiemian`
- RSSHub 路由：`/jiemian/lists/1`
- 目标数据：界面新闻栏目/列表新闻
- 项目注册位置：`sources.yaml`
- 归属任务：`news_regulator`
- 归属频道：`media`

### 当前结果

- 本地访问：HTTP 503
- RSSHub 实际目标：`https://www.jiemian.com/lists/1.html`

### RSSHub 日志

```text
Request https://www.jiemian.com/lists/1.html with error 403 remaining retry attempts: 2
Error in /jiemian/lists/1: FetchError: [GET] "https://www.jiemian.com/lists/1.html": 403 Forbidden
```

### 初步判断

目标站点拒绝了 RSSHub 请求，可能是：

1. 网站反爬或 IP 风控；
2. User-Agent、Referer 或请求头不符合要求；
3. 页面需要浏览器执行 JavaScript；
4. 目标站点需要验证码或登录态。

目前没有证据表明仅配置用户账号即可解决。

### 复现命令

```powershell
Invoke-WebRequest -UseBasicParsing `
  -Uri "http://127.0.0.1:11200/jiemian/lists/1"
```

### 后续排查

- 在 RSSHub 容器中测试目标站点的响应状态和响应头；
- 对比普通 requests、curl_cffi、Chromium 浏览器请求结果；
- 检查 RSSHub 该路由是否支持 Chromium 渲染；
- 检查是否存在新的界面新闻栏目 URL；
- 若长期 403，寻找界面新闻官方 RSS 或替代财经媒体源。

## 3. 晚点财经

### 来源

- source id：`latepost`
- RSSHub 路由：`/latepost`
- 目标数据：晚点财经栏目/新闻
- 项目注册位置：`sources.yaml`
- 归属任务：`news_regulator`
- 归属频道：`media`

### 当前结果

- 本地访问：HTTP 503
- RSSHub 实际目标接口：`https://www.latepost.com/site/get-column`

### RSSHub 日志

```text
Request https://www.latepost.com/site/get-column fail: Error: unable to get local issuer certificate
TypeError: fetch failed
Error in /latepost: FetchError: [GET] "https://www.latepost.com/site/get-column": <no response> fetch failed (unable to get local issuer certificate)
```

### 初步判断

RSSHub 容器内 Node.js 无法验证晚点财经网站的 HTTPS 证书链，可能原因：

1. 目标站点证书链不完整或配置异常；
2. 容器内 CA 证书缺失或过期；
3. 网络代理对 TLS 连接进行了中间转发；
4. RSSHub/Node 的 TLS 校验与目标站点不兼容。

这不是典型的用户登录问题。

### 复现命令

```powershell
Invoke-WebRequest -UseBasicParsing `
  -Uri "http://127.0.0.1:11200/latepost"
```

### 后续排查

- 在 RSSHub 容器内执行 `curl -Iv https://www.latepost.com/site/get-column`；
- 检查目标站点证书链；
- 检查容器系统 CA 证书；
- 检查 Docker/Windows 网络代理是否重签 HTTPS；
- 不建议直接全局关闭 TLS 证书验证；
- 如无法修复，寻找晚点财经官方 RSS 或替代源。

## 统一排查命令

查看 RSSHub 最近日志：

```powershell
docker logs --tail 300 ashares-rsshub
```

筛选三个来源：

```powershell
docker logs --tail 500 ashares-rsshub 2>&1 |
  Select-String -Pattern "report/stock|jiemian|latepost|503|403|certificate|JSON"
```

查看容器状态：

```powershell
docker compose ps
```

重新运行新闻管线：

```powershell
.\.venv\Scripts\python.exe run_news.py --mode pipeline --pipeline news_hourly
```

## 当前处理原则

- 三个来源暂不删除，保留在 `sources.yaml` 以便后续修复；
- 单个来源失败不阻断其他来源；
- 不把 503 解释为需要用户登录；
- 不直接关闭 HTTPS 证书验证；
- 修复前先保存原始响应、请求 URL、状态码、响应头和容器日志；
- 修复后需要单独验证路由，再运行完整 `news_hourly` 管线。
