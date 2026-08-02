# Tushare 数据源验证

这些样例用于验证 Tushare 兼容服务是否满足本项目的基础数据需求。

## 安全

样例不会保存真实 Token。运行前仅在当前 PowerShell 会话设置环境变量：

```powershell
$env:TUSHARE_API_KEY = "你的 API Key"
.\.venv\Scripts\python.exe dev_doc\tushare_validation\validate_basic.py
```

样例按指南使用 `https://teajoin.com` 作为 SDK 请求地址，Token 不会打印、写文件或提交到 Git。

## 验证内容

- `stock_basic`：股票基础信息
- `trade_cal`：交易日历
- `daily`：日线行情
- `daily_basic`：PE/PB/市值/换手率等每日指标
- `income`：利润表
- `balancesheet`：资产负债表
- `cashflow`：现金流量表
- `fina_indicator`：ROE、负债率等财务指标

验证结果只保存字段名、行数和错误摘要，不保存完整响应和 Token。
