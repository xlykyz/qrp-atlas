# Tushare 接口调用规范
# 来源：卖家提供，2026-05-13
# 更新：2026-05-13

## 初始化方式（必须严格按此顺序）

```python
import tushare as ts

pro = ts.pro_api('你的token')
pro._DataApi__http_url = "http://124.220.22.110:8020/"
```

**注意：** `_DataApi__http_url` 必须在创建 pro 对象后**立即设置**，否则请求会走 tushare 官方接口（无数据返回）。

## 项目中的标准用法

项目中已有封装，直接调用即可：

```python
from qrp_atlas.config import get_tushare_pro

pro = get_tushare_pro()
```

底层代码（`config/tushare_client.py`）已严格遵循上述初始化顺序。

## 日常数据获取

```python
# 按交易日获取全市场日线
df = pro.daily(trade_date="20260513")

# 按股票代码获取历史日线
df = pro.daily(ts_code="000001.SZ", start_date="19900101", end_date="20121231")

# 获取复权因子
df = pro.adj_factor(ts_code="000001.SZ")
```

## 验证连接

```bash
cd ~/projects/qrp-atlas
python -c "
from qrp_atlas.config import get_tushare_pro
pro = get_tushare_pro()
df = pro.index_basic(limit=5)
print('连接成功，返回', len(df), '行')
print(df)
"
```

## 已知问题

如果提示 "Token 不对"，检查代码是否少了这行：
```python
pro._DataApi__http_url = "http://124.220.22.110:8020/"
```

## Token 信息

- Token 存在项目根目录 `.env` 文件中
- 格式：`TUSHARE_TOKEN=xxx`
- 代码通过 `dotenv` 自动加载，`get_tushare_pro()` 自动读取
