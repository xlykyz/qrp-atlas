# 东方财富「问董秘」数据采集调研报告

## 一、 核心结论：直接返回还是截断数据？

通过对东方财富股吧「问董秘」列表页（`https://guba.eastmoney.com/qa/list?type=1`）和详情页的深入抓包与源码分析，结论如下：

**列表页返回的数据是“截断数据”。**
* **提问内容截断**：列表中显示的投资者提问如果字数较多，会被缩写并加上 `......` 后缀。
* **回答内容截断**：董秘的回复内容在列表页同样只展示前两行左右，字数较多时会被强行截断，并附带一个 `查看全部>>` 的超链接。
* **缺失核心字段**：列表页上**不包含**投资者的提问时间（仅包含董秘的回复时间），也无法获取提问者的具体个人主页链接及地区标签等。

**因此，如果需要抓取“完整数据”（包括提问全文、回答全文、精确提问时间、提问者UID等），必须采用“列表页爬取索引 + 详情页爬取全文”的双级爬取策略。**

---

## 二、 完整抓取方案与技术实现路径

为了获取不截断的完整问答数据，推荐采用以下高效率、低风险的自动化采集架构：

```
[步骤1：列表页增量扫描] (高频)
       │
       ▼ (提取 帖子详情页ID 与 股票代码)
[步骤2：详情页静态抓取] (低频/分布式)
       │
       ▼ (提取 提问全文 + 回答全文 + 双向时间戳)
[步骤3：结构化持久化] (存储为 MongoDB / CSV)
```

### 1. 列表页分页与增量扫描
列表页使用“加载更多”的 AJAX 异步接口。
* **接口地址**：`https://guba.eastmoney.com/interface/GetData.aspx`
* **请求方式**：`GET`
* **核心请求参数**：
  * `path`: `qa/list`
  * `type`: `1` （代表“最新答复”频道）
  * `page`: `[页码]` （例如：`2`, `3`, `4`...）
* **返回值**：返回的是一段包含新条目的 HTML 片段。可以通过轻量级的 HTML 解析库（如 BeautifulSoup）解析出该页包含的所有详情页链接。

### 2. 详情页静态解析（不截断的关键）
从列表页提取到的详情链接格式为：`https://guba.eastmoney.com/news,{股票代码},{帖子ID}.html`（例如 `https://guba.eastmoney.com/news,600630,1742684064.html`）。

* **页面属性**：该页面为**纯静态 HTML 服务端渲染**。
* **采集优势**：不需要执行任何复杂的 JavaScript 逻辑，直接使用 `requests` 或 `aiohttp` 发送 HTTP GET 请求，即可瞬间返回完整 HTML 源码。
* **解析定位（XPath/CSS选择器）**：
  * **完整提问内容**：`//div[@id="zwconbody"]//div[contains(@class, "question")]` 或提取特定问答容器中的首段。
  * **完整回答内容**：紧随回复人之后的文本区块。
  * **提问时间**：可以在问题发布者昵称旁边提取到，格式为 `YYYY-MM-DD HH:MM:SS`。
  * **回复时间**：可以在回复区块底部或通过页面 `json-ld` / 元数据中提取。

---

## 三、 推荐的 Python 异步采集代码模版

以下是一个基于 `asyncio` 和 `aiohttp` 实现的、兼顾效率和反爬安全性的完整抓取脚本模版：

```python
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import random

# 请求头配置
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://guba.eastmoney.com/qa/list?type=1"
}

async def fetch_detail(session, url):
    """
    爬取详情页获取不截断的完整数据
    """
    try:
        # 随机延迟，降低被封IP的风险
        await asyncio.sleep(random.uniform(1.0, 2.5))
        async with session.get(url, headers=HEADERS, timeout=10) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # 示例解析逻辑（具体选择器根据页面结构微调）
                question_div = soup.find(class_="qa_question_detail") or soup.find(id="zwconbody")
                reply_div = soup.find(class_="qa_reply_detail")
                
                question_text = question_div.get_text(strip=True) if question_div else "未提取到提问"
                reply_text = reply_div.get_text(strip=True) if reply_div else "未提取到回复"
                
                return {
                    "url": url,
                    "full_question": question_text,
                    "full_reply": reply_text
                }
    except Exception as e:
        print(f"Error fetching detail {url}: {e}")
    return None

async def get_list_page(session, page):
    """
    获取列表页并提取所有详情页URL
    """
    list_url = f"https://guba.eastmoney.com/interface/GetData.aspx?path=qa/list&type=1&page={page}"
    try:
        async with session.get(list_url, headers=HEADERS, timeout=10) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # 寻找所有详情页链接
                links = soup.find_all('a', href=re.compile(r'/news,\d+,\d+\.html'))
                detail_urls = list(set([f"https://guba.eastmoney.com{link['href']}" for link in links]))
                return detail_urls
    except Exception as e:
        print(f"Error fetching list page {page}: {e}")
    return []

async def main():
    async with aiohttp.ClientSession() as session:
        # 1. 扫描前5页列表
        all_detail_tasks = []
        for page in range(1, 6):
            print(f"Scanning list page {page}...")
            detail_urls = await get_list_page(session, page)
            
            # 2. 为获取到的每一个详情链接创建协程任务
            for url in detail_urls:
                all_detail_tasks.append(fetch_detail(session, url))
                
        # 3. 并发安全抓取详情页全文
        print(f"Starting to crawl {len(all_detail_tasks)} full Q&A details...")
        results = await asyncio.gather(*all_detail_tasks)
        
        # 4. 过滤空值并保存
        valid_results = [r for r in results if r]
        print(f"Successfully collected {len(valid_results)} complete records!")
        # 写入本地或数据库...

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 四、 工业级数据采集避坑指南

1. **强 Referer 校验**：
   东方财富的接口（尤其是 `GetData.aspx`）会校验请求头的 `Referer`。发送列表页及详情页请求时，请务必保证 Request Headers 中带有 `"Referer": "https://guba.eastmoney.com/qa/list?type=1"`。
2. **IP 频率控制与代理池**：
   董秘问答的实时更新频率并不算极高（全市场每日大约几百条到一千条）。建议将爬虫设定为**定时任务**（如每 15 分钟运行一次，每次只抓取前 2 页的最新增量数据）。这样可以避免持续高频请求触发 WAF 防火墙，无需昂贵的代理池即可稳定运行。
3. **文本清洗注意点**：
   由于详情页中董秘的回答往往包含许多免责声明和前缀（如：“尊敬的投资者您好...”、“感谢您的关注...”），在做情感分析或舆情挖掘时，建议使用正则或大模型对前缀进行剥离，仅保留正文核心部分。
