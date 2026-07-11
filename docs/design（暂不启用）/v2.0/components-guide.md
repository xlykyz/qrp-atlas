# Components Guide — QRP Atlas v2.0

---

## 全局布局

### 侧边栏

```
宽度:   220px（展开）/ 56px（折叠，可选后期加）
背景:   #0d1117，左侧 1px 实线分隔右侧内容区
```

**Logo 区：**
- 高度 52px（与 topbar 对齐）
- Logo + "QRP Atlas" 文字，蓝色点缀
- 下方细线分隔

**导航项：**
```
正常态:  text-slate-400, hover → bg-white/5 text-slate-100
选中态:  bg-blue-500/10 text-blue-400, 左侧 2px 蓝色竖条
图标:   16px lucide icon，与文字间距 10px
```

**导航分组：** 用 11px 全大写灰色标签分组（如 "MARKET" / "TOOLS"）

### 顶栏（Top Bar）

```
高度:  52px
背景:  #0d1117，底部 1px 分隔线
```

左侧：页面标题（16px, 600, text-slate-100）
右侧：页面级注入控件（日期选择器等）+ 主题切换按钮

---

## Stat Card（KPI 卡片）

首页 5 个指标卡片，改造方向：

```
背景:    #111827（bg-surface）
边框:    1px rgba(255,255,255,0.09)
圆角:    12px
内边距:  16px
左侧:    4px 宽彩色竖条（颜色随语义：蓝/红/绿/深红/深绿）
```

**卡片内部布局（上下两行）：**

```
[图标 16px]  [标签 13px slate-400]
[大数字 28px bold mono]  [副指标 13px 带色]
```

副指标示例：`40.9%` 显示为红色（涨的占比）

**hover 态：** 边框色变为对应语义色，`box-shadow: 0 0 0 1px ...`

---

## Board Filter（板块筛选 Chip）

```
正常态: bg-white/5, border border-white/10, text-slate-400, rounded-full px-3 py-1
选中态: bg-blue-500/15, border-blue-500/40, text-blue-300
hover:  bg-white/10
```

文字 12px，图标可选。当全部选中时"全部"高亮，其他取消。

---

## 数据表格

### 表头

```
背景:   #0d1117（比卡片更深）
文字:   11px, 500, text-slate-500, uppercase, letter-spacing: 0.05em
对齐:   数字列右对齐，文字列左对齐
内边距: px-4 py-2
border-bottom: 1px rgba(255,255,255,0.08)
```

### 数据行

```
高度:       40px
正常背景:   transparent
hover 背景: rgba(255,255,255,0.03)
active:     cursor-pointer（可点击行）
border-bottom: 1px rgba(255,255,255,0.04)
```

### 关键列设计

**代码列：** `font-mono text-blue-400` + 点击下划线

**名称列：** `text-slate-100 font-medium`，ST 标记用琥珀色小标签

**涨跌幅列（核心）：**
- 正值：`text-[#f43f5e] font-mono font-medium`，前缀 `+`
- 负值：`text-[#10b981] font-mono font-medium`
- 零值：`text-slate-500`
- 数值右对齐，宽度固定（`w-20`）

**涨停/跌停标记：**
- 涨停：`bg-red-500/10 text-red-400 rounded px-1.5 text-[11px]` + "🚀" 或 "涨停"
- 跌停：`bg-emerald-500/10 text-emerald-400`

**成交额：** `font-mono text-slate-300`，单位换行显示 `亿`（小字灰色）

### 分页

```
背景:   与表头同色
文字:   13px, text-slate-400
按钮:   选中页码用 bg-blue-500/20 text-blue-300 rounded
```

---

## 市场情绪条（新增组件）

位于 KPI 卡片下方，是首页新增的核心视觉元素。

```
容器: w-full h-8 bg-white/5 rounded-full overflow-hidden flex
上涨段: bg-gradient-to-r from-red-500/60 to-red-400/40, 宽度 = 上涨占比%
下跌段: bg-gradient-to-r from-emerald-400/40 to-emerald-500/60, 宽度 = 下跌占比%
平盘段: bg-white/10（剩余宽度）

左标签: "涨 40.9%" text-red-400 text-xs
右标签: "跌 53.1%" text-emerald-400 text-xs
中标签: 涨停数 / 跌停数，小标签样式
```

---

## 空状态 / 加载态

**骨架屏（loading）：**
- 卡片内 `animate-pulse bg-white/8 rounded`
- 表格行替换为 2–3 个 skeleton 行

**空数据：**
- 居中图标（`FileX` lucide）+ 灰色说明文字
- 如果是非交易日：显示"今日非交易日，显示最近 YYYY-MM-DD 数据"

**错误态：**
- 橙色左边框卡片（`border-l-2 border-amber-500`）
- 错误文字 + "重试" 按钮（blue）

---

## 可复用工具

### 数值格式化规范

| 数据类型 | 格式 | 示例 |
|---------|------|------|
| 涨跌幅 | `+5.23%` / `-2.10%` | 正前加 `+` |
| 成交额（亿） | `12.45亿` | 除以 1e8，保留 2 位 |
| 成交额（大） | `1,234.56亿` | 千位符 |
| 换手率 | `3.21%` | 无正负号 |
| 股票代码 | `000001` | 始终 6 位，蓝色等宽 |
| 价格 | `1,723.50` | 千位符，2 位小数，等宽 |
