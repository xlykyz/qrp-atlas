# Design Tokens — QRP Atlas

## 色彩系统

### 品牌主色 — Atlas Sapphire

深蓝宝石色调，传达专业、深度与可信赖感。灵感来自高端金融终端与航海仪表。

```
亮色模式:
  primary-50:  #eff6ff   (最浅 — 背景/选中态)
  primary-100: #dbeafe
  primary-200: #bfdbfe
  primary-300: #93c5fd
  primary-400: #60a5fa
  primary-500: #3b82f6   (主色 — 按钮/链接/强调)
  primary-600: #2563eb   (hover 态)
  primary-700: #1d4ed8
  primary-800: #1e40af
  primary-900: #1e3a8a   (最深 — sidebar/header 底色)
  primary-950: #172554

暗色模式:
  primary-50:  #172554
  primary-100: #1e3a8a
  primary-200: #1e40af
  primary-300: #1d4ed8
  primary-400: #2563eb
  primary-500: #3b82f6
  primary-600: #60a5fa
  primary-700: #93c5fd
  primary-800: #bfdbfe
  primary-900: #dbeafe
  primary-950: #eff6ff
```

### 强调色 — Atlas Amber

暖金色调，用于数据高亮、通知、CTA 等需要吸引注意力的场景。与冷蓝色主色形成温度对比。

```
amber-400: #fbbf24  (图表高亮线)
amber-500: #f59e0b  (强调按钮/徽章)
amber-600: #d97706
```

### 语义色（A 股惯例：红涨绿跌）

```
up (涨):   #dc2626 (red-600) — 涨幅、阳线、盈利
down (跌): #16a34a (green-600) — 跌幅、阴线、亏损

up-bg:     #fef2f2 (red-50)   — 涨幅背景
down-bg:   #f0fdf4 (green-50) — 跌幅背景

dark mode:
up:        #f87171 (red-400)
down:      #4ade80 (green-400)
up-bg:     rgba(220,38,38,0.12)
down-bg:   rgba(22,163,74,0.12)
```

### 中性色（Refined Slate — 比原版更暖）

```
亮色:
  bg:           #f8fafc  (slate-50, 页面底色 — 原版纯白太白)
  surface:      #ffffff  (卡片底色)
  surface-alt:  #f1f5f9  (slate-100, 表格斑马纹)
  border:       #e2e8f0  (slate-200)
  text-primary: #0f172a  (slate-900)
  text-secondary:#475569 (slate-600)
  text-muted:   #94a3b8  (slate-400)

暗色:
  bg:           #0b1121  (自定义深蓝黑 — 不是纯黑)
  surface:      #111827  (gray-900)
  surface-alt:  #1a2332  (自定义)
  border:       #1e293b  (slate-800)
  text-primary: #f1f5f9  (slate-100)
  text-secondary:#94a3b8 (slate-400)
  text-muted:   #64748b  (slate-500)
```

### Sidebar 专用色

```
亮色:
  sidebar-bg:        #0f1d3a  (primary-950 方向但更深)
  sidebar-text:      #cbd5e1  (slate-300)
  sidebar-active-bg: rgba(59,130,246,0.2)
  sidebar-active-text:#ffffff
  sidebar-hover-bg:  rgba(255,255,255,0.06)

暗色:
  sidebar-bg:        #070d1a  (几乎黑但带蓝)
  sidebar-text:      #94a3b8
  sidebar-active-bg: rgba(59,130,246,0.15)
  sidebar-active-text:#bfdbfe  (primary-200)
  sidebar-hover-bg:  rgba(255,255,255,0.04)
```

---

## 字体系统

### 字体族

```
--font-sans:     'Geist Variable', system-ui, -apple-system, sans-serif
--font-mono:     'Geist Mono', 'JetBrains Mono', 'Cascadia Code', monospace
--font-display:  'Geist Variable', sans-serif  (与 sans 相同，语义区分)
```

Geist（由 Vercel 设计）是现代几何无衬线体，字形干净利落，非常适合数据密集型界面。

### 字体大小阶梯

```
text-2xs:   0.625rem (10px) — 表格密集数据、微标签
text-xs:    0.75rem  (12px) — 辅助信息、图例
text-sm:    0.875rem (14px) — 正文、表格内容
text-base:  1rem     (16px) — 卡片标题、导航
text-lg:    1.125rem (18px) — 面板标题
text-xl:    1.25rem  (20px) — 页面标题
text-2xl:   1.5rem   (24px) — KPI 大数字
text-3xl:   1.875rem (30px) — Hero 数字
text-4xl:   2.25rem  (36px) — 极少数场景
```

### 字重

```
--font-normal:  400  — 正文
--font-medium:  500  — 强调正文、标签、按钮
--font-semibold:600  — 标题、导航
--font-bold:    700  — KPI 数字
```

### 行高

```
--leading-tight:  1.25  — 标题
--leading-normal: 1.5   — 正文
--leading-relaxed:1.625 — 长文本
```

---

## 间距系统（8px 基准）

```css
--space-0:  0
--space-1:  0.25rem (4px)   — 紧凑内边距
--space-2:  0.5rem  (8px)   — 图标与文字间距、标签内边距
--space-3:  0.75rem (12px)  — 表格单元格内边距
--space-4:  1rem    (16px)  — 卡片内边距
--space-5:  1.25rem (20px)  — 大卡片内边距
--space-6:  1.5rem  (24px)  — 区块间距
--space-8:  2rem    (32px)  — 大区块间距
--space-10: 2.5rem  (40px)  — 页面级间距
--space-12: 3rem    (48px)
--space-16: 4rem    (64px)
```

页面内容区 padding 从统一的 `p-6`(24px) 改为 `px-8 py-6`(32px/24px) — 增加水平呼吸感。

---

## 阴影系统

```css
--shadow-xs:   0 1px 2px 0 rgb(0 0 0 / 0.03)
--shadow-sm:   0 1px 3px 0 rgb(0 0 0 / 0.06), 0 1px 2px -1px rgb(0 0 0 / 0.06)
--shadow-md:   0 4px 6px -1px rgb(0 0 0 / 0.07), 0 2px 4px -2px rgb(0 0 0 / 0.05)
--shadow-lg:   0 10px 15px -3px rgb(0 0 0 / 0.08), 0 4px 6px -4px rgb(0 0 0 / 0.04)
--shadow-xl:   0 20px 25px -5px rgb(0 0 0 / 0.08), 0 8px 10px -6px rgb(0 0 0 / 0.04)
```

KPI 卡片使用 `shadow-sm`（hover 时升级为 `shadow-md`），数据表格区不使用阴影。

暗色模式下的阴影使用 `rgb(0 0 0 / 0.3)` 底色，更重但柔和。

---

## 圆角系统

```css
--radius-xs:   0.25rem (4px)   — inline code、小标签
--radius-sm:   0.375rem (6px)  — 小按钮、input
--radius-md:   0.5rem   (8px)  — 按钮、卡片（默认）
--radius-lg:   0.75rem  (12px) — 大卡片、模态框
--radius-xl:   1rem     (16px) — 极少用
```

---

## 过渡与动画

```css
--transition-fast:   120ms cubic-bezier(0.4, 0, 0.2, 1)  — hover 颜色变化
--transition-normal: 200ms cubic-bezier(0.4, 0, 0.2, 1)  — 展开/折叠
--transition-slow:   350ms cubic-bezier(0.4, 0, 0.2, 1)  — 页面切换、模态框
```

所有交互元素必须有 `transition`，数值在 120-200ms。避免生硬的 instant 变化。

---

## 图表配色（lightweight-charts）

```
K 线:
  阳线(涨): #dc2626
  阴线(跌): #16a34a

均线:
  MA5:    #f59e0b (amber)
  MA10:   #3b82f6 (blue)
  MA20:   #8b5cf6 (violet)
  MA60:   #06b6d4 (cyan)

成交量:
  红柱: rgba(220,38,38,0.5)
  绿柱: rgba(22,163,74,0.5)

回测净值曲线:
  策略净值: #3b82f6 (blue, 2px)
  基准:     #94a3b8 (slate, 1px dashed)
  回撤填充: rgba(239,68,68,0.12)
```

---

## 图标系统

统一使用 `lucide-react`（项目已安装），禁用所有 emoji 图标。映射关系：

| 当前 (emoji) | 替换为 (lucide) |
|-------------|----------------|
| ☀️ 亮色模式 | `Sun` |
| 🌙 暗色模式 | `Moon` |
| 🛠️ 数据库预览 | `Database` |
| 🟢🟡🔴⚪ 阶段 | `TrendingUp/Down/Waves/Circle` |
| 页内 emoji 标题 | 移除或用 lucide |

导航项增加图标：

| 导航 | 图标 |
|------|------|
| 今日概览 | `LayoutDashboard` |
| 个股复盘 | `Search` (或 `LineChart`) |
| 复盘日志 | `ClipboardList` |
| 数据库预览 | `Database` |
| 回测分析 | `BarChart3` |
