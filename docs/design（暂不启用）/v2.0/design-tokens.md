# Design Tokens — QRP Atlas v2.0

---

## 颜色系统

### 基础背景层（深度由深到浅）

```
--atlas-bg-deep:     #07091280   /* 最底层，全页背景 */
--atlas-bg-base:     #0d1117     /* 页面主背景 */
--atlas-bg-surface:  #111827     /* 卡片/面板背景 */
--atlas-bg-elevated: #1a2236     /* 悬浮元素、下拉菜单 */
--atlas-bg-overlay:  #212d42     /* tooltip、modal 背景 */
```

### 边框

```
--atlas-border-subtle:  rgba(255,255,255,0.05)   /* 分割线 */
--atlas-border-default: rgba(255,255,255,0.09)   /* 卡片边框 */
--atlas-border-strong:  rgba(255,255,255,0.16)   /* 强调边框 */
--atlas-border-focus:   #3b82f6                  /* 聚焦环 */
```

### 品牌色（主交互色）

```
--atlas-blue-400: #60a5fa
--atlas-blue-500: #3b82f6   /* 主色，按钮、链接、选中 */
--atlas-blue-600: #2563eb   /* hover 态 */
--atlas-blue-glow: rgba(59,130,246,0.20)
```

### 语义色

```
/* A 股：涨 = 红，跌 = 绿 */
--atlas-bull:        #f43f5e   /* 上涨，比 red-500 更鲜艳 */
--atlas-bull-dim:    rgba(244,63,94,0.12)
--atlas-bear:        #10b981   /* 下跌，emerald-500 */
--atlas-bear-dim:    rgba(16,185,129,0.12)
--atlas-limit-up:    #e11d48   /* 涨停，更深红 */
--atlas-limit-down:  #059669   /* 跌停，更深绿 */

/* 中性 */
--atlas-neutral:     #94a3b8   /* 平盘/0% */
--atlas-warning:     #f59e0b   /* 警示，amber-500 */
--atlas-info:        #06b6d4   /* 信息提示，cyan-500 */
```

### 文字色

```
--atlas-text-primary:   #f1f5f9   /* 主文字，slate-100 */
--atlas-text-secondary: #94a3b8   /* 次要文字，slate-400 */
--atlas-text-muted:     #475569   /* 辅助文字，slate-600 */
--atlas-text-disabled:  #2d3f55   /* 禁用文字 */
```

---

## 字体系统

### 字体栈

```css
/* 界面文字（已在项目中） */
font-family: 'Geist Variable', 'Inter', system-ui, sans-serif;

/* 数字/代码（新增，可从 Google Fonts 加载） */
font-family: 'Geist Mono', 'JetBrains Mono', 'Fira Code', monospace;
```

### 字号层级

| 用途 | 大小 | 粗细 | 示例 |
|------|------|------|------|
| 大 KPI 数字 | 28–32px | 700 | `2,187` |
| 卡片标题 | 13px | 500 | `上涨` |
| 表格数字 | 13px | 400 mono | `+5.23%` |
| 表格文字 | 13px | 400 | `贵州茅台` |
| 表头 | 11px | 500 | `涨跌幅` |
| 辅助说明 | 11px | 400 | `占比 40.9%` |
| 导航标签 | 13px | 500 | `今日概览` |

---

## 间距系统

采用 4px 基准网格（Tailwind 默认）。

| 用途 | 值 |
|------|----|
| 卡片内边距 | 16px (p-4) |
| 卡片间距 | 12px (gap-3) |
| 行高（表格） | 40px |
| 分组间距 | 24px (gap-6) |
| 页面水平留白 | 24px (px-6) |
| 侧边栏宽 | 220px |
| 顶栏高 | 52px |

---

## 圆角系统

```
卡片/面板:  rounded-xl  (12px)
按钮:       rounded-lg   (8px)
标签/chip:  rounded-full
输入框:     rounded-lg   (8px)
tooltip:    rounded-md   (6px)
```

---

## 阴影系统

深色主题下少用阴影，以背景色差异替代。仅在以下场景用：

```css
/* 下拉菜单、弹出面板 */
box-shadow: 0 8px 32px rgba(0,0,0,0.48), 0 1px 0 rgba(255,255,255,0.06) inset;

/* 卡片 hover 态 */
box-shadow: 0 0 0 1px rgba(59,130,246,0.30), 0 4px 16px rgba(0,0,0,0.24);
```

---

## 数据可视化配色

图表序列色（避开红绿，防止与涨跌语义混淆）：

```
系列 1: #3b82f6   (蓝)
系列 2: #f59e0b   (琥珀)
系列 3: #8b5cf6   (紫)
系列 4: #06b6d4   (青)
系列 5: #ec4899   (粉)
```

---

## 与现有代码对应

在 `web/src/index.css` 的 `:root` / `.dark` 块内更新以下 CSS 变量即可全局生效：

```css
.dark {
  --background:   7 9 18;    /* #070912 → oklch 近似 */
  --card:         17 24 39;  /* #111827 */
  --primary:      59 130 246; /* #3b82f6 */
  /* ... 其余按上表映射 */
}
```
