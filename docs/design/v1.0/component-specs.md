# Component Specs — QRP Atlas

每个组件的改造说明，按优先级排列。改动范围限于 `index.css` 和组件文件，不动架构。

---

## 1. Layout / Shell（最高优先级）

### Sidebar

```diff
- bg-slate-900 dark:bg-slate-950 → bg-sidebar (navy 黑，不是 slate 黑)
- 256px 宽 → 248px 宽
- 纯色背景 → 顶部到下部微妙渐变 (bg-sidebar → bg-sidebar-end)
- Brand 区域：纯文字 → Logo 图标 + 文字
```

**Brand 区域改进：**
- 加 `Dot` 图标（lucide）作为 logo mark
- 文字 "QRP Atlas" 加 letter-spacing
- 下方加一条细微分割线，与导航区分开

**导航项改进：**
- 每项加 lucide 图标
- `isActive` 时左侧出现 3px 宽的 primary 色竖线指示器
- hover 时轻微向右平移 2px（`translate-x-0.5`）
- 导航项之间间距从 `space-y-1` 改为 `space-y-0.5`

**底部区域新增：**
- 主题切换移到 sidebar 底部（不占用顶栏空间）
- 底部显示版本号/构建信息（如 `v1.0.0`）

### Top Bar

```diff
- 占用整行高度 + border-bottom → 更轻量的顶栏
- h1 标题 → 左边加面包屑或页面图标
- 主题切换移出 → 顶栏更纯粹，只留标题 + 页面控件
- border → 更淡的分割，暗色模式几乎不可见
```

### Content Area

```diff
- p-6 (24px everywhere) → px-8 py-6 (32px 水平，24px 垂直)
- bg-white → bg-slate-50 (浅灰底，让白色卡片浮起来)
```

---

## 2. KPI Cards（今日概览 / 回测分析）

当前问题：KPI 卡片和普通卡片视觉上没有区别。

**改进方案：**

```
┌─────────────────────────┐
│  📊 股票总数             │  ← icon + label (text-muted, text-xs, uppercase tracking)
│                         │
│  5,284                  │  ← value (text-3xl, font-bold)
│                         │
│  ▴ +12 vs 昨日  245 ↑   │  ← trend line mini sparkline + delta
└─────────────────────────┘
```

- 卡片背景：`bg-surface`，border 更淡 (`border-slate-100`)
- 数值字号增大到 `text-3xl`（原来可能是 `text-2xl` 或更小）
- 增加 mini trend 指示（简单的箭头 + 数值）
- hover 时卡片轻微上浮 (`translate-y-px`)，阴影升级

**色彩编码：**
- 总览类指标（总数、覆盖面）：使用 primary 色图标
- 上涨指标：red 背景微 tint
- 下跌指标：green 背景微 tint
- 涨停/跌停：amber 背景微 tint

---

## 3. Data Table（今日概览）

当前问题：标准 shadcn Table，没有金融数据表的"质感"。

**改进方案：**

```
┌──────────────────────────────────────────────────────────┐
│ 表头行: bg-slate-100 text-slate-600 uppercase text-2xs   │
│         带排序箭头，sticky top                            │
├──────────────────────────────────────────────────────────┤
│ 数据行: 斑马纹 (bg-white / bg-slate-50)                  │
│         hover: bg-primary-50/5                           │
│                                                          │
│ 涨跌幅列: 红色/绿色文字 + 半透明背景色条                    │
│   ████████░░ +8.32%                                     │
│                                                          │
│ 代码列: font-mono, text-primary-500, cursor-pointer       │
└──────────────────────────────────────────────────────────┘
```

**关键改动：**
- 表头用 `text-2xs uppercase tracking-wider` 增强专业感
- 涨跌幅列不只用文字颜色，加一个半透明背景条（width = abs(pct) / maxPct）
- 代码列用等宽字体，主色链接色
- 行高压缩到 `py-2`（节省垂直空间，显示更多数据）
- 斑马纹交替，hover 行用 primary-50 浅色

---

## 4. Cards / Panels

当前问题：所有内容装在 Card 里，但 Card 太"素"。

**改进方案：**

```diff
- 纯白卡片 + 标准 border → 带微妙阴影 + 更淡 border
- 无卡片标题区设计 → 卡片 header 区（标题 + 操作按钮）
```

**卡片层级：**
1. **数据卡片**（KPI）：`shadow-sm`，hover 上浮
2. **内容面板**（表格容器、图表容器）：`shadow-xs`，更低调
3. **表单卡片**（个股操作面板）：`shadow-sm`，带 primary 色顶边

---

## 5. Button 体系

当前有 shadcn Button variant 系统，但缺乏语义化使用。

**补充变体：**

```css
/* ghost icon button — 工具栏小图标 */
btn-ghost-icon: 透明背景，hover 时 bg-slate-100，仅图标

/* success / danger outline — 交易操作 */
btn-outline-success: green 边框 + green 文字 (买入)
btn-outline-danger:  red 边框 + red 文字 (卖出)

/* cta — 主要行动按钮 */
btn-cta: primary 渐变背景 (primary-500 → primary-600)
```

---

## 6. Badge / Tag

为阶段标签（上升期/震荡期等）设计专门组件：

```
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ 🟢 上升期 │  │ 🟡 震荡期 │  │ 🔴 下降期 │  │ ⚪ 混沌期 │
└──────────┘  └──────────┘  └──────────┘  └──────────┘

改为:

┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ ↗ 上升期  │  │ ↔ 震荡期  │  │ ↘ 下降期  │  │ ○ 混沌期  │
└──────────┘  └──────────┘  └──────────┘  └──────────┘

使用 lucide 图标 + 半透明背景 + 对应颜色 border-left
```

---

## 7. Input / Select / DatePicker

当前基于 shadcn，质量尚可。微调：

- focus ring 从黑色改为 primary-500
- placeholder 颜色改为 `text-slate-400`
- DatePicker 的 selected day 从黑色改为 primary-500
- Select 下拉框加 `shadow-lg`

---

## 8. Tab 切换

当前 review-logs 和 raw-preview 有 Tab 切换。

**改进方案：**

```html
<div class="flex gap-1 p-1 bg-slate-100 rounded-lg">
  <button class="tab-active">判读记录</button>
  <button class="tab-inactive">交易记录</button>
</div>
```

- 使用 pill 式切换（`bg-slate-100` 容器 + 白色激活项）
- 代替当前独立的两个按钮
- 暗色模式下容器用 `bg-slate-800`

---

## 9. 加载 / 空状态 / 错误状态

当前已经处理了这三种状态，但视觉上可以加强：

**Loading:**
- 骨架屏代替 spinner（表格用 5 行骨架行）
- 卡片内容区域用 `animate-pulse` 灰色块

**Empty:**
- 居中插图（简单 SVG）+ 引导文字 + 行动按钮
- 不是只有灰色文字 "暂无数据"

**Error:**
- 红色左边框卡片 + 错误详情可展开
- 重试按钮突出

---

## 10. 图表区域（lightweight-charts）

当前图表功能完整，但容器设计需要改进：

- 图表容器加 `rounded-lg` + `shadow-sm` 卡片包裹
- 图例不要用裸 `div`，用半透明背景卡片浮在图表上方
- 时间轴和价格轴颜色与主题一致
- 十字光标颜色使用 primary-400

---

## 实施优先级

| 优先级 | 范围 | 预计工作量 |
|--------|------|----------|
| P0 | Layout + Sidebar + 色彩系统 | 2-3h |
| P1 | KPI 卡片 + 数据表格 | 2-3h |
| P2 | 按钮/徽章/输入组件微调 | 1-2h |
| P3 | 加载/空/错误状态 | 1-2h |
| P4 | 图表容器 + 动画细节 | 1-2h |

总计约 7-12 小时可将整体视觉提升到专业金融产品水准。
