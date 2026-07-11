# QRP Atlas 前端视觉设计方案 — 交付物索引

## 完成内容

为 QRP Atlas 项目创建了完整的前端视觉设计方案，包含以下交付物：

### 1. 设计交互预览 (`preview.html`)
可直接在浏览器中打开的交互式设计展示页面，包含：
- 完整色彩系统展示（品牌主色 + 语义色）
- 字体层级规范
- 组件库（Button / Badge / KPI Card / Data Table / Input / Tab）
- 页面 Layout 完整 Mockup
- Before/After 对比展示

### 2. 设计规范文档 (`README.md`)
方案总览，包括：
- 当前问题诊断（6 个核心视觉问题）
- 设计目标定义
- 品牌概念阐述
- 文件索引

### 3. Design Tokens 规范 (`design-tokens.md`)
完整的设计令牌规范，覆盖：
- 品牌主色 Atlas Sapphire 体系（亮/暗双模式）
- 语义色（红涨绿跌、琥珀强调）
- 字体族、字号阶梯、字重、行高
- 8px 间距系统
- 阴影与圆角系统
- 过渡动画参数
- 图表配色方案
- 图标系统映射

### 4. 组件改造规格 (`component-specs.md`)
10 组核心组件的详细改造方案，含实施优先级：
- P0: Layout + Sidebar + 色彩系统
- P1: KPI 卡片 + 数据表格
- P2: 按钮/徽章/输入组件微调
- P3: 加载/空/错误状态
- P4: 图表容器 + 动画细节

## 核心设计决策

| 决策 | 说明 |
|------|------|
| 品牌色 | Atlas Sapphire (blue-500) 替代原 neutral gray |
| 强调色 | Amber gold 用于数据高亮和 CTA |
| Sidebar | navy 渐变替代纯 slate-900 黑色 |
| 数据表格 | 涨跌幅加背景条 + 斑马纹 |
| 图标 | lucide-react 统一替代 emoji |
| 字符集 | 保留 Geist Variable + 引入等宽字体用于数据 |
