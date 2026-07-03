# qrp-atlas web

qrp-atlas 前端应用。这个目录可以作为独立的 Vite 项目开发，不需要在本机启动数据管道或写入数据库。

## 本机前端开发

```bash
npm install
npm run dev
```

开发服务默认运行在 `http://localhost:3000`。

## API 地址

前端通过 `VITE_API_BASE_URL` 调用后端 API。首次在本机开发时复制环境变量模板：

```bash
copy .env.example .env.local
```

然后把 `.env.local` 里的地址改成目标后端，例如：

```bash
VITE_API_BASE_URL=http://192.168.x.x:8000
```

如果不设置 `VITE_API_BASE_URL`，前端会默认请求当前页面主机的 `:8000` 端口。

## 常用命令

```bash
npm run dev
npm run lint
npm run build
npm run preview
```
