# 临时公网穿透方案

目标：将 Linux 开发机上的 FastAPI 后端 (0.0.0.0:8000) 临时暴露到公网，供前端或其他外部客户端访问。

## 环境前提

- **目标机器**：Linux 开发机（非本 Windows 机器）
- **后端**：uvicorn 监听 `0.0.0.0:8000`，systemd 服务 `qrp-atlas-api.service`，用户 `claire`
- **内网地址**：当前 `192.168.0.102:8000`

---

## 方案一：ngrok（推荐：最省事）

适合临时演示、快速调试。免费版够用，但每次重启域名随机变化。

### 安装（Linux 开发机上执行一次）

```bash
# 方式 A：snap（推荐，自动更新）
sudo snap install ngrok

# 方式 B：手动下载
curl -s https://ngrok-agent.s3.amazonaws.com/ngrok.asc | sudo tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null
echo "deb https://ngrok-agent.s3.amazonaws.com buster main" | sudo tee /etc/apt/sources.list.d/ngrok.list
sudo apt update && sudo apt install ngrok
```

首次使用需要注册免费账号并配置 authtoken：

```bash
ngrok config add-authtoken <你的authtoken>
```

### 启动

```bash
ngrok http 8000
```

输出示例：

```
Forwarding  https://abcd-1234.ngrok-free.app -> http://localhost:8000
```

### 使用

- 将前端 `web/.env.local` 中的 `VITE_API_BASE_URL` 改为上面的 ngrok URL
- 重启 Vite dev server
- API docs 可通过 `https://xxxx.ngrok-free.app/docs` 访问

### 后台运行

```bash
# 方式 A：screen
screen -S ngrok
ngrok http 8000
# Ctrl+A D 脱离，screen -r ngrok 重新进入

# 方式 B：systemd（见方案一附录）
```

### 限制

- 免费版：每分钟 40 连接，每次重启域名变
- 付费版（$8/月）：固定域名 + 更高并发

### 附录：systemd 单元（可选）

`/etc/systemd/system/ngrok-tunnel.service`:

```ini
[Unit]
Description=ngrok tunnel for QRP Atlas API
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=claire
Environment=NGROK_AUTHTOKEN=<你的token>
ExecStart=/usr/bin/ngrok http 8000 --log stdout
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ngrok-tunnel.service
# 查看公网 URL：curl -s http://127.0.0.1:4040/api/tunnels | jq -r '.tunnels[0].public_url'
```

---

## 方案二：frp（自建中转）

适合你有公网 VPS、需要长期稳定穿透的场景。无流量限制，延迟低。

### 架构

```
公网客户端 --> VPS(frps) --> 内网 Linux 开发机(frpc) --> localhost:8000
```

### VPS 上部署 frps

```bash
# 下载 frp
wget https://github.com/fatedier/frp/releases/download/v0.61.2/frp_0.61.2_linux_amd64.tar.gz
tar xzf frp_*.tar.gz && cd frp_*

# frps.toml 最小配置
cat > frps.toml << 'EOF'
bindPort = 7000
vhostHTTPPort = 8080
EOF

# 启动
./frps -c frps.toml
```

### Linux 开发机上部署 frpc

```bash
# 下载同版本 frp
wget https://github.com/fatedier/frp/releases/download/v0.61.2/frp_0.61.2_linux_amd64.tar.gz
tar xzf frp_*.tar.gz && cd frp_*

# frpc.toml 配置
cat > frpc.toml << 'EOF'
serverAddr = "<VPS公网IP>"
serverPort = 7000

[[proxies]]
name = "qrp-atlas-api"
type = "http"
localIP = "127.0.0.1"
localPort = 8000
remotePort = 8080
EOF

# 启动
./frpc -c frpc.toml
```

公网访问地址：`http://<VPS_IP>:8080`

### TLS 自选

建议在 VPS 前加一层 Nginx/Caddy 反代提供 HTTPS。

---

## 方案三：Cloudflare Tunnel

适合已有 Cloudflare 域名、需要免费稳定隧道的场景。自带 HTTPS，不限制带宽。

### 前提

- 一个域名已接入 Cloudflare DNS
- Linux 开发机可出站访问 Cloudflare

### 步骤

```bash
# 1. 安装 cloudflared
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb -o cloudflared.deb
sudo dpkg -i cloudflared.deb

# 2. 认证
cloudflared tunnel login
# 浏览器打开输出的 URL，选择域名授权

# 3. 创建隧道
cloudflared tunnel create qrp-atlas

# 4. 配置 DNS
cloudflared tunnel route dns qrp-atlas api.example.com
# 替换为你自己的域名

# 5. 编写 config.yml
cat > ~/.cloudflared/config.yml << 'EOF'
tunnel: <tunnel-id>
credentials-file: /home/claire/.cloudflared/<tunnel-id>.json
ingress:
  - hostname: api.example.com
    service: http://localhost:8000
  - service: http_status:404
EOF

# 6. 运行
cloudflared tunnel run qrp-atlas
```

完成后可通过 `https://api.example.com` 访问，自动 HTTPS。

---

## 方案四：bore / localtunnel（极简零配置）

无需注册、无需安装客户端（localtunnel 用 npx）。适合一次性快速测试。

### bore

```bash
# 安装
cargo install bore-cli

# 穿透
bore local 8000 --to bore.pub
# 输出：https://bore.pub -> localhost:8000
```

### localtunnel

```bash
# 无需安装，npx 直接跑
npx localtunnel --port 8000
# 输出：your url is: https://xxxx.loca.lt
```

**注意**：localtunnel 首次访问需要输入公网 IP 验证。

---

## 前端切换

无论使用哪种方案，穿透建立后：

1. 更新本 Windows 机器的 `web/.env.local`:
   ```bash
   VITE_API_BASE_URL=https://xxxx.ngrok-free.app
   ```
2. 重启 Vite dev server

---

## 安全提醒

- 这些方案会将内网 API 暴露到公网，**无鉴权**
- 非调试期间请关闭隧道
- 不建议长期暴露生产数据的 API
- 若需长期使用，建议加一层简单鉴权或 IP 白名单