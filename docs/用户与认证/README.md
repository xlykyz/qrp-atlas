# QRP 用户与认证基础设施

## 定位

- `auth/` 负责确认请求者身份、密码验证和会话解析。
- `users/` 负责内部用户实体、状态与用户资料。
- PostgreSQL 仅承载用户与登录认证控制面。
- 策略配置、复盘、自选股及其他 QRP 业务数据继续存储在 DuckDB，并通过稳定的 `owner_user_id` 归属用户。

## 运行模式

### Local

默认模式，不需要登录，也不会访问 PostgreSQL：

```env
QRP_AUTH_MODE=local
QRP_LOCAL_USER_ID=f445c8c9-96d8-4ce7-9f8a-9e884dd038d8
QRP_LOCAL_USERNAME=ryan
QRP_LOCAL_DISPLAY_NAME=Ryan
```

### Database

显式启用 PostgreSQL 用户验证和不透明会话：

```env
QRP_AUTH_MODE=database
QRP_AUTH_DATABASE_URL=postgresql://qrp_auth:***@127.0.0.1:5432/qrp_auth
QRP_AUTH_SESSION_TTL_SECONDS=604800
```

数据库模式不会在连接失败或验证失败时自动降级为本地用户。

## API

- `GET /api/auth/me`
- `POST /api/auth/login`
- `POST /api/auth/logout`

Local 模式下 `/me` 直接返回固定用户；登录和退出端点明确返回不支持。

## PostgreSQL 初始化

1. 执行 `deploy/postgres/001_auth_schema.sql`。
2. 设置 `QRP_AUTH_MODE=database` 和 `QRP_AUTH_DATABASE_URL`。
3. 创建首个用户：

```bash
python -m qrp_atlas.auth.cli create-user --username ryan --display-name Ryan
```

## 业务层约束

业务路由不得接收前端传入的 `owner_user_id`。应通过 `CurrentUser` 获取内部用户 ID，并在 DuckDB 的创建、查询、修改和删除操作中强制附加用户范围。
