# tests/fixtures

本目录用于存放小型、脱敏、可复现的测试样本（CSV/JSON/小 DuckDB 脚本等）。

## 规则

- ✅ 允许：小型脱敏样本（几行 CSV/JSON）、生成样本的 Python 脚本、字段对照表。
- ❌ 禁止：真实完整行情库、真实 DuckDB 文件、生产数据库导出、大体积 parquet/CSV。
- ❌ 禁止：本机缓存、测试运行产物（`*.duckdb`、`*.duckdb.wal` 已在根 `.gitignore` 中忽略）。

## 当前约定

目前 API 层测试通过 `tests/conftest.py` 中的 `build_test_db()` 在 `tmp_path` 下动态
生成临时 DuckDB，不需要外部 fixture 文件。如未来需要更大规模的样本，请在此目录新增
小型 CSV 并在 `conftest.py` 中加载，不要直接提交 DuckDB 文件。
