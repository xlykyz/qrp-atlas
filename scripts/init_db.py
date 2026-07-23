from qrp_atlas.config import DB_PATH, ensure_dirs
from qrp_atlas.database import create_empty_database


def main() -> None:
    ensure_dirs()
    tables = create_empty_database(DB_PATH)
    print(f"数据库初始化完成: {DB_PATH}")
    print("已创建表:")
    for table in tables:
        print(" -", table)


if __name__ == "__main__":
    main()
