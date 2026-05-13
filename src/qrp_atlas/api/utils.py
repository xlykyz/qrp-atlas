"""API 通用工具函数"""


def row_to_dict(row, columns):
    """将 DuckDB 行结果转为字典，日期字段转为 ISO 字符串"""
    d = dict(zip(columns, row))
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
    return d
