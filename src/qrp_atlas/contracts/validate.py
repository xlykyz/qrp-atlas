"""
validate.py - 极简校验函数

提供MVP级别的数据校验功能:
- 缺列检测: 检查 DataFrame 是否缺少必需列
- 多列检测: 检查 DataFrame 是否有多余列
- 类型转换: 自动转换数值/布尔/日期类型

使用示例:
    from qrp_atlas.contracts import quick_validate, validate_schema

    # 快速校验 + 自动类型转换
    df = quick_validate(df, "daily_market_snapshot")

    # 仅校验结构
    validate_schema(df, "daily_market_snapshot", allow_extra=True)

    # 单独使用类型转换
    from qrp_atlas.contracts import convert_numeric, convert_boolean, convert_date
    df = convert_numeric(df)
    df = convert_boolean(df)
    df = convert_date(df)

异常类型:
    - ValidationError: 校验异常基类
    - MissingColumnsError: 缺少必需列
    - ExtraColumnsError: 存在多余列(strict模式)
    - TypeConversionError: 类型转换失败
"""

from typing import List, Set, Tuple
import pandas as pd

from .schema import get_table
from .conventions import NUMERIC_COLUMNS, BOOLEAN_COLUMNS, DATE_COLUMNS

_NUMERIC_DUCKDB_TYPES = {"DOUBLE", "INTEGER", "BIGINT", "DECIMAL", "FLOAT"}


def _schema_columns_by_dtype(table_name: str) -> tuple[list[str], list[str], list[str]]:
    """按 DuckDB dtype 从 schema 推导数值、布尔、日期列。"""
    schema = get_table(table_name)
    numeric_columns = []
    boolean_columns = []
    date_columns = []
    for col in schema.columns:
        dtype = col.dtype.upper().split("(", 1)[0]
        if dtype in _NUMERIC_DUCKDB_TYPES:
            numeric_columns.append(col.name)
        elif dtype == "BOOLEAN":
            boolean_columns.append(col.name)
        elif dtype in {"DATE", "TIMESTAMP"}:
            date_columns.append(col.name)
    return numeric_columns, boolean_columns, date_columns


class ValidationError(Exception):
    """校验异常基类"""
    pass


class MissingColumnsError(ValidationError):
    """缺少必需列异常

    Attributes:
        missing: 缺少的列名集合
        table: 表名(可选)
    """
    def __init__(self, missing: Set[str], table: str = None):
        self.missing = missing
        self.table = table
        msg = f"Missing columns: {sorted(missing)}"
        if table:
            msg = f"[{table}] {msg}"
        super().__init__(msg)


class ExtraColumnsError(ValidationError):
    """存在多余列异常(strict模式)

    Attributes:
        extra: 多余的列名集合
        table: 表名(可选)
    """
    def __init__(self, extra: Set[str], table: str = None):
        self.extra = extra
        self.table = table
        msg = f"Extra columns: {sorted(extra)}"
        if table:
            msg = f"[{table}] {msg}"
        super().__init__(msg)


class TypeConversionError(ValidationError):
    """类型转换失败异常

    Attributes:
        column: 列名
        dtype: 目标类型
        errors: 错误数量
        table: 表名(可选)
    """
    def __init__(self, column: str, dtype: str, errors: int, table: str = None):
        self.column = column
        self.dtype = dtype
        self.errors = errors
        self.table = table
        msg = f"Failed to convert column '{column}' to {dtype}: {errors} errors"
        if table:
            msg = f"[{table}] {msg}"
        super().__init__(msg)


def check_missing_columns(
    df: pd.DataFrame,
    required: Set[str],
    table_name: str = None
) -> Tuple[bool, Set[str]]:
    """检查 DataFrame 是否缺少必需列

    Args:
        df: pandas DataFrame
        required: 必需列名集合
        table_name: 表名(用于错误信息, 可选)

    Returns:
        (是否有缺失, 缺失列集合) - 无缺失时返回 (False, set())

    Raises:
        MissingColumnsError: 存在缺失列时抛出

    Example:
        check_missing_columns(df, {"ticker", "trade_date"})
    """
    actual = set(df.columns)
    missing = required - actual
    if missing:
        raise MissingColumnsError(missing, table_name)
    return False, set()


def check_extra_columns(
    df: pd.DataFrame,
    expected: Set[str],
    table_name: str = None,
    strict: bool = False
) -> Tuple[bool, Set[str]]:
    """检查 DataFrame 是否存在多余列

    Args:
        df: pandas DataFrame
        expected: 期望列名集合
        table_name: 表名(用于错误信息, 可选)
        strict: 严格模式, 存在多余列时抛出异常

    Returns:
        (是否有多余, 多余列集合)

    Raises:
        ExtraColumnsError: strict=True 且存在多余列时抛出

    Example:
        has_extra, extra_cols = check_extra_columns(df, {"ticker", "close"})
        check_extra_columns(df, {"ticker"}, strict=True)  # 严格模式
    """
    actual = set(df.columns)
    extra = actual - expected
    if extra:
        if strict:
            raise ExtraColumnsError(extra, table_name)
        return True, extra
    return False, set()


def validate_schema(
    df: pd.DataFrame,
    table_name: str,
    allow_extra: bool = True
) -> Tuple[bool, Set[str], Set[str]]:
    """校验 DataFrame 是否符合表结构

    Args:
        df: pandas DataFrame
        table_name: 表名
        allow_extra: 是否允许额外列，默认 True

    Returns:
        (校验通过, 缺失列集合, 多余列集合)

    Raises:
        MissingColumnsError: 缺少必需列时抛出
        ExtraColumnsError: allow_extra=False 且存在多余列时抛出

    Example:
        validate_schema(df, "daily_market_snapshot")
        validate_schema(df, "daily_market_snapshot", allow_extra=False)
    """
    schema = get_table(table_name)
    required = {col.name for col in schema.columns if col.nullable is False} | set(schema.primary_key)
    expected = set(schema.column_names())
    try:
        check_missing_columns(df, required, table_name)
    except MissingColumnsError:
        raise
    extra_cols = set()
    if not allow_extra:
        try:
            _, extra_cols = check_extra_columns(df, expected, table_name, strict=True)
        except ExtraColumnsError:
            raise
    else:
        _, extra_cols = check_extra_columns(df, expected, table_name, strict=False)
    return True, set(), extra_cols


def align_to_schema(
    df: pd.DataFrame,
    table_name: str,
    fill_missing_optional: bool = True,
    drop_extra: bool = False,
) -> pd.DataFrame:
    """按 schema 补齐 optional columns，并按 schema 顺序重排列。"""
    schema = get_table(table_name)
    df = df.copy()

    required = {col.name for col in schema.columns if col.nullable is False} | set(schema.primary_key)
    expected = list(schema.column_names())
    check_missing_columns(df, required, table_name)

    if fill_missing_optional:
        for col in schema.columns:
            if col.nullable and col.name not in df.columns:
                df[col.name] = None

    schema_cols = [col for col in expected if col in df.columns]
    if drop_extra:
        return df[schema_cols]

    extra_cols = [col for col in df.columns if col not in expected]
    return df[schema_cols + extra_cols]


def convert_numeric(
    df: pd.DataFrame,
    columns: List[str] = None,
    errors: str = "coerce"
) -> pd.DataFrame:
    """转换数值类型列

    Args:
        df: pandas DataFrame
        columns: 要转换的列名列表，默认转换所有 NUMERIC_COLUMNS
        errors: 错误处理方式，"coerce"转为NaN，"raise"抛出异常

    Returns:
        转换后的 DataFrame(副本)

    Example:
        df = convert_numeric(df)
        df = convert_numeric(df, columns=["close", "volume"])
    """
    df = df.copy()
    cols_to_convert = columns if columns else [c for c in df.columns if c in NUMERIC_COLUMNS]
    for col in cols_to_convert:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors=errors)
    return df


def convert_boolean(
    df: pd.DataFrame,
    columns: List[str] = None,
    true_values: Set[str] = None,
    false_values: Set[str] = None
) -> pd.DataFrame:
    """转换布尔类型列

    Args:
        df: pandas DataFrame
        columns: 要转换的列名列表，默认转换所有 BOOLEAN_COLUMNS
        true_values: True 值集合，默认 {"1", "true", "True", ...}
        false_values: False 值集合，默认 {"0", "false", "False", ...}

    Returns:
        转换后的 DataFrame(副本)

    Example:
        df = convert_boolean(df)
        df = convert_boolean(df, true_values={"是", "1"}, false_values={"否", "0"})
    """
    df = df.copy()
    cols_to_convert = columns if columns else [c for c in df.columns if c in BOOLEAN_COLUMNS]
    true_vals = true_values or {"1", "true", "True", "TRUE", "yes", "Yes", "YES", "是"}
    false_vals = false_values or {"0", "false", "False", "FALSE", "no", "No", "NO", "否"}
    for col in cols_to_convert:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: True if str(x) in true_vals else (False if str(x) in false_vals else pd.NA)
            )
    return df


def convert_date(
    df: pd.DataFrame,
    columns: List[str] = None,
    format: str = None
) -> pd.DataFrame:
    """转换日期类型列

    Args:
        df: pandas DataFrame
        columns: 要转换的列名列表，默认转换所有 DATE_COLUMNS
        format: 日期格式，默认自动检测

    Returns:
        转换后的 DataFrame(副本)

    Example:
        df = convert_date(df)
        df = convert_date(df, format="%Y%m%d")
    """
    df = df.copy()
    cols_to_convert = columns if columns else [c for c in df.columns if c in DATE_COLUMNS]
    for col in cols_to_convert:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=format, errors="coerce")
    return df


def canonicalize(
    df: pd.DataFrame,
    table_name: str = None,
    numeric_columns: List[str] = None,
    boolean_columns: List[str] = None,
    date_columns: List[str] = None,
    date_format: str = None
) -> pd.DataFrame:
    """标准化 DataFrame 类型

    依次执行数值、布尔、日期类型转换。

    Args:
        df: pandas DataFrame
        table_name: 表名(仅用于日志, 可选)
        numeric_columns: 数值列，默认 NUMERIC_COLUMNS
        boolean_columns: 布尔列，默认 BOOLEAN_COLUMNS
        date_columns: 日期列，默认 DATE_COLUMNS
        date_format: 日期格式

    Returns:
        标准化后的 DataFrame

    Example:
        df = canonicalize(df)
    """
    if table_name:
        schema_numeric, schema_boolean, schema_date = _schema_columns_by_dtype(table_name)
        numeric_columns = numeric_columns or schema_numeric
        boolean_columns = boolean_columns or schema_boolean
        date_columns = date_columns or schema_date
    df = convert_numeric(df, numeric_columns)
    df = convert_boolean(df, boolean_columns)
    df = convert_date(df, date_columns, format=date_format)
    return df


def quick_validate(
    df: pd.DataFrame,
    table_name: str,
    allow_extra: bool = True,
    auto_convert: bool = True
) -> pd.DataFrame:
    """快速校验并标准化 DataFrame

    组合 validate_schema + canonicalize，一步完成校验和类型转换。

    Args:
        df: pandas DataFrame
        table_name: 表名
        allow_extra: 是否允许额外列，默认 True
        auto_convert: 是否自动转换类型，默认 True

    Returns:
        校验通过且类型转换后的 DataFrame

    Raises:
        MissingColumnsError: 缺少必需列时抛出
        ExtraColumnsError: allow_extra=False 且存在多余列时抛出

    Example:
        df = quick_validate(df, "daily_market_snapshot")
        df = quick_validate(df, "daily_market_snapshot", allow_extra=False, auto_convert=False)
    """
    validate_schema(df, table_name, allow_extra=allow_extra)
    if auto_convert:
        df = canonicalize(df, table_name)
    return df
