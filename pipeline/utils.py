from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


DEFAULT_CONFIG_PATH = "/data/config/pipeline_config.yaml"
FALLBACK_CONFIG_PATH = "/app/config/pipeline_config.yaml"

DQ_CODES = [
    "ORPHANED_ACCOUNT",
    "DUPLICATE_DEDUPED",
    "TYPE_MISMATCH",
    "DATE_FORMAT",
    "CURRENCY_VARIANT",
    "NULL_REQUIRED",
]


def load_config() -> Dict:
    config_path = os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG_PATH)

    if not os.path.exists(config_path):
        config_path = FALLBACK_CONFIG_PATH

    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def get_spark(config: Dict, stage_name: str) -> SparkSession:
    """Create Spark session without runtime Maven/Delta downloads."""

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("HOSTNAME", "localhost")
    os.environ.setdefault("PYSPARK_PYTHON", "python")
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python")

    spark_config = config.get("spark", {}) or {}

    master = spark_config.get("master", "local[2]")
    app_name = f"{spark_config.get('app_name', 'nedbank-de-pipeline')}-{stage_name}"
    shuffle_partitions = int(spark_config.get("shuffle_partitions", 4))

    spark = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.ip", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.hadoop.parquet.compression", "UNCOMPRESSED")
        .config("spark.io.compression.codec", "lz4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def reset_path(path: str) -> None:
    if path.startswith("/data/output") or path.startswith("/tmp"):
        shutil.rmtree(path, ignore_errors=True)


def write_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Compatibility wrapper: writes parquet to avoid runtime Delta JAR download."""

    Path(path).parent.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .format("parquet")
        .mode(mode)
        .option("compression", "uncompressed")
        .save(path)
    )


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Compatibility wrapper: reads parquet output paths."""

    return spark.read.format("parquet").load(path)


def stable_bigint_key(*cols: str) -> F.Column:
    prepared = [
        F.coalesce(F.col(c).cast("string"), F.lit(""))
        for c in cols
    ]

    return F.pmod(
        F.xxhash64(*prepared),
        F.lit(9223372036854775807),
    ).cast(T.LongType())


def parse_flexible_date(col_name: str) -> F.Column:
    c = F.trim(F.col(col_name).cast("string"))

    epoch_seconds = (
        F.when(
            c.rlike(r"^[0-9]{13}$"),
            (c.cast("double") / F.lit(1000)).cast("long"),
        )
        .when(
            c.rlike(r"^[0-9]{10}$"),
            c.cast("long"),
        )
    )

    return F.coalesce(
        F.to_date(c, "yyyy-MM-dd"),
        F.to_date(c, "dd/MM/yyyy"),
        F.to_date(F.from_unixtime(epoch_seconds)),
    )


def parse_timestamp(date_col: str, time_col: str) -> F.Column:
    parsed_date = F.date_format(parse_flexible_date(date_col), "yyyy-MM-dd")

    parsed_time = F.coalesce(
        F.trim(F.col(time_col).cast("string")),
        F.lit("00:00:00"),
    )

    return F.to_timestamp(
        F.concat_ws(" ", parsed_date, parsed_time),
        "yyyy-MM-dd HH:mm:ss",
    )


def standardise_currency(col_name: str) -> F.Column:
    c = F.upper(F.trim(F.col(col_name).cast("string")))

    return (
        F.when(c.isin("ZAR", "R", "RAND", "RANDS", "710"), F.lit("ZAR"))
        .otherwise(c)
    )


def dedupe_latest(
    df: DataFrame,
    key_col: str,
    order_cols: Optional[Iterable[F.Column]] = None,
) -> DataFrame:
    if order_cols is None:
        order_cols = [F.col("ingestion_timestamp").desc_nulls_last()]

    w = Window.partitionBy(key_col).orderBy(*order_cols)

    return (
        df
        .where(
            F.col(key_col).isNotNull()
            & (F.trim(F.col(key_col).cast("string")) != "")
        )
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def add_missing_columns(df: DataFrame, required: Dict[str, T.DataType]) -> DataFrame:
    for name, dtype in required.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))

    return df


def coerce_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    for field in schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))

    return df.select(
        [
            F.col(field.name).cast(field.dataType).alias(field.name)
            for field in schema.fields
        ]
    )


def write_dq_report(path: str, counts: Optional[Dict[str, int]] = None) -> None:
    counts = counts or {}

    issue_counts = {
        code: int(counts.get(code, 0))
        for code in DQ_CODES
    }

    total_flagged = sum(issue_counts.values())

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_flagged_records": total_flagged,
            "issue_count": issue_counts,
        },
        "issues": [
            {
                "issue_code": code,
                "record_count": issue_counts[code],
                "handling_action": _handling_action_for(code),
            }
            for code in DQ_CODES
        ],
    }

    Path(path).parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)


def _handling_action_for(code: str) -> str:
    return {
        "ORPHANED_ACCOUNT": "quarantine_from_gold_fact_and_report",
        "DUPLICATE_DEDUPED": "deduplicate_keep_latest_and_report",
        "TYPE_MISMATCH": "cast_to_expected_type_and_report_if_uncastable",
        "DATE_FORMAT": "parse_supported_formats_and_report_if_unparseable",
        "CURRENCY_VARIANT": "standardise_to_ZAR",
        "NULL_REQUIRED": "reject_from_silver_and_report",
    }.get(code, "report")