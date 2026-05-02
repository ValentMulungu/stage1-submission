
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipeline.utils import get_spark, load_config, read_delta


def output_path(config: dict, logical_name: str) -> str:
    output = config.get("output", {})
    value = output.get(logical_name) or output.get(f"{logical_name}_path")

    if not value:
        raise KeyError(
            f"Missing output path for '{logical_name}'. "
            f"Available output keys: {list(output.keys())}"
        )

    return value


def count_rows(df: DataFrame) -> int:
    return int(df.count())


def count_nulls(df: DataFrame, column_name: str) -> int:
    if column_name not in df.columns:
        return -1
    return int(df.where(F.col(column_name).isNull()).count())


def count_duplicates(df: DataFrame, key_columns: List[str]) -> int:
    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        return -1

    return int(
        df.groupBy(*key_columns)
        .count()
        .where(F.col("count") > 1)
        .count()
    )


def sample_records(df: DataFrame, limit: int = 5) -> List[dict]:
    return [row.asDict(recursive=True) for row in df.limit(limit).collect()]


def validate_table_exists(path: str) -> bool:
    return Path(path).exists()


def main() -> None:
    print("[Validation] Starting output validation...")

    config = load_config()
    spark = get_spark(config, "validation")

    bronze_root = output_path(config, "bronze")
    silver_root = output_path(config, "silver")
    gold_root = output_path(config, "gold")

    report: Dict = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "PASS",
        "tables": {},
        "checks": {},
        "samples": {},
        "official_validation_queries": {},
        "recommendations": [],
    }

    expected_paths = {
        "bronze_accounts": f"{bronze_root}/accounts",
        "bronze_customers": f"{bronze_root}/customers",
        "bronze_transactions": f"{bronze_root}/transactions",
        "silver_accounts": f"{silver_root}/accounts",
        "silver_customers": f"{silver_root}/customers",
        "silver_transactions": f"{silver_root}/transactions",
        "gold_dim_accounts": f"{gold_root}/dim_accounts",
        "gold_dim_customers": f"{gold_root}/dim_customers",
        "gold_fact_transactions": f"{gold_root}/fact_transactions",
    }

    missing_tables = [name for name, path in expected_paths.items() if not validate_table_exists(path)]
    report["checks"]["missing_tables"] = missing_tables

    if missing_tables:
        report["status"] = "FAIL"

    bronze_accounts = read_delta(spark, expected_paths["bronze_accounts"])
    bronze_customers = read_delta(spark, expected_paths["bronze_customers"])
    bronze_transactions = read_delta(spark, expected_paths["bronze_transactions"])

    silver_accounts = read_delta(spark, expected_paths["silver_accounts"])
    silver_customers = read_delta(spark, expected_paths["silver_customers"])
    silver_transactions = read_delta(spark, expected_paths["silver_transactions"])

    dim_accounts = read_delta(spark, expected_paths["gold_dim_accounts"])
    dim_customers = read_delta(spark, expected_paths["gold_dim_customers"])
    fact_transactions = read_delta(spark, expected_paths["gold_fact_transactions"])

    counts = {
        "bronze_accounts": count_rows(bronze_accounts),
        "bronze_customers": count_rows(bronze_customers),
        "bronze_transactions": count_rows(bronze_transactions),
        "silver_accounts": count_rows(silver_accounts),
        "silver_customers": count_rows(silver_customers),
        "silver_transactions": count_rows(silver_transactions),
        "gold_dim_accounts": count_rows(dim_accounts),
        "gold_dim_customers": count_rows(dim_customers),
        "gold_fact_transactions": count_rows(fact_transactions),
    }

    report["tables"]["row_counts"] = counts

    report["checks"]["duplicates"] = {
        "silver_accounts_duplicate_account_id": count_duplicates(silver_accounts, ["account_id"]),
        "silver_customers_duplicate_customer_id": count_duplicates(silver_customers, ["customer_id"]),
        "silver_transactions_duplicate_transaction_id": count_duplicates(silver_transactions, ["transaction_id"]),
        "gold_dim_accounts_duplicate_account_sk": count_duplicates(dim_accounts, ["account_sk"]),
        "gold_dim_customers_duplicate_customer_sk": count_duplicates(dim_customers, ["customer_sk"]),
        "gold_fact_transactions_duplicate_transaction_sk": count_duplicates(fact_transactions, ["transaction_sk"]),
    }

    report["checks"]["null_critical_keys"] = {
        "silver_accounts_null_account_id": count_nulls(silver_accounts, "account_id"),
        "silver_customers_null_customer_id": count_nulls(silver_customers, "customer_id"),
        "silver_transactions_null_transaction_id": count_nulls(silver_transactions, "transaction_id"),
        "silver_transactions_null_account_id": count_nulls(silver_transactions, "account_id"),
        "gold_dim_accounts_null_account_sk": count_nulls(dim_accounts, "account_sk"),
        "gold_dim_customers_null_customer_sk": count_nulls(dim_customers, "customer_sk"),
        "gold_fact_transactions_null_transaction_sk": count_nulls(fact_transactions, "transaction_sk"),
        "gold_fact_transactions_null_account_sk": count_nulls(fact_transactions, "account_sk"),
        "gold_fact_transactions_null_customer_sk": count_nulls(fact_transactions, "customer_sk"),
    }

    fact_account_orphans = (
        fact_transactions.select("account_sk").dropDuplicates()
        .join(dim_accounts.select("account_sk").dropDuplicates(), on="account_sk", how="left_anti")
        .count()
    )

    fact_customer_orphans = (
        fact_transactions.select("customer_sk").dropDuplicates()
        .join(dim_customers.select("customer_sk").dropDuplicates(), on="customer_sk", how="left_anti")
        .count()
    )

    account_customer_orphans = (
        dim_accounts.select("customer_id").dropDuplicates()
        .where(F.col("customer_id").isNotNull())
        .join(dim_customers.select("customer_id").dropDuplicates(), on="customer_id", how="left_anti")
        .count()
    )

    report["checks"]["referential_integrity"] = {
        "fact_account_sk_not_in_dim_accounts": int(fact_account_orphans),
        "fact_customer_sk_not_in_dim_customers": int(fact_customer_orphans),
        "dim_account_customer_id_not_in_dim_customers": int(account_customer_orphans),
    }

    report["checks"]["reconciliation"] = {
        "bronze_to_silver_accounts_delta": counts["bronze_accounts"] - counts["silver_accounts"],
        "bronze_to_silver_customers_delta": counts["bronze_customers"] - counts["silver_customers"],
        "bronze_to_silver_transactions_delta": counts["bronze_transactions"] - counts["silver_transactions"],
        "silver_transactions_to_gold_fact_delta": counts["silver_transactions"] - counts["gold_fact_transactions"],
    }

    if "amount" in fact_transactions.columns:
        amount_summary = (
            fact_transactions
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("average_amount"),
                F.min("amount").alias("minimum_amount"),
                F.max("amount").alias("maximum_amount"),
            )
            .collect()[0]
            .asDict()
        )

        report["tables"]["fact_transaction_amount_summary"] = {
            key: float(value) if value is not None and key != "transaction_count" else value
            for key, value in amount_summary.items()
        }

    official_transaction_volume = (
        fact_transactions
        .groupBy("transaction_type")
        .agg(
            F.count("*").alias("record_count"),
            F.sum("amount").alias("total_amount"),
        )
        .withColumn(
            "pct_of_total",
            F.round(
                F.col("record_count") * F.lit(100.0)
                / F.sum("record_count").over(Window.partitionBy()),
                2,
            ),
        )
        .orderBy("transaction_type")
    )

    official_unlinked_accounts = (
        dim_accounts.alias("a")
        .join(
            dim_customers.alias("c"),
            F.col("a.customer_id") == F.col("c.customer_id"),
            "left",
        )
        .where(F.col("c.customer_id").isNull())
        .count()
    )

    official_province_distribution = (
        dim_accounts.alias("a")
        .join(
            dim_customers.alias("c"),
            F.col("a.customer_id") == F.col("c.customer_id"),
            "inner",
        )
        .groupBy(F.col("c.province").alias("province"))
        .agg(F.countDistinct("a.account_id").alias("account_count"))
        .orderBy("province")
    )

    report["official_validation_queries"] = {
        "query_1_transaction_volume_by_type": [
            row.asDict(recursive=True)
            for row in official_transaction_volume.collect()
        ],
        "query_2_unlinked_accounts": int(official_unlinked_accounts),
        "query_3_province_distribution": [
            row.asDict(recursive=True)
            for row in official_province_distribution.collect()
        ],
    }

    report["samples"]["gold_dim_accounts"] = sample_records(dim_accounts, 5)
    report["samples"]["gold_dim_customers"] = sample_records(dim_customers, 5)
    report["samples"]["gold_fact_transactions"] = sample_records(fact_transactions, 5)

    issues_found = False

    for value in report["checks"]["duplicates"].values():
        if value not in (0, -1):
            issues_found = True

    for value in report["checks"]["null_critical_keys"].values():
        if value not in (0, -1):
            issues_found = True

    for value in report["checks"]["referential_integrity"].values():
        if value != 0:
            issues_found = True

    expected_types = {"CREDIT", "DEBIT", "FEE", "REVERSAL"}
    actual_types = {row["transaction_type"] for row in report["official_validation_queries"]["query_1_transaction_volume_by_type"]}
    if actual_types != expected_types:
        issues_found = True
        report["recommendations"].append(
            f"Transaction types should be exactly {sorted(expected_types)} but found {sorted(actual_types)}"
        )

    if report["official_validation_queries"]["query_2_unlinked_accounts"] != 0:
        issues_found = True
        report["recommendations"].append("Unlinked accounts found in dim_accounts")

    expected_provinces = {
        "Eastern Cape",
        "Free State",
        "Gauteng",
        "KwaZulu-Natal",
        "Limpopo",
        "Mpumalanga",
        "North West",
        "Northern Cape",
        "Western Cape",
    }
    actual_provinces = {row["province"] for row in report["official_validation_queries"]["query_3_province_distribution"]}
    if actual_provinces != expected_provinces:
        issues_found = True
        report["recommendations"].append(
            f"Province values should be exactly {sorted(expected_provinces)} but found {sorted(actual_provinces)}"
        )

    if issues_found and report["status"] != "FAIL":
        report["status"] = "WARN"

    output_report_path = "/data/output/validation_report.json"
    Path(output_report_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_report_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, default=str)

    print("[Validation] Completed.")
    print(f"[Validation] Status: {report['status']}")
    print(f"[Validation] Report written to: {output_report_path}")

    spark.stop()


if __name__ == "__main__":
    main()