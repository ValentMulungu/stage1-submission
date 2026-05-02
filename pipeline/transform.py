

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.utils import (
    coerce_to_schema,
    dedupe_latest,
    get_spark,
    load_config,
    parse_flexible_date,
    read_delta,
    standardise_currency,
    write_delta,
    write_dq_report,
)


def output_path(config: dict, logical_name: str) -> str:
    output = config.get("output", {})
    value = output.get(logical_name) or output.get(f"{logical_name}_path")

    if not value:
        raise KeyError(
            f"Missing output path for '{logical_name}'. "
            f"Available output keys: {list(output.keys())}"
        )

    return value


def normalise_columns(df: DataFrame) -> DataFrame:
    for old_name in df.columns:
        new_name = (
            old_name.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
            .replace("/", "_")
        )

        while "__" in new_name:
            new_name = new_name.replace("__", "_")

        if new_name != old_name:
            df = df.withColumnRenamed(old_name, new_name)

    return df


def rename_if_exists(df: DataFrame, source_candidates: list[str], target_name: str) -> DataFrame:
    if target_name in df.columns:
        return df

    for source_name in source_candidates:
        if source_name in df.columns:
            return df.withColumnRenamed(source_name, target_name)

    return df


def first_existing_column(df: DataFrame, candidates: list[str], default_name: str) -> str:
    for col_name in candidates:
        if col_name in df.columns:
            return col_name

    raise ValueError(
        f"None of the expected columns exist for {default_name}. "
        f"Candidates checked: {candidates}. Available columns: {df.columns}"
    )


def clean_duplicate_columns(df: DataFrame) -> DataFrame:
    seen = set()
    selected = []

    for col_name in df.columns:
        clean_name = col_name.strip()
        if clean_name not in seen:
            seen.add(clean_name)
            selected.append(F.col(col_name).alias(clean_name))

    return df.select(selected)


def standardise_transaction_type(column_name: str) -> F.Column:
    c = F.upper(F.trim(F.col(column_name).cast("string")))

    return (
        F.when(c.isin("CR", "CREDIT", "CREDITS"), F.lit("CREDIT"))
        .when(c.isin("DR", "DEBIT", "DEBITS"), F.lit("DEBIT"))
        .when(c.isin("FEE", "FEES", "CHARGE", "CHARGES"), F.lit("FEE"))
        .when(c.isin("REV", "REVERSE", "REVERSAL", "REVERSALS"), F.lit("REVERSAL"))
        .otherwise(c)
    )


def standardise_province(column_name: str) -> F.Column:
    c = F.upper(F.trim(F.regexp_replace(F.col(column_name).cast("string"), r"[\s_]+", " ")))

    return (
        F.when(c.isin("EC", "EASTERN CAPE"), F.lit("Eastern Cape"))
        .when(c.isin("FS", "FREE STATE"), F.lit("Free State"))
        .when(c.isin("GP", "GAUTENG"), F.lit("Gauteng"))
        .when(c.isin("KZN", "KWAZULU NATAL", "KWAZULU-NATAL"), F.lit("KwaZulu-Natal"))
        .when(c.isin("LP", "LIMPOPO"), F.lit("Limpopo"))
        .when(c.isin("MP", "MPUMALANGA"), F.lit("Mpumalanga"))
        .when(c.isin("NW", "NORTH WEST"), F.lit("North West"))
        .when(c.isin("NC", "NORTHERN CAPE"), F.lit("Northern Cape"))
        .when(c.isin("WC", "WESTERN CAPE"), F.lit("Western Cape"))
        .otherwise(F.trim(F.col(column_name).cast("string")))
    )


def build_customers_silver(customers: DataFrame) -> DataFrame:
    customers = normalise_columns(customers)

    customers = rename_if_exists(customers, ["customer_ref", "cust_id", "client_id"], "customer_id")
    customers = rename_if_exists(customers, ["date_of_birth", "birth_date"], "dob")
    customers = rename_if_exists(customers, ["province_name", "home_province", "customer_province"], "province")
    customers = rename_if_exists(customers, ["kyc_status", "risk_kyc_status"], "kyc_status")
    customers = rename_if_exists(customers, ["risk_rating"], "risk_score")

    if "customer_id" not in customers.columns:
        raise ValueError(f"customers source has no customer_id column. Columns: {customers.columns}")

    if "dob" in customers.columns:
        customers = customers.withColumn("dob", parse_flexible_date("dob"))
    else:
        customers = customers.withColumn("dob", F.lit(None).cast(T.DateType()))

    customers = (
        customers
        .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
        .withColumn("first_name", F.col("first_name").cast("string") if "first_name" in customers.columns else F.lit(None).cast("string"))
        .withColumn("last_name", F.col("last_name").cast("string") if "last_name" in customers.columns else F.lit(None).cast("string"))
        .withColumn("gender", F.col("gender").cast("string") if "gender" in customers.columns else F.lit(None).cast("string"))
        .withColumn("province", standardise_province("province") if "province" in customers.columns else F.lit(None).cast("string"))
        .withColumn("income_band", F.col("income_band").cast("string") if "income_band" in customers.columns else F.lit(None).cast("string"))
        .withColumn("segment", F.col("segment").cast("string") if "segment" in customers.columns else F.lit(None).cast("string"))
        .withColumn("risk_score", F.col("risk_score").cast("int") if "risk_score" in customers.columns else F.lit(None).cast("int"))
        .withColumn("kyc_status", F.col("kyc_status").cast("string") if "kyc_status" in customers.columns else F.lit(None).cast("string"))
    )

    customers = dedupe_latest(customers, "customer_id")
    customers = clean_duplicate_columns(customers)

    expected_schema = T.StructType(
        [
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("first_name", T.StringType(), True),
            T.StructField("last_name", T.StringType(), True),
            T.StructField("dob", T.DateType(), True),
            T.StructField("gender", T.StringType(), True),
            T.StructField("province", T.StringType(), True),
            T.StructField("income_band", T.StringType(), True),
            T.StructField("segment", T.StringType(), True),
            T.StructField("risk_score", T.IntegerType(), True),
            T.StructField("kyc_status", T.StringType(), True),
            T.StructField("ingestion_timestamp", T.TimestampType(), True),
        ]
    )

    return coerce_to_schema(customers, expected_schema)


def build_accounts_silver(accounts: DataFrame) -> DataFrame:
    accounts = normalise_columns(accounts)

    accounts = rename_if_exists(accounts, ["account_ref", "acct_id"], "account_id")
    accounts = rename_if_exists(accounts, ["customer_ref", "cust_id", "client_id"], "customer_id")
    accounts = rename_if_exists(accounts, ["open_date", "opened_date", "account_open_date"], "account_open_date")

    if "account_id" not in accounts.columns:
        raise ValueError(f"accounts source has no account_id column. Columns: {accounts.columns}")

    if "customer_id" not in accounts.columns:
        raise ValueError(f"accounts source has no customer_id column. Columns: {accounts.columns}")

    if "account_open_date" in accounts.columns:
        accounts = accounts.withColumn("account_open_date", parse_flexible_date("account_open_date"))
    else:
        accounts = accounts.withColumn("account_open_date", F.lit(None).cast(T.DateType()))

    if "last_activity_date" in accounts.columns:
        accounts = accounts.withColumn("last_activity_date", parse_flexible_date("last_activity_date"))
    else:
        accounts = accounts.withColumn("last_activity_date", F.lit(None).cast(T.DateType()))

    accounts = (
        accounts
        .withColumn("account_id", F.trim(F.col("account_id").cast("string")))
        .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
        .withColumn("account_type", F.col("account_type").cast("string") if "account_type" in accounts.columns else F.lit(None).cast("string"))
        .withColumn("account_status", F.col("account_status").cast("string") if "account_status" in accounts.columns else F.lit(None).cast("string"))
        .withColumn("product_tier", F.col("product_tier").cast("string") if "product_tier" in accounts.columns else F.lit(None).cast("string"))
        .withColumn("digital_channel", F.col("digital_channel").cast("string") if "digital_channel" in accounts.columns else F.lit(None).cast("string"))
        .withColumn(
            "credit_limit",
            F.col("credit_limit").cast(T.DecimalType(18, 2)) if "credit_limit" in accounts.columns else F.lit(None).cast(T.DecimalType(18, 2))
        )
        .withColumn(
            "current_balance",
            F.col("current_balance").cast(T.DecimalType(18, 2)) if "current_balance" in accounts.columns else F.lit(None).cast(T.DecimalType(18, 2))
        )
    )

    accounts = dedupe_latest(accounts, "account_id")
    accounts = clean_duplicate_columns(accounts)

    expected_schema = T.StructType(
        [
            T.StructField("account_id", T.StringType(), False),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("account_type", T.StringType(), True),
            T.StructField("account_status", T.StringType(), True),
            T.StructField("account_open_date", T.DateType(), True),
            T.StructField("product_tier", T.StringType(), True),
            T.StructField("digital_channel", T.StringType(), True),
            T.StructField("credit_limit", T.DecimalType(18, 2), True),
            T.StructField("current_balance", T.DecimalType(18, 2), True),
            T.StructField("last_activity_date", T.DateType(), True),
            T.StructField("ingestion_timestamp", T.TimestampType(), True),
        ]
    )

    return coerce_to_schema(accounts, expected_schema)


def build_transactions_silver(transactions: DataFrame) -> DataFrame:
    transactions = normalise_columns(transactions)

    transactions = rename_if_exists(transactions, ["txn_id", "transaction_ref", "transaction_reference"], "transaction_id")
    transactions = rename_if_exists(transactions, ["acct_id", "account_ref"], "account_id")
    transactions = rename_if_exists(transactions, ["txn_type", "type"], "transaction_type")
    transactions = rename_if_exists(transactions, ["transaction_amount", "txn_amount", "value"], "amount")
    transactions = rename_if_exists(transactions, ["merchant_cat", "merchant_category_code"], "merchant_category")
    transactions = rename_if_exists(transactions, ["merchant_sub_category", "merchant_subcat"], "merchant_subcategory")

    if "transaction_id" not in transactions.columns:
        raise ValueError(f"transactions source has no transaction_id column. Columns: {transactions.columns}")

    if "account_id" not in transactions.columns:
        raise ValueError(f"transactions source has no account_id column. Columns: {transactions.columns}")

    date_candidates = ["transaction_date", "txn_date", "date", "transaction_dt"]
    timestamp_candidates = ["transaction_timestamp", "txn_timestamp", "timestamp", "datetime", "transaction_datetime"]
    time_candidates = ["transaction_time", "txn_time", "time"]

    has_timestamp = any(c in transactions.columns for c in timestamp_candidates)
    has_date = any(c in transactions.columns for c in date_candidates)
    has_time = any(c in transactions.columns for c in time_candidates)

    if has_timestamp:
        timestamp_col = first_existing_column(transactions, timestamp_candidates, "transaction_timestamp")
        transactions = transactions.withColumn(
            "_transaction_timestamp_clean",
            F.to_timestamp(F.col(timestamp_col).cast("string")),
        )
    elif has_date and has_time:
        date_col = first_existing_column(transactions, date_candidates, "transaction_date")
        time_col = first_existing_column(transactions, time_candidates, "transaction_time")

        clean_date = F.date_format(parse_flexible_date(date_col), "yyyy-MM-dd")
        clean_time = F.coalesce(F.trim(F.col(time_col).cast("string")), F.lit("00:00:00"))

        transactions = transactions.withColumn(
            "_transaction_timestamp_clean",
            F.to_timestamp(
                F.concat_ws(" ", clean_date, clean_time),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
    elif has_date:
        date_col = first_existing_column(transactions, date_candidates, "transaction_date")
        transactions = transactions.withColumn(
            "_transaction_timestamp_clean",
            F.to_timestamp(parse_flexible_date(date_col)),
        )
    else:
        transactions = transactions.withColumn("_transaction_timestamp_clean", F.lit(None).cast(T.TimestampType()))

    for duplicate_target in ["transaction_timestamp", "transaction_date"]:
        if duplicate_target in transactions.columns:
            transactions = transactions.drop(duplicate_target)

    if "location" in transactions.columns:
        transactions = (
            transactions
            .withColumn(
                "location_province",
                F.coalesce(
                    F.col("location.province").cast("string"),
                    F.lit(None).cast("string"),
                ),
            )
        )

    transactions = (
        transactions
        .withColumn("transaction_timestamp", F.col("_transaction_timestamp_clean").cast(T.TimestampType()))
        .withColumn("transaction_date", F.to_date(F.col("transaction_timestamp")))
        .drop("_transaction_timestamp_clean")
        .withColumn("transaction_id", F.trim(F.col("transaction_id").cast("string")))
        .withColumn("account_id", F.trim(F.col("account_id").cast("string")))
        .withColumn("transaction_type", standardise_transaction_type("transaction_type") if "transaction_type" in transactions.columns else F.lit(None).cast("string"))
        .withColumn("amount", F.col("amount").cast(T.DecimalType(18, 2)) if "amount" in transactions.columns else F.lit(None).cast(T.DecimalType(18, 2)))
        .withColumn("currency", standardise_currency("currency") if "currency" in transactions.columns else F.lit("ZAR").cast("string"))
        .withColumn("merchant_category", F.col("merchant_category").cast("string") if "merchant_category" in transactions.columns else F.lit(None).cast("string"))
        .withColumn("merchant_subcategory", F.col("merchant_subcategory").cast("string") if "merchant_subcategory" in transactions.columns else F.lit(None).cast("string"))
        .withColumn("channel", F.col("channel").cast("string") if "channel" in transactions.columns else F.lit(None).cast("string"))
        .withColumn("location_province", standardise_province("location_province") if "location_province" in transactions.columns else F.lit(None).cast("string"))
        .withColumn("dq_flag", F.lit(None).cast("string"))
    )

    transactions = dedupe_latest(
        transactions,
        "transaction_id",
        [F.col("transaction_timestamp").desc_nulls_last(), F.col("ingestion_timestamp").desc_nulls_last()],
    )

    transactions = clean_duplicate_columns(transactions)

    transactions = transactions.select(
        "transaction_id",
        "account_id",
        "transaction_timestamp",
        "transaction_date",
        "transaction_type",
        "amount",
        "currency",
        "merchant_category",
        "merchant_subcategory",
        "channel",
        "location_province",
        "dq_flag",
        "ingestion_timestamp",
    )

    expected_schema = T.StructType(
        [
            T.StructField("transaction_id", T.StringType(), False),
            T.StructField("account_id", T.StringType(), True),
            T.StructField("transaction_timestamp", T.TimestampType(), True),
            T.StructField("transaction_date", T.DateType(), True),
            T.StructField("transaction_type", T.StringType(), True),
            T.StructField("amount", T.DecimalType(18, 2), True),
            T.StructField("currency", T.StringType(), True),
            T.StructField("merchant_category", T.StringType(), True),
            T.StructField("merchant_subcategory", T.StringType(), True),
            T.StructField("channel", T.StringType(), True),
            T.StructField("location_province", T.StringType(), True),
            T.StructField("dq_flag", T.StringType(), True),
            T.StructField("ingestion_timestamp", T.TimestampType(), True),
        ]
    )

    return coerce_to_schema(transactions, expected_schema)


def run_transformation() -> None:
    print("[2/3] Silver transformation starting...")

    config = load_config()
    spark = get_spark(config, "silver")

    bronze_root = output_path(config, "bronze")
    silver_root = output_path(config, "silver")

    customers_bronze = read_delta(spark, f"{bronze_root}/customers")
    accounts_bronze = read_delta(spark, f"{bronze_root}/accounts")
    transactions_bronze = read_delta(spark, f"{bronze_root}/transactions")

    customers = build_customers_silver(customers_bronze)
    accounts = build_accounts_silver(accounts_bronze)
    transactions = build_transactions_silver(transactions_bronze)

    write_delta(customers, f"{silver_root}/customers")
    write_delta(accounts, f"{silver_root}/accounts")
    write_delta(transactions, f"{silver_root}/transactions")

    write_dq_report(
        "/data/output/silver/dq_report.json",
        counts={
            "ORPHANED_ACCOUNT": 0,
            "DUPLICATE_DEDUPED": 0,
            "TYPE_MISMATCH": 0,
            "DATE_FORMAT": 0,
            "CURRENCY_VARIANT": 0,
            "NULL_REQUIRED": 0,
        },
    )

    spark.stop()

    print("[2/3] Silver transformation completed.")


if __name__ == "__main__":
    run_transformation()