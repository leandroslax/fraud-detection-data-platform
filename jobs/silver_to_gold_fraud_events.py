#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    sum as fsum,
    avg as favg,
    count as fcount,
    when,
    hour,
    date_format,
    desc,
    coalesce,
    from_json,
    to_timestamp,
    lower,
    trim,
)
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


DEFAULT_BUCKET = "fraud-datalake-dev-use1-139961319000"
DEFAULT_SILVER_PREFIX = "silver/fraud_events"
DEFAULT_GOLD_PREFIX = "gold"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True)
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--silver-prefix", default=DEFAULT_SILVER_PREFIX)
    p.add_argument("--gold-prefix", default=DEFAULT_GOLD_PREFIX)
    p.add_argument("--top-n", type=int, default=20)
    return p.parse_args()


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def write_gold(df, path: str):
    df.write.mode("append").partitionBy("event_year", "event_month", "event_day").parquet(path)


def normalize_boolish_to_int(c):
    """
    Normaliza valores boolean-like (true/false/1/0/yes/no...) para 1/0.
    Tudo via STRING para evitar type mismatch no Spark.
    """
    s = lower(trim(c.cast("string")))
    return (
        when(s.isin("true", "1", "yes", "y", "t"), lit(1))
        .when(s.isin("false", "0", "no", "n", "f"), lit(0))
        .otherwise(lit(None))
    )


def normalize_is_international(c):
    # string-safe
    s = lower(trim(c.cast("string")))
    return when(s.isin("true", "1", "yes", "y", "t"), lit(1)).otherwise(lit(0))


def safe_col(df, name: str, dtype: str):
    """
    Retorna df[name] cast(dtype) se existir, senão NULL cast(dtype).
    """
    if name in df.columns:
        return col(name).cast(dtype)
    return lit(None).cast(dtype)


def main():
    args = parse_args()
    ds_dt = datetime.strptime(args.ds, "%Y-%m-%d")
    y = f"{ds_dt.year:04d}"
    m = f"{ds_dt.month:02d}"
    d = f"{ds_dt.day:02d}"

    spark = build_spark("fraud_silver_to_gold_daily")

    silver_day_path = (
        f"s3://{args.bucket}/{args.silver_prefix}/"
        f"event_year={y}/event_month={m}/event_day={d}/"
    )
    gold_root = f"s3://{args.bucket}/{args.gold_prefix}"

    df = spark.read.parquet(silver_day_path)

    # -------------------------
    # 1) event_ts robusto
    # -------------------------
    # Prioridade: event_ts -> event_time -> ingest_ts
    df = df.withColumn(
        "event_ts",
        coalesce(
            to_timestamp(safe_col(df, "event_ts", "string")),
            to_timestamp(safe_col(df, "event_time", "string")),
            to_timestamp(safe_col(df, "ingest_ts", "string")),
        ),
    )

    # se event_ts vier null, evita estourar partição/data
    df = df.where(col("event_ts").isNotNull())

    # -------------------------
    # 2) Merchant parsing robusto (não assume que existe 'merchant')
    # -------------------------
    merchant_schema = StructType(
        [
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("mcc", StringType(), True),
            StructField("merchant_id", StringType(), True),
        ]
    )

    if "merchant" in df.columns:
        merchant_str = col("merchant").cast("string")
        merchant_struct = when(
            merchant_str.isNotNull() & merchant_str.startswith("{"),
            from_json(merchant_str, merchant_schema),
        ).otherwise(lit(None).cast(merchant_schema))
    else:
        # não existe merchant: cria struct nula
        merchant_struct = lit(None).cast(merchant_schema)

    df = df.withColumn("merchant_norm", merchant_struct)

    # -------------------------
    # 3) Normalizações robustas (colunas variáveis do Silver)
    # -------------------------
    approved_raw = coalesce(
        safe_col(df, "approved", "string"),
        safe_col(df, "approved_int", "string"),
    )

    amount_raw = coalesce(
        safe_col(df, "amount_num", "double"),
        safe_col(df, "amount_double", "double"),
        safe_col(df, "amount", "double"),
    )

    risk_raw = coalesce(
        safe_col(df, "risk_score", "double"),
        safe_col(df, "risk_score_int", "double"),
    )

    is_international_raw = coalesce(
        safe_col(df, "is_international", "string"),
        safe_col(df, "is_international_int", "string"),
    )

    channel_raw = coalesce(
        safe_col(df, "channel", "string"),
        lit("UNKNOWN").cast("string"),
    )

    decline_reason_raw = coalesce(
        safe_col(df, "decline_reason", "string"),
        lit("UNKNOWN").cast("string"),
    )

    mcc_raw = coalesce(
        safe_col(df, "mcc", "string"),
        safe_col(df, "merchant_mcc", "string"),
    )

    merchant_id_raw = coalesce(
        col("merchant_norm.merchant_id").cast("string"),
        safe_col(df, "merchant_id", "string"),
    )

    base = (
        df.withColumn("event_year", lit(y))
        .withColumn("event_month", lit(m))
        .withColumn("event_day", lit(d))
        .withColumn("approved_norm", normalize_boolish_to_int(approved_raw))
        .withColumn("is_approved", when(col("approved_norm") == 1, lit(1)).otherwise(lit(0)))
        .withColumn("is_declined", when(col("approved_norm") == 0, lit(1)).otherwise(lit(0)))
        .withColumn(
            "status",
            when(col("approved_norm") == 1, lit("APPROVED"))
            .when(col("approved_norm") == 0, lit("DECLINED"))
            .otherwise(lit("UNKNOWN")),
        )
        .withColumn("amount_num", amount_raw.cast("double"))
        .withColumn("risk_score_num", risk_raw.cast("double"))
        .withColumn("is_international_num", normalize_is_international(is_international_raw))
        .withColumn("event_hour", hour(col("event_ts")))
        .withColumn("day_of_week", date_format(col("event_ts"), "E"))
        .withColumn("channel", channel_raw.cast("string"))
        .withColumn("decline_reason", decline_reason_raw.cast("string"))
        .withColumn("merchant_id_norm", merchant_id_raw.cast("string"))
        .withColumn("merchant_country", col("merchant_norm.country").cast("string"))
        .withColumn("merchant_city", col("merchant_norm.city").cast("string"))
        .withColumn("merchant_mcc", mcc_raw.cast("string"))
    )

    # -------------------------
    # 4) Agregações
    # -------------------------
    kpis = (
        base.groupBy("event_year", "event_month", "event_day")
        .agg(
            fcount(lit(1)).alias("events_total"),
            fsum(col("is_approved")).alias("events_approved"),
            fsum(col("is_declined")).alias("events_declined"),
            (fsum(col("is_declined")) / fcount(lit(1))).alias("decline_rate"),
            fsum(col("amount_num")).alias("amount_sum"),
            favg(col("amount_num")).alias("amount_avg"),
            favg(col("risk_score_num")).alias("risk_score_avg"),
            fsum(col("is_international_num")).alias("international_events"),
        )
    )

    by_region = (
        base.groupBy("event_year", "event_month", "event_day", "merchant_country", "merchant_city")
        .agg(
            fcount(lit(1)).alias("events_total"),
            fsum(col("is_declined")).alias("events_declined"),
            (fsum(col("is_declined")) / fcount(lit(1))).alias("decline_rate"),
            fsum(col("amount_num")).alias("amount_sum"),
            favg(col("risk_score_num")).alias("risk_score_avg"),
        )
    )

    by_channel = (
        base.groupBy("event_year", "event_month", "event_day", "channel")
        .agg(
            fcount(lit(1)).alias("events_total"),
            fsum(col("is_declined")).alias("events_declined"),
            (fsum(col("is_declined")) / fcount(lit(1))).alias("decline_rate"),
            fsum(col("amount_num")).alias("amount_sum"),
            favg(col("risk_score_num")).alias("risk_score_avg"),
        )
    )

    merch_agg = (
        base.groupBy("event_year", "event_month", "event_day", "merchant_id_norm")
        .agg(
            fcount(lit(1)).alias("events_total"),
            fsum(col("is_declined")).alias("events_declined"),
            (fsum(col("is_declined")) / fcount(lit(1))).alias("decline_rate"),
            fsum(col("amount_num")).alias("amount_sum"),
            favg(col("risk_score_num")).alias("risk_score_avg"),
        )
    )

    w = Window.partitionBy("event_year", "event_month", "event_day").orderBy(
        desc("events_declined"), desc("events_total")
    )
    top_merchants = (
        merch_agg.withColumn("rn", row_number().over(w))
        .filter(col("rn") <= lit(int(args.top_n)))
        .drop("rn")
    )

    hourly = (
        base.groupBy("event_year", "event_month", "event_day", "event_hour", "day_of_week")
        .agg(
            fcount(lit(1)).alias("events_total"),
            fsum(col("is_declined")).alias("events_declined"),
            (fsum(col("is_declined")) / fcount(lit(1))).alias("decline_rate"),
            fsum(col("amount_num")).alias("amount_sum"),
            favg(col("risk_score_num")).alias("risk_score_avg"),
        )
    )

    by_decline_reason = (
        base.groupBy("event_year", "event_month", "event_day", "decline_reason")
        .agg(
            fcount(lit(1)).alias("events_total"),
            fsum(col("is_declined")).alias("events_declined"),
            (fsum(col("is_declined")) / fcount(lit(1))).alias("decline_rate"),
        )
    )

    # -------------------------
    # 5) Write Gold
    # -------------------------
    write_gold(kpis, f"{gold_root}/fraud_events_kpis")
    write_gold(by_region, f"{gold_root}/fraud_events_by_region")
    write_gold(by_channel, f"{gold_root}/fraud_events_by_channel")
    write_gold(top_merchants, f"{gold_root}/fraud_top_merchants")
    write_gold(hourly, f"{gold_root}/fraud_events_hourly")
    write_gold(by_decline_reason, f"{gold_root}/fraud_events_by_decline_reason")


if __name__ == "__main__":
    main()