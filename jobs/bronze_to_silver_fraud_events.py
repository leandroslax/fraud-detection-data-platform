from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_event_ts(col_event_time: F.Column) -> F.Column:
    s = col_event_time.cast("string")

    digits = F.regexp_extract(s, r"^(\d+)$", 1)
    as_long = F.when(digits != "", digits.cast("long"))

    ts_from_epoch = F.when(
        as_long.isNotNull() & (as_long >= F.lit(10**12)),
        F.to_timestamp((as_long / 1000).cast("double")),
    ).when(
        as_long.isNotNull(),
        F.to_timestamp(as_long.cast("double")),
    )

    ts_br = F.to_timestamp(s, "dd/MM/yyyy HH:mm:ss")
    ts_iso = F.to_timestamp(s)

    return F.coalesce(ts_from_epoch, ts_br, ts_iso)


def clean_amount(col_amount: F.Column) -> F.Column:
    s = col_amount.cast("string")
    s = F.regexp_replace(s, r"R\$\s*", "")
    s = F.regexp_replace(s, r"\s+", "")

    s = F.when(
        s.contains(","),
        F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", "."),
    ).otherwise(s)

    s = F.regexp_replace(s, r"[^0-9\.\-]", "")
    return s.cast("double")


def build_ingest_ts(df):
    cols = set(df.columns)

    if "ingest_time" in cols:
        return parse_event_ts(F.col("ingest_time"))
    if "ingest_timestamp" in cols:
        return F.col("ingest_timestamp").cast("timestamp")
    return F.current_timestamp()


def build_country_iso2(df):
    """
    Normaliza country_iso2 a partir de possíveis colunas:
      - country_iso2
      - country
      - country_code
      - countryCode
      - country_iso
    Retorna ISO2 em MAIÚSCULO, ou null se inválido.
    """
    cols = set(df.columns)

    candidates = []
    for c in ["country_iso2", "country", "country_code", "countryCode", "country_iso"]:
        if c in cols:
            candidates.append(F.col(c).cast("string"))

    if not candidates:
        return F.lit(None).cast("string")

    raw = F.upper(F.trim(F.coalesce(*candidates)))

    # limpa lixo comum
    raw = F.when(
        (raw.isNull())
        | (raw == "")
        | (raw == "NULL")
        | (raw == "UNK")
        | (raw == "UNKNOWN")
        | (raw == "N/A"),
        F.lit(None).cast("string"),
    ).otherwise(raw)

    # se vier "BRASIL"/"BRAZIL" etc, você pode mapear também, mas aqui focamos ISO2
    # garante 2 letras (BR, US, GB, AR...)
    raw = F.when(F.length(raw) == 2, raw).otherwise(F.lit(None).cast("string"))
    return raw


def iso2_to_country_name(col_iso2: F.Column) -> F.Column:
    """
    Converte ISO2 -> Nome do país (inglês), pra QuickSight reconhecer como Location.
    (Se quiser PT-BR depois, dá pra trocar os labels.)
    """
    mapping = F.create_map(
        F.lit("BR"), F.lit("Brazil"),
        F.lit("US"), F.lit("United States"),
        F.lit("GB"), F.lit("United Kingdom"),
        F.lit("AR"), F.lit("Argentina"),
    )
    return mapping.getItem(col_iso2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)  # YYYY-MM-DD
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("fraud-bronze-to-silver").getOrCreate()

    ds = args.ds
    yyyy, mm, dd = ds.split("-")

    bronze_path = (
        f"s3://{args.bucket}/bronze/source=stream/entity=fraud_events/"
        f"year={yyyy}/month={mm}/day={dd}/*.gz"
    )
    silver_path = f"s3://{args.bucket}/silver/fraud_events/"

    # 1) Lê gzip como texto
    raw = spark.read.text(bronze_path)

    # 2) Conserta "JSON colado"
    fixed = raw.select(
        F.regexp_replace(F.col("value"), r"(?<=\})\s*(?=\{)", "\n").alias("value")
    )

    # 3) Explode em linhas de JSON
    json_lines = (
        fixed.select(F.explode(F.split(F.col("value"), "\n")).alias("json"))
        .where(F.length(F.col("json")) > 2)
    )

    # 4) Infer schema (amostra pequena)
    sample_rows = json_lines.limit(500).collect()
    sample = [r["json"] for r in sample_rows] if sample_rows else []
    if not sample:
        raise ValueError(f"Nenhum JSON encontrado em {bronze_path}")

    rdd = spark.sparkContext.parallelize(sample)
    inferred = spark.read.json(rdd).schema

    df = json_lines.select(F.from_json(F.col("json"), inferred).alias("r")).select("r.*")

    cols = set(df.columns)

    # 5) event_ts
    if "event_time" in cols:
        event_ts_col = parse_event_ts(F.col("event_time"))
    elif "event_ts" in cols:
        event_ts_col = F.col("event_ts").cast("timestamp")
    else:
        raise ValueError("Nenhuma coluna de tempo do evento encontrada (event_time/event_ts).")

    # 6) Normalizações + country
    df = (
        df.withColumn("event_ts", event_ts_col)
        .withColumn("ingest_ts", build_ingest_ts(df))
        .withColumn(
            "amount_double",
            clean_amount(F.col("amount")) if "amount" in cols else F.lit(None).cast("double"),
        )
        .withColumn("country_iso2", build_country_iso2(df))
        # campo amigo do QuickSight (Location)
        .withColumn("country_geo", iso2_to_country_name(F.col("country_iso2")))
    )

    # descarta event_ts nulo
    df = df.where(F.col("event_ts").isNotNull())

    # 7) Partição por data do evento
    df = (
        df.withColumn("event_year", F.year("event_ts"))
        .withColumn("event_month", F.format_string("%02d", F.month("event_ts")))
        .withColumn("event_day", F.format_string("%02d", F.dayofmonth("event_ts")))
    )

    # 8) Grava Silver
    (
        df.write.mode("append")
        .partitionBy("event_year", "event_month", "event_day")
        .parquet(silver_path)
    )

    print(f"OK: wrote silver to {silver_path}")


if __name__ == "__main__":
    main()