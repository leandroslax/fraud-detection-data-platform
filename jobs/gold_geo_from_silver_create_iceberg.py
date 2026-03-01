from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark import SparkFiles

SILVER_DAY_PATHS = [
    "s3://fraud-datalake-dev-use1-139961319000/silver/fraud_events/year=2026/month=02/day=25/",
]

# ✅ Vamos criar esta tabela pelo Spark (GlueCatalog Iceberg)
ICEBERG_TABLE = "glue_catalog.fraud_lake.gold_fraud_events_geo_iceberg"

# ✅ Warehouse / location da tabela Iceberg (limpo, sem Athena criar metadata)
ICEBERG_BASE = "s3://fraud-datalake-dev-use1-139961319000/gold_iceberg/fraud_events_geo/"
ICEBERG_DATA_PATH = ICEBERG_BASE + "data/"

GEO_SCHEMA = StructType([
    StructField("country_name", StringType(), True),
    StructField("country_iso2", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])

@F.udf(GEO_SCHEMA)
def geoip_lookup(ip: str):
    if ip is None or str(ip).strip() == "":
        return (None, None, None, None)
    try:
        import geoip2.database
        mmdb_path = SparkFiles.get("GeoLite2-City.mmdb")
        reader = geoip2.database.Reader(mmdb_path)
        resp = reader.city(ip)
        out = (
            resp.country.name,
            resp.country.iso_code,
            resp.location.latitude,
            resp.location.longitude,
        )
        reader.close()
        return out
    except Exception:
        return (None, None, None, None)

def main():
    spark = __import__("pyspark").sql.SparkSession.builder.getOrCreate()

    # ✅ Cria a tabela via Spark com property NOVA (write.data.path)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
        ds date,
        country_name string,
        country_iso2 string,
        events_total bigint,
        transactions_total bigint,
        approved_total bigint,
        declined_total bigint,
        amount_sum double,
        risk_score_avg double,
        international_total bigint
      )
      USING iceberg
      PARTITIONED BY (days(ds))
      LOCATION '{ICEBERG_BASE}'
      TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.data.path'='{ICEBERG_DATA_PATH}'
      )
    """)

    # ✅ Lê Silver (somente partição existente)
    silver = spark.read.parquet(*SILVER_DAY_PATHS)

    enriched = (
        silver
        .withColumn("ds_date", F.to_date("ds"))
        .withColumn("geo", geoip_lookup(F.col("device_ip")))
        .withColumn("country_name", F.col("geo.country_name"))
        .withColumn("country_iso2", F.col("geo.country_iso2"))
        .drop("geo")
        .filter(F.col("ds_date").isNotNull())
        .filter(F.col("country_iso2").isNotNull())
    )

    agg = (
        enriched
        .groupBy("ds_date", "country_name", "country_iso2")
        .agg(
            F.count(F.lit(1)).alias("transactions_total"),
            F.sum(F.col("approved_int")).alias("approved_total"),
            F.sum(F.when(F.col("approved_int") == 0, 1).otherwise(0)).alias("declined_total"),
            F.sum(F.col("amount_double")).alias("amount_sum"),
            F.avg(F.col("risk_score_int")).alias("risk_score_avg"),
            F.sum(F.col("is_international_int")).alias("international_total"),
        )
        .withColumnRenamed("ds_date", "ds")
        .withColumn("events_total", F.col("transactions_total"))
        .select(
            "ds",
            "country_name",
            "country_iso2",
            "events_total",
            "transactions_total",
            "approved_total",
            "declined_total",
            "amount_sum",
            "risk_score_avg",
            "international_total",
        )
    )

    # ✅ Escreve no Iceberg
    agg.writeTo(ICEBERG_TABLE).append()

if __name__ == "__main__":
    main()