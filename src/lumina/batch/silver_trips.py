"""Build silver trips from raw TLC rows in Bronze.

The job expects a Spark session running in UTC because TLC timestamps are
naive New York local times.

References starting with ``bq:`` are BigQuery tables, ``iceberg:`` are Apache
Iceberg tables (format v2 on GCS), and anything else is a Parquet path.
"""

import argparse
import re
from typing import Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

SOURCE = "tlc"
SOURCE_TIMEZONE = "America/New_York"
KM_PER_MILE = 1.60934
MAX_SPEED_KMH = 120.0
BUCKET_SECONDS = 15 * 60
BIGQUERY_PREFIX = "bq:"
ICEBERG_PREFIX = "iceberg:"
PERIOD_PATTERN = re.compile(r"\d{4}-(0[1-9]|1[0-2])")

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
]

SILVER_COLUMNS = [
    "viaje_id",
    "inicio_utc",
    "fin_utc",
    "ts_15m",
    "dia",
    "zona_origen_id",
    "zona_destino_id",
    "distancia_km",
    "duracion_min",
    "tarifa",
]


def _source_zones(zone_map: DataFrame) -> DataFrame:
    zones = zone_map.filter(F.col("fuente") == SOURCE).select(
        F.col("zona_origen").cast("string").alias("zona_origen"), "zona_id"
    )
    ambiguous = (
        zones.groupBy("zona_origen").count().filter(F.col("count") > 1).collect()
    )
    if ambiguous:
        codes = ", ".join(sorted(row.zona_origen for row in ambiguous))
        raise ValueError(f"map_zona_fuente has several zones for: {codes}")
    return zones


def _lookup_zone(df: DataFrame, zones: DataFrame, key: str, target: str) -> DataFrame:
    lookup = zones.select(
        F.col("zona_origen").alias(f"_{target}_key"),
        F.col("zona_id").alias(target),
    )
    return df.join(
        lookup, F.col(key).cast("string") == F.col(f"_{target}_key"), "left"
    ).drop(f"_{target}_key")


def _rejection_reason(periodo: str) -> Column:
    any_null = F.lit(False)
    for column in REQUIRED_COLUMNS:
        any_null = any_null | F.col(column).isNull()

    pickup_month = F.date_format("tpep_pickup_datetime", "yyyy-MM")
    return (
        F.when(any_null, "campo_nulo")
        .when(pickup_month != periodo, "fecha_fuera_de_periodo")
        .when(F.col("trip_distance") <= 0, "distancia_no_positiva")
        .when(F.col("fin_utc") <= F.col("inicio_utc"), "fin_no_posterior_a_inicio")
        .when(F.col("fare_amount") < 0, "tarifa_negativa")
        .when(F.col("_velocidad_kmh") > MAX_SPEED_KMH, "velocidad_fuera_de_rango")
        .when(
            F.col("zona_origen_id").isNull() | F.col("zona_destino_id").isNull(),
            "zona_desconocida",
        )
    )


def build_silver_trips(
    bronze: DataFrame, zone_map: DataFrame, periodo: str
) -> tuple[DataFrame, DataFrame]:
    """Return ``(silver, rejected)``; rejected rows keep Bronze columns plus ``motivo``."""
    if not PERIOD_PATTERN.fullmatch(periodo):
        raise ValueError(f"periodo must be YYYY-MM, got {periodo}")
    zones = _source_zones(zone_map)
    keyed = _lookup_zone(bronze, zones, "PULocationID", "zona_origen_id")
    keyed = _lookup_zone(keyed, zones, "DOLocationID", "zona_destino_id")

    natural_key = F.concat_ws(
        "|", *[F.col(column).cast("string") for column in REQUIRED_COLUMNS]
    )
    start_seconds = F.col("inicio_utc").cast("long")
    enriched = (
        keyed.withColumn(
            "inicio_utc",
            F.to_utc_timestamp("tpep_pickup_datetime", SOURCE_TIMEZONE),
        )
        .withColumn(
            "fin_utc", F.to_utc_timestamp("tpep_dropoff_datetime", SOURCE_TIMEZONE)
        )
        .withColumn("distancia_km", F.col("trip_distance") * KM_PER_MILE)
        .withColumn(
            "duracion_min",
            (F.col("fin_utc").cast("long") - start_seconds) / 60.0,
        )
        .withColumn(
            "_velocidad_kmh", F.col("distancia_km") / (F.col("duracion_min") / 60.0)
        )
        .withColumn(
            "ts_15m",
            F.timestamp_seconds(
                F.floor(start_seconds / BUCKET_SECONDS) * BUCKET_SECONDS
            ),
        )
        .withColumn("dia", F.to_date("inicio_utc"))
        .withColumn("tarifa", F.col("fare_amount"))
        .withColumn("viaje_id", F.sha2(natural_key, 256))
        .withColumn("motivo", _rejection_reason(periodo))
    )

    silver = (
        enriched.filter(F.col("motivo").isNull())
        .dropDuplicates(["viaje_id"])
        .select(*SILVER_COLUMNS)
    )
    rejected = enriched.filter(F.col("motivo").isNotNull()).select(
        *bronze.columns, "motivo"
    )
    return silver, rejected


def _read(spark: SparkSession, reference: str) -> DataFrame:
    if reference.startswith(BIGQUERY_PREFIX):
        return (
            spark.read.format("bigquery")
            .option("table", reference.removeprefix(BIGQUERY_PREFIX))
            .load()
        )
    if reference.startswith(ICEBERG_PREFIX):
        return spark.table(reference.removeprefix(ICEBERG_PREFIX))
    return spark.read.parquet(reference)


def _write_iceberg(
    df: DataFrame, table_name: str, partition_col: str | None = None
) -> None:
    writer = (
        df.writeTo(table_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
    )
    if partition_col:
        writer = writer.partitionedBy(partition_col)
    writer.createOrReplace()


def _write_silver(silver: DataFrame, reference: str, temp_bucket: str | None) -> None:
    if reference.startswith(ICEBERG_PREFIX):
        _write_iceberg(
            silver, reference.removeprefix(ICEBERG_PREFIX), partition_col="dia"
        )
        return
    if reference.startswith(BIGQUERY_PREFIX):
        (
            silver.write.format("bigquery")
            .option("writeMethod", "indirect")
            .option("temporaryGcsBucket", temp_bucket)
            .option("partitionField", "dia")
            .option("partitionType", "DAY")
            .option("clusteredFields", "zona_origen_id")
            .mode("overwrite")
            .save(reference.removeprefix(BIGQUERY_PREFIX))
        )
        return
    silver.write.mode("overwrite").partitionBy("dia").parquet(reference)


def _write_rejected(rejected: DataFrame, reference: str) -> None:
    if reference.startswith(ICEBERG_PREFIX):
        _write_iceberg(rejected, reference.removeprefix(ICEBERG_PREFIX))
        return
    if reference.startswith(BIGQUERY_PREFIX):
        (
            rejected.write.format("bigquery")
            .option("writeMethod", "direct")
            .mode("overwrite")
            .save(reference.removeprefix(BIGQUERY_PREFIX))
        )
        return
    rejected.write.mode("overwrite").parquet(reference)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze", required=True)
    parser.add_argument("--zone-map", required=True)
    parser.add_argument("--silver", required=True)
    parser.add_argument("--rejected", required=True)
    parser.add_argument("--periodo", required=True, help="source month, YYYY-MM")
    parser.add_argument(
        "--temp-bucket", help="GCS bucket for BigQuery loads; required for bq: silver"
    )
    args = parser.parse_args(argv)
    if args.silver.startswith(BIGQUERY_PREFIX) and not args.temp_bucket:
        parser.error("--temp-bucket is required when --silver is a BigQuery table")

    spark = (
        SparkSession.builder.appName("lumina-silver-trips")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    silver, rejected = build_silver_trips(
        _read(spark, args.bronze), _read(spark, args.zone_map), args.periodo
    )
    _write_silver(silver, args.silver, args.temp_bucket)
    _write_rejected(rejected, args.rejected)


if __name__ == "__main__":
    main()
