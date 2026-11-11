from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lumina.batch.silver_trips import build_silver_trips, main

BRONZE_SCHEMA = StructType(
    [
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("PULocationID", LongType()),
        StructField("DOLocationID", LongType()),
        StructField("trip_distance", DoubleType()),
        StructField("fare_amount", DoubleType()),
    ]
)
MAP_SCHEMA = StructType(
    [
        StructField("fuente", StringType()),
        StructField("zona_origen", StringType()),
        StructField("zona_id", LongType()),
    ]
)
ZONE_MAP = [("tlc", "10", 1), ("tlc", "20", 2), ("uber", "99", 3)]

PICKUP = datetime(2024, 1, 15, 8, 7)
DROPOFF = datetime(2024, 1, 15, 8, 37)
VALID_TRIP = (PICKUP, DROPOFF, 10, 20, 10.0, 25.0)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def zone_map(spark, rows=None):
    return spark.createDataFrame(ZONE_MAP if rows is None else rows, MAP_SCHEMA)


def run(spark, trips):
    silver, rejected = build_silver_trips(
        spark.createDataFrame(trips, BRONZE_SCHEMA), zone_map(spark), "2024-01"
    )
    return silver.collect(), rejected.collect()


def test_valid_trip_is_normalized_and_keyed(spark):
    silver, rejected = run(spark, [VALID_TRIP])

    assert rejected == []
    assert len(silver) == 1
    row = silver[0]
    assert row.viaje_id
    assert row.inicio_utc == datetime(2024, 1, 15, 13, 7)
    assert row.fin_utc == datetime(2024, 1, 15, 13, 37)
    assert row.ts_15m == datetime(2024, 1, 15, 13, 0)
    assert row.dia == date(2024, 1, 15)
    assert row.zona_origen_id == 1
    assert row.zona_destino_id == 2
    assert row.distancia_km == pytest.approx(16.0934)
    assert row.duracion_min == pytest.approx(30.0)
    assert row.tarifa == 25.0


def test_exact_duplicates_are_kept_once(spark):
    silver, rejected = run(spark, [VALID_TRIP, VALID_TRIP])

    assert len(silver) == 1
    assert rejected == []


@pytest.mark.parametrize(
    "trip, motivo",
    [
        ((None, DROPOFF, 10, 20, 10.0, 25.0), "campo_nulo"),
        ((PICKUP, DROPOFF, None, 20, 10.0, 25.0), "campo_nulo"),
        ((PICKUP, DROPOFF, 10, 20, 0.0, 25.0), "distancia_no_positiva"),
        ((PICKUP, PICKUP, 10, 20, 10.0, 25.0), "fin_no_posterior_a_inicio"),
        ((PICKUP, DROPOFF, 10, 20, 10.0, -1.0), "tarifa_negativa"),
        ((PICKUP, DROPOFF, 10, 20, 100.0, 25.0), "velocidad_fuera_de_rango"),
        ((PICKUP, DROPOFF, 77, 20, 10.0, 25.0), "zona_desconocida"),
        ((PICKUP, DROPOFF, 10, 99, 10.0, 25.0), "zona_desconocida"),
        (
            (datetime(2002, 12, 31, 17, 0), datetime(2002, 12, 31, 17, 30), 10, 20, 10.0, 25.0),
            "fecha_fuera_de_periodo",
        ),
        (
            (datetime(2024, 2, 1, 0, 1), datetime(2024, 2, 1, 0, 31), 10, 20, 10.0, 25.0),
            "fecha_fuera_de_periodo",
        ),
    ],
)
def test_invalid_trip_is_rejected_with_reason(spark, trip, motivo):
    silver, rejected = run(spark, [trip])

    assert silver == []
    assert [row.motivo for row in rejected] == [motivo]


def test_ambiguous_zone_map_fails_instead_of_multiplying_trips(spark):
    ambiguous = [("tlc", "10", 1), ("tlc", "10", 2), ("tlc", "20", 2)]
    bronze = spark.createDataFrame([VALID_TRIP], BRONZE_SCHEMA)

    with pytest.raises(ValueError, match="10"):
        build_silver_trips(bronze, zone_map(spark, ambiguous), "2024-01")


def test_period_must_be_year_and_month(spark):
    bronze = spark.createDataFrame([VALID_TRIP], BRONZE_SCHEMA)

    with pytest.raises(ValueError, match="2024-1"):
        build_silver_trips(bronze, zone_map(spark), "2024-1")


def test_main_reads_bronze_and_writes_silver_and_rejected(spark, tmp_path):
    bronze_path = str(tmp_path / "bronze")
    map_path = str(tmp_path / "map")
    silver_path = str(tmp_path / "silver")
    rejected_path = str(tmp_path / "rejected")
    invalid_trip = (PICKUP, DROPOFF, 10, 20, 0.0, 25.0)
    spark.createDataFrame([VALID_TRIP, invalid_trip], BRONZE_SCHEMA).write.parquet(
        bronze_path
    )
    zone_map(spark).write.parquet(map_path)

    main(
        [
            "--bronze", bronze_path,
            "--zone-map", map_path,
            "--silver", silver_path,
            "--rejected", rejected_path,
            "--periodo", "2024-01",
        ]
    )

    silver = spark.read.parquet(silver_path).collect()
    rejected = spark.read.parquet(rejected_path).collect()
    assert [row.zona_origen_id for row in silver] == [1]
    assert [row.motivo for row in rejected] == ["distancia_no_positiva"]
