"""Streaming GPS pipeline: Pub/Sub to Silver in BigQuery, invalid events to the DLQ topic.

Each event is validated, deduplicated by ``evento_id`` and keyed with
``zona_id`` and ``ts_15m`` before it reaches ``silver.gps_vehiculo``.

This file has no project imports so Dataflow can run it as a single script.
"""

import argparse
import csv
import json
import logging
from datetime import datetime, timezone
from typing import Iterable

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.deduplicate import DeduplicatePerKey
from apache_beam.utils.timestamp import Duration

VALID = "validos"
ERRORS = "errores"
REQUIRED_FIELDS = ("evento_id", "vehiculo_id", "lat", "lon", "velocidad", "ts")
NUMERIC_FIELDS = ("lat", "lon", "velocidad")
# Replayed events carry simulated timestamps, so latency is measured on tiempo_real only.
MODES = ("tiempo_real", "reproduccion")
MAX_SPEED_KMH = 150.0
BUCKET_SECONDS = 15 * 60
# How long an evento_id is remembered; the first copy is emitted immediately.
DEDUP_MEMORY_SECONDS = 10 * 60


class InvalidEvent(Exception):
    def __init__(self, motivo: str):
        super().__init__(motivo)
        self.motivo = motivo


def load_zones(path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as handle:
        return [
            {
                "zona_id": int(row["zona_id"]),
                "min_lon": float(row["min_lon"]),
                "min_lat": float(row["min_lat"]),
                "max_lon": float(row["max_lon"]),
                "max_lat": float(row["max_lat"]),
            }
            for row in csv.DictReader(handle)
        ]


def find_zone(zones: list[dict], lat: float, lon: float) -> int | None:
    for zone in zones:
        if (
            zone["min_lon"] <= lon < zone["max_lon"]
            and zone["min_lat"] <= lat < zone["max_lat"]
        ):
            return zone["zona_id"]
    return None


def _parse_ts(value) -> datetime:
    if not isinstance(value, str):
        raise InvalidEvent("ts_invalido")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise InvalidEvent("ts_invalido") from error
    if parsed.tzinfo is None:
        raise InvalidEvent("ts_invalido")
    return parsed.astimezone(timezone.utc)


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def enrich(message: bytes, zones: list[dict]) -> dict:
    """Validate one raw message and return the Silver row; raise InvalidEvent otherwise."""
    try:
        data = json.loads(message.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise InvalidEvent("json_invalido") from error
    if not isinstance(data, dict):
        raise InvalidEvent("json_invalido")
    if any(data.get(field) is None for field in REQUIRED_FIELDS):
        raise InvalidEvent("campo_faltante")
    if not all(_is_number(data[field]) for field in NUMERIC_FIELDS):
        raise InvalidEvent("tipo_invalido")

    lat, lon, speed = float(data["lat"]), float(data["lon"]), float(data["velocidad"])
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise InvalidEvent("coordenada_invalida")
    if not 0 <= speed <= MAX_SPEED_KMH:
        raise InvalidEvent("velocidad_fuera_de_rango")
    ts = _parse_ts(data["ts"])
    zone = find_zone(zones, lat, lon)
    if zone is None:
        raise InvalidEvent("zona_desconocida")
    mode = data.get("modo", "tiempo_real")
    if mode not in MODES:
        raise InvalidEvent("modo_invalido")

    bucket = datetime.fromtimestamp(
        int(ts.timestamp()) // BUCKET_SECONDS * BUCKET_SECONDS, tz=timezone.utc
    )
    return {
        "evento_id": str(data["evento_id"]),
        "vehiculo_id": str(data["vehiculo_id"]),
        "lat": lat,
        "lon": lon,
        "velocidad_kmh": speed,
        "ts": ts.isoformat(),
        "ts_15m": bucket.isoformat(),
        "dia": ts.date().isoformat(),
        "zona_id": zone,
        "modo": mode,
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ValidateGps(beam.DoFn):
    def __init__(self, zones: list[dict]):
        self.zones = zones

    def process(self, message: bytes) -> Iterable:
        try:
            yield enrich(message, self.zones)
        except InvalidEvent as error:
            yield beam.pvalue.TaggedOutput(
                ERRORS,
                {
                    "motivo": error.motivo,
                    "mensaje_original": message.decode("utf-8", errors="replace"),
                    "detectado_en": _now(),
                },
            )


def _key_by_event(row: dict) -> tuple[str, dict]:
    return row["evento_id"], row


def _stamp_processing_time(row: dict) -> dict:
    return {**row, "procesado_en": _now()}


class ProcessGps(beam.PTransform):
    """Raw Pub/Sub payloads in; ``{VALID: rows, ERRORS: error records}`` out."""

    def __init__(self, zones: list[dict], dedup_memory_seconds: int = DEDUP_MEMORY_SECONDS):
        super().__init__()
        self.zones = zones
        self.dedup_memory_seconds = dedup_memory_seconds

    def expand(self, messages):
        results = messages | "Validate" >> beam.ParDo(ValidateGps(self.zones)).with_outputs(
            ERRORS, main=VALID
        )
        valid = (
            results[VALID]
            | "KeyByEvent" >> beam.Map(_key_by_event)
            | "DropDuplicates" >> DeduplicatePerKey(
                processing_time_duration=Duration(seconds=self.dedup_memory_seconds)
            )
            | "DropKey" >> beam.Values()
            | "StampProcessingTime" >> beam.Map(_stamp_processing_time)
        )
        return {VALID: valid, ERRORS: results[ERRORS]}


def _to_json_bytes(record: dict) -> bytes:
    return json.dumps(record, ensure_ascii=False).encode("utf-8")


def run(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--output_table", required=True, help="PROJECT:DATASET.TABLE")
    parser.add_argument("--error_topic", required=True)
    parser.add_argument("--zones", required=True, help="local CSV with zone bounds")
    args, pipeline_args = parser.parse_known_args(argv)

    zones = load_zones(args.zones)
    options = PipelineOptions(pipeline_args, streaming=True, save_main_session=True)
    with beam.Pipeline(options=options) as pipeline:
        outputs = (
            pipeline
            | "ReadGps" >> beam.io.ReadFromPubSub(
                subscription=args.input_subscription, timestamp_attribute="ts"
            )
            | "ProcessGps" >> ProcessGps(zones)
        )
        _ = outputs[VALID] | "WriteSilver" >> beam.io.WriteToBigQuery(
            args.output_table,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
        _ = (
            outputs[ERRORS]
            | "ToJson" >> beam.Map(_to_json_bytes)
            | "WriteDlq" >> beam.io.WriteToPubSub(args.error_topic)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
