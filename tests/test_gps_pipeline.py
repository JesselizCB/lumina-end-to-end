import json
import random
from datetime import datetime, timezone
from pathlib import Path

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from lumina.streaming.gps_pipeline import (
    ERRORS,
    VALID,
    InvalidEvent,
    ProcessGps,
    enrich,
    load_zones,
)
from lumina.streaming.gps_simulator import create_fleet, tick

ZONES = load_zones(Path(__file__).parents[1] / "seeds" / "zonas.csv")
NOW = datetime(2026, 11, 11, 16, 7, 30, tzinfo=timezone.utc)


def event(**overrides):
    payload = {
        "evento_id": "v001-1",
        "vehiculo_id": "v001",
        "lat": 40.75,
        "lon": -73.96,
        "velocidad": 32.5,
        "ts": "2026-11-11T16:07:30Z",
    }
    payload.update(overrides)
    return json.dumps(payload).encode("utf-8")


def test_valid_event_gets_zone_and_time_keys():
    row = enrich(event(), ZONES)

    assert row == {
        "evento_id": "v001-1",
        "vehiculo_id": "v001",
        "lat": 40.75,
        "lon": -73.96,
        "velocidad_kmh": 32.5,
        "ts": "2026-11-11T16:07:30+00:00",
        "ts_15m": "2026-11-11T16:00:00+00:00",
        "dia": "2026-11-11",
        "zona_id": 2,
    }


@pytest.mark.parametrize(
    "message, motivo",
    [
        (b"{not json", "json_invalido"),
        (b"[1, 2]", "json_invalido"),
        (event(lat=None), "campo_faltante"),
        (event(velocidad="rapido"), "tipo_invalido"),
        (event(lat=True), "tipo_invalido"),
        (event(lat=200.0), "coordenada_invalida"),
        (event(velocidad=999.0), "velocidad_fuera_de_rango"),
        (event(velocidad=-1.0), "velocidad_fuera_de_rango"),
        (event(ts="ayer"), "ts_invalido"),
        (event(ts="2026-11-11T16:07:30"), "ts_invalido"),
        (event(lat=-33.45, lon=-70.66), "zona_desconocida"),
    ],
)
def test_invalid_event_is_rejected_with_reason(message, motivo):
    with pytest.raises(InvalidEvent) as error:
        enrich(message, ZONES)

    assert error.value.motivo == motivo


def without_processing_time(row):
    return {key: value for key, value in row.items() if key != "procesado_en"}


def test_pipeline_deduplicates_and_routes_errors():
    with TestPipeline() as pipeline:
        outputs = (
            pipeline
            | beam.Create([event(), event(), event(velocidad=999.0)])
            | ProcessGps(ZONES)
        )

        assert_that(
            outputs[VALID] | "Ids" >> beam.Map(lambda row: row["evento_id"]),
            equal_to(["v001-1"]),
            label="OneValidEvent",
        )
        assert_that(
            outputs[VALID] | "HasProcessingTime" >> beam.Map(lambda row: bool(row["procesado_en"])),
            equal_to([True]),
            label="ProcessingTimeStamped",
        )
        assert_that(
            outputs[ERRORS] | "Reasons" >> beam.Map(lambda row: row["motivo"]),
            equal_to(["velocidad_fuera_de_rango"]),
            label="OneError",
        )
        assert_that(
            outputs[ERRORS]
            | "Originals" >> beam.Map(lambda row: json.loads(row["mensaje_original"])["velocidad"]),
            equal_to([999.0]),
            label="ErrorKeepsOriginal",
        )


def test_duplicates_far_apart_in_event_time_are_still_removed():
    with TestPipeline() as pipeline:
        outputs = (
            pipeline
            | beam.Create([(event(), 1_000), (event(), 1_060)])
            | "EventTime" >> beam.MapTuple(
                lambda message, seconds: beam.window.TimestampedValue(message, seconds)
            )
            | ProcessGps(ZONES)
        )

        assert_that(
            outputs[VALID] | "Ids" >> beam.Map(lambda row: row["evento_id"]),
            equal_to(["v001-1"]),
        )


def test_simulated_events_are_valid_and_inside_zones():
    fleet = create_fleet(20, random.Random(1))

    messages = tick(fleet, NOW, random.Random(2), error_rate=0.0, duplicate_rate=0.0)

    assert len(messages) == 20
    rows = [enrich(message, ZONES) for message in messages]
    assert {row["zona_id"] for row in rows} <= {1, 2, 3, 4, 5}
    assert len({row["evento_id"] for row in rows}) == 20


def test_simulator_can_inject_errors():
    fleet = create_fleet(20, random.Random(1))

    messages = tick(fleet, NOW, random.Random(2), error_rate=1.0, duplicate_rate=0.0)

    for message in messages:
        with pytest.raises(InvalidEvent):
            enrich(message, ZONES)


def test_simulator_can_inject_duplicates():
    fleet = create_fleet(5, random.Random(1))

    messages = tick(fleet, NOW, random.Random(2), error_rate=0.0, duplicate_rate=1.0)

    assert len(messages) == 10
    assert len(set(messages)) == 5
