"""Publish simulated fleet GPS events to Pub/Sub.

Each vehicle sends one JSON event per tick. A share of events can be corrupted
or sent twice to exercise the pipeline's validation and deduplication.

With --replay-start and --replay-end the events carry simulated times instead of
the current time, to cover other slots of the day; they are marked
``modo: reproduccion``.
"""

import argparse
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator

# Provisional DataLandia bounds; they match seeds/zonas.csv.
MIN_LON, MIN_LAT, MAX_LON, MAX_LAT = -74.05, 40.55, -73.75, 40.90
MARGIN = 0.001
STEP_DEGREES = 0.002
MAX_SIMULATED_SPEED = 60.0


@dataclass
class Vehicle:
    vehiculo_id: str
    lat: float
    lon: float


def _clamp(value: float, low: float, high: float) -> float:
    return min(max(value, low + MARGIN), high - MARGIN)


def create_fleet(size: int, rng: random.Random) -> list[Vehicle]:
    return [
        Vehicle(
            vehiculo_id=f"v{number:03d}",
            lat=rng.uniform(MIN_LAT + MARGIN, MAX_LAT - MARGIN),
            lon=rng.uniform(MIN_LON + MARGIN, MAX_LON - MARGIN),
        )
        for number in range(1, size + 1)
    ]


def _move(vehicle: Vehicle, rng: random.Random) -> None:
    vehicle.lat = _clamp(vehicle.lat + rng.uniform(-STEP_DEGREES, STEP_DEGREES), MIN_LAT, MAX_LAT)
    vehicle.lon = _clamp(vehicle.lon + rng.uniform(-STEP_DEGREES, STEP_DEGREES), MIN_LON, MAX_LON)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _event(vehicle: Vehicle, now: datetime, rng: random.Random) -> dict:
    return {
        "evento_id": f"{vehicle.vehiculo_id}-{int(now.timestamp() * 1000)}",
        "vehiculo_id": vehicle.vehiculo_id,
        "lat": round(vehicle.lat, 6),
        "lon": round(vehicle.lon, 6),
        "velocidad": round(rng.uniform(0, MAX_SIMULATED_SPEED), 1),
        "ts": _iso(now),
    }


def _corrupt(event: dict, rng: random.Random) -> bytes:
    kind = rng.choice(["velocidad", "sin_lat", "json", "coordenada"])
    if kind == "json":
        return b"{not json"
    broken = dict(event)
    if kind == "velocidad":
        broken["velocidad"] = 999.0
    elif kind == "sin_lat":
        del broken["lat"]
    else:
        broken["lat"] = 200.0
    return json.dumps(broken).encode("utf-8")


def tick(
    fleet: list[Vehicle],
    now: datetime,
    rng: random.Random,
    error_rate: float,
    duplicate_rate: float,
    modo: str | None = None,
) -> list[bytes]:
    """Move every vehicle once and return the payloads to publish."""
    messages = []
    for vehicle in fleet:
        _move(vehicle, rng)
        event = _event(vehicle, now, rng)
        if modo:
            event["modo"] = modo
        payload = (
            _corrupt(event, rng)
            if rng.random() < error_rate
            else json.dumps(event).encode("utf-8")
        )
        messages.append(payload)
        if rng.random() < duplicate_rate:
            messages.append(payload)
    return messages


def replay_clock(start: datetime, end: datetime, step_seconds: float) -> Iterator[datetime]:
    """Simulated moments from start (inclusive) to end (exclusive)."""
    if end <= start:
        raise ValueError("replay end must be after replay start")
    moment = start
    while moment < end:
        yield moment
        moment += timedelta(seconds=step_seconds)


def _real_clock(duration: float, interval: float) -> Iterator[datetime]:
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        yield datetime.now(timezone.utc)
        time.sleep(interval)


def _paced(moments: Iterator[datetime], interval: float) -> Iterator[datetime]:
    for moment in moments:
        yield moment
        time.sleep(interval)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--topic", default="gps-vehiculo")
    parser.add_argument("--vehicles", type=int, default=50)
    parser.add_argument("--interval", type=float, default=5.0, help="real seconds between ticks")
    parser.add_argument("--duration", type=float, default=180.0, help="total seconds in real time")
    parser.add_argument("--error-rate", type=float, default=0.02)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--replay-start", type=datetime.fromisoformat,
                        help="simulated start with offset, e.g. 2026-11-10T06:00:00-05:00")
    parser.add_argument("--replay-end", type=datetime.fromisoformat,
                        help="simulated end with offset (exclusive)")
    parser.add_argument("--replay-step", type=float, default=300.0,
                        help="simulated seconds advanced per tick")
    args = parser.parse_args(argv)
    if (args.replay_start is None) != (args.replay_end is None):
        parser.error("--replay-start and --replay-end go together")

    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)
    rng = random.Random(args.seed)
    fleet = create_fleet(args.vehicles, rng)
    if args.replay_start:
        clock = _paced(replay_clock(args.replay_start, args.replay_end, args.replay_step), args.interval)
        modo = "reproduccion"
    else:
        clock = _real_clock(args.duration, args.interval)
        modo = None

    published = 0
    for now in clock:
        messages = tick(fleet, now, rng, args.error_rate, args.duplicate_rate, modo=modo)
        futures = [publisher.publish(topic_path, message, ts=_iso(now)) for message in messages]
        for future in futures:
            future.result()
        published += len(messages)
        print(f"{_iso(now)} published {len(messages)} (total {published})", flush=True)


if __name__ == "__main__":
    main()
