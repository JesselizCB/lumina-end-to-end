"""Publish simulated fleet GPS events to Pub/Sub.

Each vehicle sends one JSON event per tick. A share of events can be corrupted
or sent twice to exercise the pipeline's validation and deduplication.
"""

import argparse
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone

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
) -> list[bytes]:
    """Move every vehicle once and return the payloads to publish."""
    messages = []
    for vehicle in fleet:
        _move(vehicle, rng)
        event = _event(vehicle, now, rng)
        payload = (
            _corrupt(event, rng)
            if rng.random() < error_rate
            else json.dumps(event).encode("utf-8")
        )
        messages.append(payload)
        if rng.random() < duplicate_rate:
            messages.append(payload)
    return messages


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--topic", default="gps-vehiculo")
    parser.add_argument("--vehicles", type=int, default=50)
    parser.add_argument("--interval", type=float, default=5.0, help="seconds between ticks")
    parser.add_argument("--duration", type=float, default=180.0, help="total seconds")
    parser.add_argument("--error-rate", type=float, default=0.02)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args(argv)

    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)
    rng = random.Random(args.seed)
    fleet = create_fleet(args.vehicles, rng)
    published = 0
    deadline = time.monotonic() + args.duration
    while time.monotonic() < deadline:
        now = datetime.now(timezone.utc)
        messages = tick(fleet, now, rng, args.error_rate, args.duplicate_rate)
        futures = [publisher.publish(topic_path, message, ts=_iso(now)) for message in messages]
        for future in futures:
            future.result()
        published += len(messages)
        print(f"{_iso(now)} published {len(messages)} (total {published})", flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
