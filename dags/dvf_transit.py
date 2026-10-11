"""Public transport travel times from GTFS. This module defines NO DAG.

pgRouting routes on a static graph; it can't follow timetables. So the
timetable part is computed here, in Python, and stored per stop:

    "leaving from stop S, how many minutes does it take to reach the pole P
     (e.g. Bordeaux centre) by 08:45 on a reference weekday?"

Algorithm: backward Connection Scan (latest departure). Every connection
(one vehicle hop between two consecutive stops) of the reference day is
scanned by decreasing departure time; a stop's value is the latest time you
can be there and still arrive on time, by staying on a vehicle, alighting
and transferring (with a margin), or walking a short footpath between stops.

The result ignores the wait before the first departure: it assumes you time
your arrival at the stop. pgRouting then adds the walk from the door to the
stop, so door-to-pole = walk + (deadline - latest departure).
"""

import csv
import io
import math
import zipfile
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from itertools import pairwise

WALK_SPEED_MS = 1.25  # 4.5 km/h, same as the walking graph
DETOUR = 1.3  # straight line -> street distance
MAX_FOOTPATH_M = 400  # walking between two stops
MAX_ACCESS_M = 600  # walking from the last stop to the pole
TRANSFER_MARGIN_S = 120
GTFS_ROUTE_MODES = {
    0: "tram",
    1: "metro",
    2: "train",
    3: "bus",
    4: "ferry",
    5: "tram",
    6: "cable",
    7: "funiculaire",
    11: "trolleybus",
    12: "monorail",
}


@dataclass(frozen=True, slots=True)
class Stop:
    key: str  # "<network>:<stop_id>"
    name: str
    lat: float
    lon: float


@dataclass(frozen=True, slots=True)
class Connection:
    dep: int  # seconds after midnight (GTFS allows > 24h)
    arr: int
    dep_stop: str
    arr_stop: str
    trip: str


def parse_time(value: str) -> int:
    h, m, s = (int(x) for x in value.strip().split(":"))
    return h * 3600 + m * 60 + s


def _rows(zf: zipfile.ZipFile, name: str) -> Iterable[dict[str, str]]:
    if name not in zf.namelist():
        return []
    return csv.DictReader(
        io.TextIOWrapper(zf.open(name), encoding="utf-8-sig", newline="")
    )


def services_on(zf: zipfile.ZipFile, day: date) -> set[str]:
    """service_ids running on `day` (calendar.txt + calendar_dates.txt exceptions)."""
    weekday = (
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    )[day.weekday()]
    stamp = day.strftime("%Y%m%d")
    active = {
        r["service_id"]
        for r in _rows(zf, "calendar.txt")
        if r[weekday] == "1" and r["start_date"] <= stamp <= r["end_date"]
    }
    for r in _rows(zf, "calendar_dates.txt"):
        if r["date"] == stamp:
            if r["exception_type"] == "1":
                active.add(r["service_id"])
            elif r["exception_type"] == "2":
                active.discard(r["service_id"])
    return active


def read_stops(
    zf: zipfile.ZipFile, network: str, keep: Callable[[float, float], bool]
) -> dict[str, Stop]:
    """Boarding points (location_type 0 or empty) kept by `keep(lat, lon)`."""
    stops = {}
    for r in _rows(zf, "stops.txt"):
        if r.get("location_type", "") not in ("", "0"):
            continue
        try:
            lat, lon = float(r["stop_lat"]), float(r["stop_lon"])
        except KeyError, ValueError:
            continue
        if keep(lat, lon):
            key = f"{network}:{r['stop_id']}"
            stops[key] = Stop(key, r.get("stop_name", "").strip(), lat, lon)
    return stops


def read_connections(
    zf: zipfile.ZipFile,
    network: str,
    day: date,
    stops: dict[str, Stop],
    window: tuple[int, int],
) -> tuple[list[Connection], dict[str, set[str]]]:
    """Connections of `day` departing in `window`, between kept stops; and the
    modes serving each stop (tram, bus, train...)."""
    services = services_on(zf, day)
    route_mode = {
        r["route_id"]: mode_of(r.get("route_type")) for r in _rows(zf, "routes.txt")
    }
    trips = {
        r["trip_id"]: route_mode.get(r["route_id"], "bus")
        for r in _rows(zf, "trips.txt")
        if r["service_id"] in services
    }
    by_trip: dict[str, list[tuple[int, str, int, int]]] = defaultdict(list)
    for r in _rows(zf, "stop_times.txt"):
        trip = r["trip_id"]
        if trip not in trips:
            continue
        stop = f"{network}:{r['stop_id']}"
        arr = r.get("arrival_time") or r.get("departure_time")
        dep = r.get("departure_time") or r.get("arrival_time")
        if not arr or not dep:
            continue  # untimed intermediate stop
        by_trip[trip].append(
            (int(r["stop_sequence"]), stop, parse_time(arr), parse_time(dep))
        )
    connections, modes = [], defaultdict(set)
    start, end = window
    for trip, calls in by_trip.items():
        calls.sort()
        for (_, a, _, a_dep), (_, b, b_arr, _) in pairwise(calls):
            if a in stops and b in stops and start <= a_dep <= end and b_arr >= a_dep:
                connections.append(Connection(a_dep, b_arr, a, b, f"{network}:{trip}"))
                modes[a].add(trips[trip])
                modes[b].add(trips[trip])
    return connections, modes


def mode_of(route_type: str | None) -> str:
    rt = int(route_type or 3)
    return GTFS_ROUTE_MODES.get(rt, "bus") if rt < 100 else _extended_mode(rt)


def _extended_mode(route_type: int) -> str:
    """Extended GTFS route types (100-1700)."""
    if 100 <= route_type < 200:
        return "train"
    if 400 <= route_type < 500:
        return "metro"
    if 900 <= route_type < 1000:
        return "tram"
    if 1000 <= route_type < 1300:
        return "ferry"
    return "bus"


def distance_m(a: Stop, b_lat: float, b_lon: float) -> float:
    """Equirectangular distance: exact enough below a few km."""
    k = 111_320.0
    dx = (a.lon - b_lon) * k * math.cos(math.radians((a.lat + b_lat) / 2))
    dy = (a.lat - b_lat) * k
    return math.hypot(dx, dy)


def walk_seconds(meters: float) -> int:
    return round(meters * DETOUR / WALK_SPEED_MS)


def footpaths(
    stops: dict[str, Stop], max_m: float = MAX_FOOTPATH_M
) -> dict[str, list[tuple[str, int]]]:
    """Pairs of stops within max_m, with the walking time (grid hashing, O(n))."""
    cell = max_m / 111_320.0  # degrees, generous for longitude
    grid: dict[tuple[int, int], list[Stop]] = defaultdict(list)
    for s in stops.values():
        grid[(int(s.lat / cell), int(s.lon / cell))].append(s)
    paths: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for (gy, gx), members in grid.items():
        neighbours = [
            o
            for dy in (-1, 0, 1)
            for dx in (-1, 0, 1)
            for o in grid.get((gy + dy, gx + dx), [])
        ]
        for s in members:
            for o in neighbours:
                if o.key != s.key:
                    d = distance_m(s, o.lat, o.lon)
                    if d <= max_m:
                        paths[s.key].append((o.key, walk_seconds(d)))
    return paths


def latest_departures(
    connections: list[Connection],
    paths: dict[str, list[tuple[str, int]]],
    target_latest: dict[str, int],
    transfer_s: int = TRANSFER_MARGIN_S,
) -> dict[str, int]:
    """Latest time to be at each stop and still reach the pole on time.

    target_latest: for stops close to the pole, the latest arrival there that
    still lets you walk to the pole by the deadline.
    """
    best: dict[str, int] = dict(target_latest)
    for stop, latest in target_latest.items():
        for other, w in paths.get(stop, []):
            if latest - w > best.get(other, -1):
                best[other] = latest - w
    on_trip: set[str] = set()
    ride: dict[str, int] = {}  # latest departure by boarding a vehicle
    for c in sorted(connections, key=lambda c: c.dep, reverse=True):
        reachable = (
            c.trip in on_trip
            or c.arr <= target_latest.get(c.arr_stop, -1)
            or c.arr + transfer_s <= ride.get(c.arr_stop, -1)
        )
        if not reachable:
            continue
        on_trip.add(c.trip)
        if c.dep > ride.get(c.dep_stop, -1):
            ride[c.dep_stop] = c.dep
            if c.dep > best.get(c.dep_stop, -1):
                best[c.dep_stop] = c.dep
            for other, w in paths.get(c.dep_stop, []):
                t = c.dep - w
                if t > ride.get(other, -1):
                    ride[other] = t
                if t > best.get(other, -1):
                    best[other] = t
    return best


def reference_day(today: date, weeks_ahead: int = 2) -> date:
    """A Tuesday `weeks_ahead` weeks out: an ordinary weekday inside the feeds."""
    day = today + timedelta(weeks=weeks_ahead)
    return day + timedelta(days=(1 - day.weekday()) % 7)


def travel_minutes(
    connections: list[Connection],
    paths: dict[str, list[tuple[str, int]]],
    access_s: dict[str, int],
    deadlines: Iterable[int],
) -> dict[str, tuple[float, float]]:
    """{stop: (best, median)} minutes from the stop to the pole.

    One latest-departure search per arrival deadline: a single deadline would
    count the slack between the vehicle's arrival and the deadline (up to a
    full headway). Over many deadlines, the minimum is the trip itself and
    the median is what a commuter with a fixed arrival time typically spends.
    access_s: walking seconds from each stop near the pole to the pole.
    """
    samples: dict[str, list[float]] = defaultdict(list)
    for deadline in deadlines:
        targets = {stop: deadline - w for stop, w in access_s.items()}
        for stop, latest in latest_departures(connections, paths, targets).items():
            samples[stop].append((deadline - latest) / 60)
    result = {}
    for stop, values in samples.items():
        values.sort()
        result[stop] = (round(values[0], 1), round(values[len(values) // 2], 1))
    return result
