"""OpenStreetMap -> walking graph. This module defines NO DAG (.airflowignore).

The graph is built straight from OSM topology: a way is split at every node
it shares with another walkable way, so the OSM node ids ARE the graph's
vertex ids (pgRouting accepts bigint ids). No snapping tolerance, no
pgr_createTopology (removed in pgRouting 4).

Walkable: what a pedestrian can legally use. Motorways are excluded; so is
anything tagged foot=no / access=private unless foot is explicitly allowed.
Major roads are kept: in towns they almost always have sidewalks that OSM
doesn't map as separate ways.
"""

from collections import Counter
from collections.abc import Iterator
from typing import NamedTuple

WALKABLE_HIGHWAYS = frozenset(
    {
        "footway",
        "path",
        "pedestrian",
        "living_street",
        "residential",
        "service",
        "unclassified",
        "tertiary",
        "tertiary_link",
        "secondary",
        "secondary_link",
        "primary",
        "primary_link",
        "trunk",
        "trunk_link",
        "track",
        "steps",
        "cycleway",
        "corridor",
        "road",
        "bridleway",
    }
)
FOOT_ALLOWED = frozenset({"yes", "designated", "permissive", "destination"})
FOOT_DENIED = frozenset({"no", "private", "use_sidepath"})
ACCESS_DENIED = frozenset({"no", "private"})


class Way(NamedTuple):
    osm_id: int
    highway: str
    nodes: list[tuple[int, float, float]]  # (node id, lon, lat)


class Segment(NamedTuple):
    osm_id: int
    source: int
    target: int
    highway: str
    coords: list[tuple[float, float]]  # (lon, lat)


def is_walkable(tags: dict[str, str]) -> bool:
    highway = tags.get("highway")
    if highway not in WALKABLE_HIGHWAYS:
        return False
    foot = tags.get("foot")
    if foot in FOOT_DENIED:
        return False
    return not (tags.get("access") in ACCESS_DENIED and foot not in FOOT_ALLOWED)


def in_bbox(lon: float, lat: float, bbox: tuple[float, float, float, float]) -> bool:
    west, south, east, north = bbox
    return west <= lon <= east and south <= lat <= north


def read_walkable_ways(
    pbf_path: str, bbox: tuple[float, float, float, float]
) -> Iterator[Way]:
    """Walkable ways with at least one node inside bbox (lon/lat, WGS84)."""
    import osmium  # only the graph task needs it (docker/airflow/Dockerfile)

    processor = (
        osmium.FileProcessor(pbf_path, osmium.osm.NODE | osmium.osm.WAY)
        .with_locations()
        .with_filter(osmium.filter.KeyFilter("highway"))
    )
    for obj in processor:
        if not obj.is_way():
            continue
        tags = dict(obj.tags)
        if not is_walkable(tags):
            continue
        nodes = [(n.ref, n.lon, n.lat) for n in obj.nodes if n.location.valid()]
        if len(nodes) >= 2 and any(in_bbox(lon, lat, bbox) for _, lon, lat in nodes):
            yield Way(obj.id, tags["highway"], nodes)


def split_at_junctions(ways: list[Way]) -> Iterator[Segment]:
    """Split each way at the nodes it shares with another way (or itself)."""
    usage = Counter(ref for way in ways for ref, _, _ in way.nodes)
    for way in ways:
        nodes = way.nodes
        start = 0
        for i in range(1, len(nodes)):
            if i == len(nodes) - 1 or usage[nodes[i][0]] > 1:
                part = nodes[start : i + 1]
                if part[0][0] != part[-1][0] or len(part) > 2:  # skip degenerate loops
                    yield Segment(
                        way.osm_id,
                        part[0][0],
                        part[-1][0],
                        way.highway,
                        [(lon, lat) for _, lon, lat in part],
                    )
                start = i


def wkt_linestring(coords: list[tuple[float, float]]) -> str:
    return "LINESTRING(" + ",".join(f"{lon:.7f} {lat:.7f}" for lon, lat in coords) + ")"
