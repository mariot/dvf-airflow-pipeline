import dvf_osm
from dvf_osm import Way


def test_walkability_rules():
    assert dvf_osm.is_walkable({"highway": "footway"})
    assert dvf_osm.is_walkable({"highway": "primary"})  # sidewalks rarely mapped apart
    assert not dvf_osm.is_walkable({"highway": "motorway"})
    assert not dvf_osm.is_walkable({"highway": "residential", "foot": "no"})
    assert not dvf_osm.is_walkable({"highway": "service", "access": "private"})
    assert dvf_osm.is_walkable(
        {"highway": "service", "access": "private", "foot": "yes"}
    )


def test_split_at_junctions_uses_osm_nodes_as_vertices():
    # a: 1-2-3-4, b: 5-3-6 -> a splits at 3 (shared), b too
    a = Way(
        10, "residential", [(1, 0.0, 0.0), (2, 0.1, 0.0), (3, 0.2, 0.0), (4, 0.3, 0.0)]
    )
    b = Way(11, "footway", [(5, 0.2, -0.1), (3, 0.2, 0.0), (6, 0.2, 0.1)])
    segments = {
        (s.osm_id, s.source, s.target, len(s.coords))
        for s in dvf_osm.split_at_junctions([a, b])
    }
    assert segments == {(10, 1, 3, 3), (10, 3, 4, 2), (11, 5, 3, 2), (11, 3, 6, 2)}


def test_wkt():
    assert dvf_osm.wkt_linestring([(-0.5, 44.8), (-0.4, 44.9)]) == (
        "LINESTRING(-0.5000000 44.8000000,-0.4000000 44.9000000)"
    )
