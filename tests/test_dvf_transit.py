import io
import zipfile
from datetime import date

import dvf_transit as tr

DAY = date(2026, 10, 27)  # a Tuesday


def feed() -> zipfile.ZipFile:
    files = {
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon,location_type\n"
        "A,Faubourg,44.80,-0.60,\nB,Pont,44.82,-0.58,0\nC,Centre,44.8425,-0.5745,0\n"
        "B2,Pont (bus),44.8201,-0.5801,0\nX,Station parent,44.82,-0.58,1\n",
        "routes.txt": "route_id,route_type\nT,0\nL,3\n",
        "trips.txt": "route_id,service_id,trip_id\nT,WEEK,t1\nT,WEEK,t2\nL,WEEK,l1\nT,SUN,s1\n",
        "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
        "WEEK,1,1,1,1,1,0,0,20260101,20271231\nSUN,0,0,0,0,0,0,1,20260101,20271231\n",
        "calendar_dates.txt": "service_id,date,exception_type\n",
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        # tram t1: A 08:00 -> B 08:10 -> C 08:20 ; tram t2 one hour later
        "t1,08:00:00,08:00:00,A,1\nt1,08:10:00,08:10:00,B,2\nt1,08:20:00,08:20:00,C,3\n"
        "t2,09:00:00,09:00:00,A,1\nt2,09:20:00,09:20:00,C,2\n"
        # bus l1: A 07:50 -> B2 08:05 (B2 is 15 m from B: transfer to t1 at B? too tight)
        "l1,07:50:00,07:50:00,A,1\nl1,08:05:00,08:05:00,B2,2\n"
        "s1,08:00:00,08:00:00,A,1\ns1,08:05:00,08:05:00,C,2\n",
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text)
    return zipfile.ZipFile(io.BytesIO(buf.getvalue()))


def load():
    zf = feed()
    stops = tr.read_stops(zf, "net", lambda lat, lon: True)
    conns, modes = tr.read_connections(zf, "net", DAY, stops, (6 * 3600, 10 * 3600))
    return stops, conns, modes


def test_services_and_stops():
    zf = feed()
    assert tr.services_on(zf, DAY) == {"WEEK"}
    assert tr.services_on(zf, date(2026, 10, 25)) == {"SUN"}
    stops, conns, modes = load()
    assert "net:X" not in stops  # parent station, not a boarding point
    assert {c.trip for c in conns} == {"net:t1", "net:t2", "net:l1"}  # no Sunday trip
    assert modes["net:B"] == {"tram"} and modes["net:B2"] == {"bus"}


def test_latest_departure_with_transfer_margin():
    stops, conns, _ = load()
    paths = tr.footpaths(stops)
    deadline = 8 * 3600 + 25 * 60
    best = tr.latest_departures(conns, paths, {"net:C": deadline})
    assert best["net:A"] == 8 * 3600  # tram t1 straight through
    assert best["net:B"] == 8 * 3600 + 10 * 60
    # the bus reaches B2 at 08:05, 15 m from B: walk 1 min, then 4 min to the
    # 08:10 tram > 2 min margin -> leaving A by bus at 07:50 also works, but
    # A's best is still the 08:00 tram
    assert best["net:B2"] >= 8 * 3600 + 10 * 60 - 60


def test_travel_minutes_min_and_median():
    stops, conns, _ = load()
    result = tr.travel_minutes(
        conns,
        tr.footpaths(stops),
        {"net:C": 0},
        range(8 * 3600 + 20 * 60, 9 * 3600 + 21 * 60, 20 * 60),
    )
    best, typical = result["net:A"]
    assert best == 20.0  # A 08:00 -> C 08:20, deadline 08:20
    assert typical > best  # deadlines between the two trams include waiting slack


def test_reference_day_is_a_tuesday():
    assert tr.reference_day(date(2026, 10, 11)).weekday() == 1
