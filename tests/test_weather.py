from datetime import date

import pandas as pd
import pytest

from warehouse.weather.fetch_weather import (
    ARCHIVE_URL, WeatherPoint, fetch, load_points, service_dates_in_arrivals,
    utc_date_range_for_service_dates,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, params, timeout, headers):
        self.calls.append((url, params))
        return FakeResponse(self.payload)


def _block(temps, rain):
    times = [f"2026-11-13T{h:02d}:00" for h in range(len(temps))]
    return {"hourly": {"time": times, "temperature_2m": temps, "precipitation": rain, "rain": rain}}


POINTS = [WeatherPoint("kota_bharu", "Kota Bharu", 6.1254, 102.2386),
          WeatherPoint("machang", "Machang", 5.7633, 102.2153)]


def test_fetch_one_request_for_all_points_in_utc():
    session = FakeSession([_block([27.0, 26.5], [0.0, 1.2]), _block([25.0, None], [0.4, None])])
    df = fetch(POINTS, date(2026, 11, 13), date(2026, 11, 13), session=session)

    (url, params), = session.calls
    assert url == ARCHIVE_URL
    assert params["timezone"] == "GMT"
    assert params["latitude"] == "6.1254,5.7633"
    assert len(df) == 4
    assert str(df["hour_utc"].dt.tz) == "UTC"
    machang = df[df.point_id == "machang"].reset_index(drop=True)
    assert machang.loc[0, "rain_mm"] == 0.4
    assert pd.isna(machang.loc[1, "temperature_c"])  # archive gap stays null, never invented


def test_fetch_single_point_payload_is_an_object():
    session = FakeSession(_block([27.0], [0.0]))
    df = fetch(POINTS[:1], date(2026, 11, 13), date(2026, 11, 13), session=session)
    assert list(df.point_id) == ["kota_bharu"]


def test_fetch_rejects_wrong_location_count():
    with pytest.raises(ValueError, match="expected 2 locations"):
        fetch(POINTS, date(2026, 11, 13), date(2026, 11, 13), session=FakeSession([_block([1.0], [0.0])]))


def test_date_range_covers_service_day_spill_over():
    assert utc_date_range_for_service_dates([date(2026, 8, 9), date(2026, 8, 12)]) == (
        date(2026, 8, 8), date(2026, 8, 13))


def test_service_dates_from_partition_folders(tmp_path):
    for d in ("2026-11-10", "2026-11-09"):
        (tmp_path / f"service_date={d}").mkdir()
    assert service_dates_in_arrivals(tmp_path) == [date(2026, 11, 9), date(2026, 11, 10)]


def test_default_points_config_is_valid():
    points = load_points()
    assert points[0].point_id == "kota_bharu"
    assert all(4.0 < p.latitude < 7.0 and 101.0 < p.longitude < 103.0 for p in points)
