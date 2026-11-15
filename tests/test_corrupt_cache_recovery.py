import os
from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import rasterio
from rasterio.errors import RasterioIOError
from rasterio.transform import from_origin
import rasters as rt

from harmonized_landsat_sentinel.HLS2_connection import HLS2Connection, _validate_geotiff
from harmonized_landsat_sentinel.exceptions import HLSDownloadFailed


def _write_valid_geotiff(path: str, value: float = 0.25, size: int = 32) -> None:
    """Create a small valid single-band GeoTIFF on disk."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = np.full((size, size), value, dtype=np.float32)
    transform = from_origin(500000.0, 4000000.0, 30.0, 30.0)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:32611",
        transform=transform,
        tiled=True,
        blockxsize=16,
        blockysize=16,
    ) as dst:
        dst.write(data, 1)


def _write_truncated_geotiff(path: str) -> None:
    """Create a valid GeoTIFF and truncate the tail so header opens but tile read fails."""
    _write_valid_geotiff(path, value=0.25, size=64)
    file_size = os.path.getsize(path)
    with open(path, "r+b") as f:
        f.truncate(max(16, file_size // 3))


@pytest.fixture
def mock_connection(tmp_path):
    """Instantiate HLS2Connection with mocked Earthdata login and temp download directory."""
    with patch("harmonized_landsat_sentinel.HLS2_connection.login", return_value=MagicMock()):
        conn = HLS2Connection(
            working_directory=str(tmp_path / "work"),
            download_directory=str(tmp_path / "downloads"),
            target_resolution=30,
            retries=2,
            wait_seconds=0.01,
        )
    return conn


def test_validate_geotiff_detects_empty_and_truncated_files(tmp_path):
    empty_tif = str(tmp_path / "empty.tif")
    with open(empty_tif, "wb"):
        pass
    assert _validate_geotiff(empty_tif) is False

    truncated_tif = str(tmp_path / "truncated.tif")
    _write_truncated_geotiff(truncated_tif)
    assert _validate_geotiff(truncated_tif) is False

    valid_tif = str(tmp_path / "valid.tif")
    _write_valid_geotiff(valid_tif)
    assert _validate_geotiff(valid_tif) is True


def test_interrupted_download_leaves_no_partial_file(mock_connection, tmp_path):
    """Simulate a transfer drop mid-download and verify no partial .tif lands at the final path."""
    granule = {"meta": {"native-id": "HLS.L30.T11SPS.2026299T181626.v2.0"}}
    target_dir = mock_connection.landsat_directory(granule, tile="11SPS", date_UTC=date(2026, 10, 26))
    final_tif = os.path.join(target_dir, "HLS.L30.T11SPS.2026299T181626.v2.0.B03.tif")

    def fake_broken_download(_granule, staging_dir):
        staged_tif = os.path.join(staging_dir, "HLS.L30.T11SPS.2026299T181626.v2.0.B03.tif")
        _write_truncated_geotiff(staged_tif)
        return [staged_tif]

    with patch("earthaccess.download", side_effect=fake_broken_download):
        with pytest.raises(HLSDownloadFailed):
            mock_connection._download_granule_atomically(granule, target_dir)

    assert not os.path.exists(final_tif)
    # Ensure no staging directories remain
    leftover_tmp = [p for p in os.listdir(target_dir) if ".tmp" in p]
    assert leftover_tmp == []


def test_truncated_cached_file_quarantined_and_redownloaded(mock_connection):
    """
    Replace a cached B03.tif with a truncated file, call albedo(), and verify it is
    detected, deleted, re-downloaded, and produces a valid Raster.
    """
    tile = "11SPS"
    obs_date = date(2026, 10, 26)
    granule_id_str = "HLS.L30.T11SPS.2026299T181626.v2.0"
    granule_meta = {"meta": {"native-id": granule_id_str}}

    granule_dir = mock_connection.landsat_directory(granule_meta, tile=tile, date_UTC=obs_date)
    corrupt_band = os.path.join(granule_dir, f"{granule_id_str}.B03.tif")
    _write_truncated_geotiff(corrupt_band)
    assert os.path.exists(corrupt_band)

    download_calls = []

    def fake_healthy_download(_granule, staging_dir):
        download_calls.append(staging_dir)
        staged_band = os.path.join(staging_dir, f"{granule_id_str}.B03.tif")
        _write_valid_geotiff(staged_band, value=0.20)
        return [staged_band]

    class FakeLandsatGranule:
        def __init__(self, directory):
            self.directory = directory
            self.band_path = os.path.join(directory, f"{granule_id_str}.B03.tif")
            self.geometry = rt.Raster.open(self.band_path).geometry

        @property
        def albedo(self):
            return rt.Raster.open(self.band_path)

    with (
        patch.object(mock_connection, "sentinel_granule", side_effect=Exception("No S30")),
        patch("harmonized_landsat_sentinel.HLS2_connection.HLSSentinelNotAvailable", Exception),
        patch.object(mock_connection, "landsat_granule", return_value=granule_meta),
        patch("earthaccess.download", side_effect=fake_healthy_download),
        patch("harmonized_landsat_sentinel.HLS2_connection.HLS2LandsatGranule", FakeLandsatGranule),
    ):
        result = mock_connection.albedo(tile=tile, date_UTC=obs_date)

    assert len(download_calls) == 1
    assert _validate_geotiff(corrupt_band) is True
    assert isinstance(result, rt.Raster)
    assert np.isclose(np.nanmean(result), 0.20)


def test_multi_sensor_fallback_when_landsat_corrupt(mock_connection):
    """
    Force an unrecoverable read error on L30 when S30 is available on the same date,
    and verify albedo() and NDVI() still return valid S30-derived Rasters.
    """
    tile = "11SPS"
    obs_date = date(2026, 10, 26)
    s30_meta = {"meta": {"native-id": "HLS.S30.T11SPS.2026299T182439.v2.0"}}
    l30_meta = {"meta": {"native-id": "HLS.L30.T11SPS.2026299T181626.v2.0"}}

    s30_dir = mock_connection.sentinel_directory(s30_meta, date_UTC=obs_date)
    s30_tif = os.path.join(s30_dir, "S30_valid.tif")
    _write_valid_geotiff(s30_tif, value=0.42)

    class HealthySentinel:
        def __init__(self, directory):
            self._r = rt.Raster.open(s30_tif)
            self.geometry = self._r.geometry

        @property
        def albedo(self):
            return self._r

        @property
        def NDVI(self):
            return self._r

    class BrokenLandsat:
        def __init__(self, directory):
            self.geometry = None

        @property
        def albedo(self):
            raise RasterioIOError("TIFFFillTile:Read error at row 4294967295")

        @property
        def NDVI(self):
            raise RasterioIOError("TIFFFillTile:Read error at row 4294967295")

    with (
        patch.object(mock_connection, "sentinel_granule", return_value=s30_meta),
        patch.object(mock_connection, "landsat_granule", return_value=l30_meta),
        patch("earthaccess.download", return_value=[]),
        patch("harmonized_landsat_sentinel.HLS2_connection.HLS2SentinelGranule", HealthySentinel),
        patch("harmonized_landsat_sentinel.HLS2_connection.HLS2LandsatGranule", BrokenLandsat),
    ):
        albedo_raster = mock_connection.albedo(tile=tile, date_UTC=obs_date)
        ndvi_raster = mock_connection.NDVI(tile=tile, date_UTC=obs_date)

    assert isinstance(albedo_raster, rt.Raster)
    assert np.isclose(np.nanmean(albedo_raster), 0.42)
    assert isinstance(ndvi_raster, rt.Raster)
    assert np.isclose(np.nanmean(ndvi_raster), 0.42)