from typing import Union, List, Set, Callable, Any
import os
import glob
import shutil
import tempfile
from time import sleep
from traceback import format_exception
from os.path import abspath, expanduser, join, exists, basename, isdir
import logging
from datetime import date, datetime, time, timedelta
from dateutil import parser

import pandas as pd
import numpy as np
import earthaccess
import colored_logging as cl

import rasters as rt
from rasters import Raster, RasterGeometry

try:
    import rasterio
    from rasterio.errors import RasterioIOError
    from rasterio.windows import Window
except ImportError:
    rasterio = None
    RasterioIOError = OSError
    Window = None

from .login import login
from .constants import *
from .exceptions import *
from .HLS_CMR_query import HLS_CMR_query
from .HLS2_sentinel_granule import HLS2SentinelGranule
from .HLS2_landsat_granule import HLS2LandsatGranule
from .timer import Timer
from .daterange import date_range

__author__ = "Gregory H. Halverson, Evan Davis"

logger = logging.getLogger(__name__)


def granule_id(granule: earthaccess.search.DataGranule):
    """
    Extracts the native granule ID from an earthaccess DataGranule object.
    Args:
        granule (earthaccess.search.DataGranule): The granule object.
    Returns:
        str: The native granule ID.
    """
    return granule["meta"]["native-id"]


def _validate_geotiff(filepath: str, expected_size: int | None = None) -> bool:
    """
    Validate that a GeoTIFF exists, matches expected byte size (if provided),
    and can read an initial raster block without GDAL/rasterio tile errors.
    """
    if not exists(filepath):
        return False

    try:
        actual_size = os.path.getsize(filepath)
    except OSError:
        return False

    if actual_size <= 0:
        logger.warning(f"Cached file is empty (0 bytes): {filepath}")
        return False

    if expected_size is not None and actual_size != expected_size:
        logger.warning(
            f"Cached file size mismatch for {filepath}: got {actual_size} bytes, expected {expected_size}"
        )
        return False

    # Only run raster block verification on GeoTIFFs
    if not filepath.lower().endswith((".tif", ".tiff")):
        return True

    if rasterio is not None:
        try:
            with rasterio.open(filepath) as ds:
                if ds.count < 1 or ds.width <= 0 or ds.height <= 0:
                    return False
                read_width = min(256, ds.width)
                read_height = min(256, ds.height)
                ds.read(1, window=Window(0, 0, read_width, read_height))
            return True
        except Exception as e:
            logger.warning(f"GeoTIFF block integrity check failed for {filepath}: {e}")
            return False
    else:
        try:
            from osgeo import gdal
            gdal.UseExceptions()
            ds = gdal.Open(filepath)
            if ds is None:
                return False
            band = ds.GetRasterBand(1)
            read_width = min(256, ds.RasterXSize)
            read_height = min(256, ds.RasterYSize)
            band.ReadAsArray(0, 0, read_width, read_height)
            ds = None
            return True
        except Exception as e:
            logger.warning(f"GDAL block integrity check failed for {filepath}: {e}")
            return False


class HLS2Connection:
    """
    Main class for interacting with the HLS2 (Harmonized Landsat and Sentinel-2) data service.
    Handles searching, downloading, and processing of HLS2 granules for given tiles and dates.
    """
    URL = CMR_SEARCH_URL

    def __init__(
            self,
            working_directory: str = HLS2_DOWNLOAD_DIRECTORY,
            download_directory: str = HLS2_DOWNLOAD_DIRECTORY,
            target_resolution: int | None = None,
            username: str | None = None,
            password: str | None = None,
            retries: int = DEFAULT_RETRIES,
            wait_seconds: float = DEFAULT_WAIT_SECONDS) -> None:
        """
        Initialize the HLS2Connection object.
        Args:
            working_directory (str): Path for working directory.
            download_directory (str): Path for downloads.
            target_resolution (int): Desired spatial resolution.
            username (str): Earthdata username (optional).
            password (str): Earthdata password (optional).
            retries (int): Number of retry attempts for failed requests.
            wait_seconds (float): Wait time between retries.
        """
        if target_resolution is None:
            target_resolution = DEFAULT_TARGET_RESOLUTION

        if working_directory is None:
            working_directory = abspath(".")

        if download_directory is None:
            download_directory = HLS2_DOWNLOAD_DIRECTORY

        logger.debug(f"HLS 2.0 working directory: {cl.dir(working_directory)}")
        logger.debug(f"HLS 2.0 download directory: {cl.dir(download_directory)}")

        self.auth = login()  # Authenticate with Earthdata
        self.working_directory = working_directory
        self.download_directory = download_directory
        self.target_resolution = target_resolution
        self.tile_grid = None  # Optionally implement if needed
        self.retries = retries
        self.wait_seconds = wait_seconds
        # DataFrames to store listing and granule metadata
        self._listing = pd.DataFrame([], columns=["date_UTC", "tile", "sentinel", "landsat"])
        self._granules = pd.DataFrame([], columns=["ID", "sensor", "tile", "date_UTC", "granule"])
        self.unavailable_dates = {}  # Track unavailable dates for sensors

        self._cleanup_orphaned_temp_files()

    def _cleanup_orphaned_temp_files(self) -> None:
        """
        Remove any orphaned *.tmp* files or staging directories left behind by interrupted runs.
        """
        base_dir = abspath(expanduser(self.download_directory))
        if not exists(base_dir):
            return

        for pattern in ("**/*.tmp", "**/*.tmp.*"):
            for orphan in glob.glob(join(base_dir, pattern), recursive=True):
                try:
                    if isdir(orphan):
                        shutil.rmtree(orphan, ignore_errors=True)
                    else:
                        os.remove(orphan)
                    logger.debug(f"Removed orphaned temporary artifact: {orphan}")
                except OSError as e:
                    logger.debug(f"Could not remove temporary artifact {orphan}: {e}")

    def _purge_corrupt_cached_files(self, directory: str) -> list[str]:
        """
        Scan an existing granule directory for corrupted GeoTIFFs and delete them
        so earthaccess will re-fetch missing assets.
        """
        removed = []
        if not exists(directory):
            return removed

        for entry in os.listdir(directory):
            filepath = join(directory, entry)
            if os.path.isfile(filepath) and filepath.lower().endswith((".tif", ".tiff")):
                if not _validate_geotiff(filepath):
                    logger.warning(f"Quarantining and deleting corrupt cached file: {filepath}")
                    try:
                        os.remove(filepath)
                        removed.append(filepath)
                    except OSError as e:
                        logger.error(f"Failed to delete corrupt cached file {filepath}: {e}")
        return removed

    def _download_granule_atomically(
            self,
            granule: earthaccess.search.DataGranule,
            target_directory: str) -> list[str]:
        """
        Download granule files into a staging directory, verify GeoTIFF integrity,
        and atomically move each file into target_directory via os.replace().
        """
        target_directory = abspath(expanduser(target_directory))
        os.makedirs(target_directory, exist_ok=True)

        # Invalidate any pre-existing corrupt files in target_directory before downloading
        self._purge_corrupt_cached_files(target_directory)

        staging_dir = tempfile.mkdtemp(prefix=".download.tmp.", dir=target_directory)
        final_paths = []

        try:
            # Hard-link or copy already-verified files into staging so earthaccess skips valid bands
            existing_valid = {}
            for fname in os.listdir(target_directory):
                fpath = join(target_directory, fname)
                if os.path.isfile(fpath) and not fname.startswith(".download.tmp"):
                    if _validate_geotiff(fpath):
                        staged_existing = join(staging_dir, fname)
                        try:
                            os.link(fpath, staged_existing)
                        except OSError:
                            shutil.copy2(fpath, staged_existing)
                        existing_valid[fname] = fpath

            downloaded_paths = earthaccess.download(granule, staging_dir)

            for item in downloaded_paths:
                if isinstance(item, Exception):
                    raise HLSDownloadFailed("Error when downloading HLS2 files") from item

                staged_file = str(item)
                fname = basename(staged_file)
                final_file = join(target_directory, fname)

                # If this file was newly downloaded (not one of our pre-validated links), validate it
                if fname not in existing_valid:
                    if not _validate_geotiff(staged_file):
                        raise HLSDownloadFailed(
                            f"Downloaded file failed GeoTIFF integrity check: {staged_file}"
                        )
                    os.replace(staged_file, final_file)

                final_paths.append(final_file)

            return final_paths
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

    def _retrieve_granule_with_retry(
            self,
            granule: earthaccess.search.DataGranule,
            directory: str,
            granule_cls: type):
        """
        Download and instantiate a granule with automatic quarantine and retries on failure.
        """
        max_attempts = max(1, self.retries)
        last_exception = None

        for attempt in range(1, max_attempts + 1):
            try:
                self._download_granule_atomically(granule, directory)
                return granule_cls(directory)
            except Exception as e:
                last_exception = e
                logger.warning(
                    f"Granule retrieval attempt {attempt}/{max_attempts} failed for {directory}: {e}"
                )
                self._purge_corrupt_cached_files(directory)
                if attempt < max_attempts:
                    sleep(self.wait_seconds)

        raise HLSDownloadFailed(
            f"Failed to retrieve valid HLS2 granule after {max_attempts} attempts: {directory}"
        ) from last_exception

    def _read_sensor_product(
            self,
            sensor_name: str,
            tile: str,
            date_UTC: date | str,
            granule_fetcher: Callable[[], Any],
            directory_getter: Callable[[], str],
            reader: Callable[[Any], Raster]) -> Raster:
        """
        Read a product/band from a sensor granule. If a GDAL/rasterio read error occurs
        on a cached GeoTIFF, delete the corrupt files, re-download, and retry.
        """
        max_attempts = max(1, self.retries)

        for attempt in range(1, max_attempts + 1):
            granule_obj = granule_fetcher()
            try:
                return reader(granule_obj)
            except (RasterioIOError, RuntimeError, OSError) as e:
                logger.warning(
                    f"Read error on {sensor_name} granule at {tile} on {date_UTC} "
                    f"(attempt {attempt}/{max_attempts}): {e}"
                )
                try:
                    granule_dir = directory_getter()
                    removed = self._purge_corrupt_cached_files(granule_dir)
                    # If block-level scan didn't isolate the bad band, remove all TIFs in that granule dir to force clean re-fetch
                    if not removed and exists(granule_dir):
                        for f in os.listdir(granule_dir):
                            fp = join(granule_dir, f)
                            if os.path.isfile(fp) and fp.lower().endswith((".tif", ".tiff")):
                                logger.warning(f"Removing unverified GeoTIFF for re-download: {fp}")
                                os.remove(fp)
                except Exception as cleanup_err:
                    logger.warning(f"Error during corrupt file cleanup: {cleanup_err}")

                if attempt >= max_attempts:
                    raise HLSBandNotAcquired(
                        f"Unrecoverable read failure for {sensor_name} at {tile} on {date_UTC}"
                    ) from e
                sleep(self.wait_seconds)

    def grid(self, tile: str, cell_size: float | None = None, buffer: int = 0) -> RasterGeometry:
        """
        Get the grid geometry for a given tile.
        Args:
            tile (str): Tile identifier.
            cell_size (float): Optional cell size for the grid.
            buffer (int): Optional buffer for the grid.
        Returns:
            RasterGeometry: The geometry object for the tile.
        """
        if self.tile_grid is None:
            from sentinel_tiles import SentinelTileGrid
            self.tile_grid = SentinelTileGrid(target_resolution=self.target_resolution)
        return self.tile_grid.grid(tile=tile, cell_size=cell_size, buffer=buffer)

    def mark_date_unavailable(self, sensor: str, tile: str, date_UTC: date | str) -> None:
        """
        Mark a date as unavailable for a given sensor and tile.
        Args:
            sensor (str): Sensor name ('Sentinel' or 'Landsat').
            tile (str): Tile identifier.
            date_UTC (date or str): Date to mark as unavailable.
        """
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()
        date_UTC = date_UTC.strftime("%Y-%m-%d")
        tile = tile[:5]
        if sensor not in self.unavailable_dates:
            self.unavailable_dates[sensor] = {}
        if tile not in self.unavailable_dates[sensor]:
            self.unavailable_dates[sensor][tile] = []
        self.unavailable_dates[sensor][tile].append(date_UTC)

    def check_unavailable_date(self, sensor: str, tile: str, date_UTC: date | str) -> bool:
        """
        Check if a date is marked as unavailable for a given sensor and tile.
        Args:
            sensor (str): Sensor name.
            tile (str): Tile identifier.
            date_UTC (date or str): Date to check.
        Returns:
            bool: True if unavailable, False otherwise.
        """
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()
        date_UTC = date_UTC.strftime("%Y-%m-%d")
        tile = tile[:5]
        if sensor not in self.unavailable_dates:
            return False
        if tile not in self.unavailable_dates[sensor]:
            return False
        if date_UTC not in self.unavailable_dates[sensor][tile]:
            return False
        return True

    def date_directory(self, date_UTC: date | str) -> str:
        """
        Get the directory path for a given date.
        Args:
            date_UTC (date or str): Date for which to get the directory.
        Returns:
            str: Directory path for the date.
        """
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        directory = join(self.download_directory, f"{date_UTC:%Y.%m.%d}")

        return directory

    def sentinel_directory(self, granule: earthaccess.search.DataGranule, date_UTC: date | str) -> str:
        """
        Get the directory path for a Sentinel granule on a given date.
        Args:
            granule (earthaccess.search.DataGranule): The Sentinel granule.
            date_UTC (date or str): Date for the granule.
        Returns:
            str: Directory path for the Sentinel granule.
        """
        date_directory = self.date_directory(date_UTC=date_UTC)
        granule_directory = join(date_directory, granule_id(granule))

        return granule_directory

    def landsat_directory(self, granule: earthaccess.search.DataGranule, tile: str, date_UTC: date | str) -> str:
        """
        Get the directory path for a Landsat granule on a given date.
        Args:
            granule (earthaccess.search.DataGranule): The Landsat granule.
            tile (str): Tile identifier.
            date_UTC (date or str): Date for the granule.
        Returns:
            str: Directory path for the Landsat granule.
        Raises:
            HLSLandsatNotAvailable: If the date is marked unavailable for Landsat.
        """
        if self.check_unavailable_date("Landsat", tile, date_UTC):
            raise HLSLandsatNotAvailable(f"Landsat is not available at tile {cl.place(tile)} on {cl.time(date_UTC)}")

        date_directory = self.date_directory(date_UTC=date_UTC)
        granule_directory = join(date_directory, granule_id(granule))

        return granule_directory

    def sentinel(self, tile: str, date_UTC: date | str) -> HLS2SentinelGranule:
        """
        Download and return a Sentinel granule for a given tile and date.
        Args:
            tile (str): Tile identifier.
            date_UTC (date or str): Date for the granule.
        Returns:
            HLS2SentinelGranule: The downloaded Sentinel granule object.
        Raises:
            HLSDownloadFailed: If download fails.
        """
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        logger.info(f"searching for Sentinel tile {cl.name(tile)} on {cl.time(date_UTC)}")
        granule: earthaccess.search.DataGranule
        granule = self.sentinel_granule(tile=tile, date_UTC=date_UTC)
        directory = self.sentinel_directory(granule, date_UTC=date_UTC)

        logger.info(f"retrieving Sentinel tile {cl.name(tile)} on {cl.time(date_UTC)}: {directory}")
        return self._retrieve_granule_with_retry(granule, directory, HLS2SentinelGranule)

    def landsat(self, tile: str, date_UTC: date | str) -> HLS2LandsatGranule:
        """
        Download and return a Landsat granule for a given tile and date.
        Args:
            tile (str): Tile identifier.
            date_UTC (date or str): Date for the granule.
        Returns:
            HLS2LandsatGranule: The downloaded Landsat granule object.
        Raises:
            HLSDownloadFailed: If download fails.
        """
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        logger.info(f"searching for Landsat tile {cl.name(tile)} on {cl.time(date_UTC)}")
        granule: earthaccess.search.DataGranule
        granule = self.landsat_granule(tile=tile, date_UTC=date_UTC)
        directory = self.landsat_directory(granule, tile=tile, date_UTC=date_UTC)

        logger.info(f"retrieving Landsat tile {cl.name(tile)} on {cl.time(date_UTC)}: {directory}")
        return self._retrieve_granule_with_retry(granule, directory, HLS2LandsatGranule)

    def _compute_composite(
            self,
            tile: str,
            date_UTC: date | str,
            product_name: str,
            sentinel_reader: Callable[[HLS2SentinelGranule], Raster],
            landsat_reader: Callable[[HLS2LandsatGranule], Raster],
            geometry: RasterGeometry | None = None) -> Raster:
        """
        Shared helper for NDVI, albedo, and generic products that safely reads each
        available sensor with retry-on-corruption and falls back gracefully if one
        sensor fails.
        """
        tile = tile[:5]
        if geometry is None:
            geometry = self.grid(tile)

        sentinel_raster = None
        landsat_raster = None
        reference_geometry = None

        # 1. Attempt Sentinel-2 (S30)
        try:
            sentinel_raster = self._read_sensor_product(
                sensor_name="Sentinel",
                tile=tile,
                date_UTC=date_UTC,
                granule_fetcher=lambda: self.sentinel(tile=tile, date_UTC=date_UTC),
                directory_getter=lambda: self.sentinel_directory(
                    self.sentinel_granule(tile=tile, date_UTC=date_UTC), date_UTC=date_UTC
                ),
                reader=sentinel_reader
            )
            reference_geometry = sentinel_raster.geometry
        except HLSSentinelNotAvailable:
            sentinel_raster = None
        except HLSSentinelMissing as e:
            raise e
        except (HLSBandNotAcquired, HLSDownloadFailed, RasterioIOError, RuntimeError) as e:
            logger.warning(
                f"Sentinel {product_name} unavailable/corrupt at {tile} on {date_UTC}; "
                f"attempting fallback to Landsat: {e}"
            )
            sentinel_raster = None

        # 2. Attempt Landsat (L30)
        try:
            landsat_raster = self._read_sensor_product(
                sensor_name="Landsat",
                tile=tile,
                date_UTC=date_UTC,
                granule_fetcher=lambda: self.landsat(tile=tile, date_UTC=date_UTC),
                directory_getter=lambda: self.landsat_directory(
                    self.landsat_granule(tile=tile, date_UTC=date_UTC), tile=tile, date_UTC=date_UTC
                ),
                reader=landsat_reader
            )
            if reference_geometry is None:
                reference_geometry = landsat_raster.geometry
        except HLSLandsatNotAvailable:
            landsat_raster = None
        except HLSLandsatMissing as e:
            raise e
        except (HLSBandNotAcquired, HLSDownloadFailed, RasterioIOError, RuntimeError) as e:
            logger.warning(
                f"Landsat {product_name} unavailable/corrupt at {tile} on {date_UTC}; "
                f"attempting fallback to Sentinel: {e}"
            )
            landsat_raster = None

        # 3. Combine or fallback
        if sentinel_raster is None and landsat_raster is None:
            raise HLSNotAvailable(f"HLS2 {product_name} is not available at {tile} on {date_UTC}")
        elif sentinel_raster is not None and landsat_raster is None:
            result = sentinel_raster
        elif sentinel_raster is None and landsat_raster is not None:
            result = landsat_raster
        else:
            result = rt.Raster(
                np.nanmean(np.dstack([sentinel_raster, landsat_raster]), axis=2),
                geometry=reference_geometry
            )

        # 4. Resample to target resolution if needed
        if self.target_resolution > 30:
            result = result.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            result = result.to_geometry(geometry, resampling="cubic")

        return result

    def NDVI(
            self,
            tile: str,
            date_UTC: date | str) -> Raster:
        """
        Compute the NDVI (Normalized Difference Vegetation Index) for a given tile and date.
        Combines Sentinel and Landsat NDVI if both are available, otherwise uses whichever is available.
        Args:
            tile (str): Tile identifier.
            date_UTC (date or str): Date for the NDVI computation.
        Returns:
            Raster: NDVI raster
        Raises:
            HLSNotAvailable: If neither Sentinel nor Landsat data is available.
        """
        return self._compute_composite(
            tile=tile,
            date_UTC=date_UTC,
            product_name="NDVI",
            sentinel_reader=lambda s: s.NDVI,
            landsat_reader=lambda l: l.NDVI
        )

    def albedo(
            self,
            tile: str,
            date_UTC: date | str) -> Raster:
        """
        Compute the albedo for a given tile and date.
        Combines Sentinel and Landsat albedo if both are available, otherwise uses whichever is available.
        Args:
            tile (str): Tile identifier.
            date_UTC (date or str): Date for the albedo computation.
        Returns:
            Raster: Albedo raster
        Raises:
            HLSNotAvailable: If neither Sentinel nor Landsat data is available.
        """
        return self._compute_composite(
            tile=tile,
            date_UTC=date_UTC,
            product_name="albedo",
            sentinel_reader=lambda s: s.albedo,
            landsat_reader=lambda l: l.albedo
        )

    def search(
            self,
            tile: str | None = None,
            start_UTC: date | datetime | str | None = None,
            end_UTC: date | datetime | str | None = None,
            collections: list[str] | None = None,
            IDs: list[str] | None = None,
            page_size: int = PAGE_SIZE) -> pd.DataFrame:
        """
        Search for HLS granules for a given tile and date range.
        Args:
            tile (str): Tile identifier.
            start_UTC (date, datetime, or str): Start date/time.
            end_UTC (date, datetime, or str): End date/time.
            collections (List[str]): List of collection names to search.
            IDs (List[str]): List of granule IDs to filter.
            page_size (int): Number of results per page.
        Returns:
            pd.DataFrame: DataFrame of found granules.
        Raises:
            HLSServerUnreachable: If the server cannot be reached after retries.
        """
        if isinstance(start_UTC, str):
            start_UTC = parser.parse(start_UTC)

            if start_UTC.time() == time(0, 0, 0):
                start_UTC = start_UTC.date()

        if end_UTC is None:
            end_UTC = start_UTC

        if isinstance(end_UTC, str):
            end_UTC = parser.parse(end_UTC)

            if end_UTC.time() == time(0, 0, 0):
                end_UTC = end_UTC.date()

        if isinstance(start_UTC, datetime):
            start_UTC = datetime.combine(start_UTC, time(0, 0, 0))

        if isinstance(end_UTC, datetime):
            end_UTC = datetime.combine(end_UTC, time(23, 59, 59))

        if collections is None:
            collections = COLLECTIONS

        if IDs is None:
            ID_message = ""
        else:
            ID_message = f" with IDs: {', '.join(IDs)}"

        logger.info(f"searching {', '.join(collections)} at {tile} from {start_UTC} to {end_UTC}{ID_message}")

        attempt_count = 0

        while attempt_count < self.retries:
            attempt_count += 1

            try:
                # Query the HLS CMR for granules
                granules = HLS_CMR_query(
                    tile=tile,
                    start_date=start_UTC,
                    end_date=end_UTC,
                    page_size=page_size
                )
                break
            except Exception as e:
                logger.warning(f"HLS connection attempt {attempt_count} failed")
                logger.warning(format_exception(e))

                if attempt_count < self.retries:
                    sleep(self.wait_seconds)
                    logger.warning(f"re-trying HLS server:")
                    continue
                else:
                    raise HLSServerUnreachable(f"HLS server un-reachable:")

        # Store found granules, avoiding duplicates
        self._granules = pd.concat([self._granules, granules]).drop_duplicates(subset=["ID", "date_UTC"])
        logger.info(f"Currently storing {cl.val(len(self._granules))} DataGranules for HLS2")

        return granules

    def dates_listed(self, tile: str) -> set[date]:
        """
        Get the set of dates for which granules are already listed for a tile.
        Args:
            tile (str): Tile identifier.
        Returns:
            Set[date]: Set of dates with listings.
        """
        return set(self._listing[self._listing.tile == tile].date_UTC.apply(lambda date_UTC: parser.parse(date_UTC).date()))

    def listing(
            self,
            tile: str,
            start_UTC: date | str,
            end_UTC: date | str | None = None,
            page_size: int = PAGE_SIZE) -> pd.DataFrame:
        """
        List available HLS2 granules for a tile and date range, including missing data flags.
        Args:
            tile (str): Tile identifier.
            start_UTC (date or str): Start date.
            end_UTC (date or str): End date (optional, defaults to start_UTC).
            page_size (int): Number of results per page.
        Returns:
            pd.DataFrame: Listing of available granules with missing data flags.
        """
        SENTINEL_REPEAT_DAYS = 5
        LANDSAT_REPEAT_DAYS = 16
        GIVEUP_DAYS = 10

        tile = tile[:5]

        timer = Timer()

        if isinstance(start_UTC, str):
            start_UTC = parser.parse(start_UTC).date()

        if end_UTC is None:
            end_UTC = start_UTC

        if isinstance(end_UTC, str):
            end_UTC = parser.parse(end_UTC).date()

        # If we don't need to check any dates, return
        date_range_set = set(date_range(start_UTC, end_UTC))
        if len(date_range_set) == 0:
            return pd.DataFrame([], columns=["date_UTC", "tile", "sentinel", "landsat"])

        # If all dates are already listed, return the subset
        if date_range_set <= self.dates_listed(tile):
            listing_subset = self._listing[self._listing.tile == tile]
            listing_subset = listing_subset[listing_subset.date_UTC.apply(lambda date_UTC: parser.parse(str(date_UTC)).date() >= start_UTC and parser.parse(str(date_UTC)).date() <= end_UTC)]
            listing_subset = listing_subset.sort_values(by="date_UTC")

            return listing_subset

        logger.info(
            f"started listing available HLS2 granules at tile {cl.place(tile)} from {cl.time(start_UTC)} to {cl.time(end_UTC)}")

        giveup_date = datetime.utcnow().date() - timedelta(days=GIVEUP_DAYS)
        search_start = start_UTC - timedelta(days=max(SENTINEL_REPEAT_DAYS, LANDSAT_REPEAT_DAYS))
        search_end = end_UTC

        # Search for granules in the expanded date range
        granules = self.search(
            tile=tile,
            start_UTC=search_start,
            end_UTC=search_end,
            page_size=page_size
        )

        # Separate Sentinel and Landsat granules
        sentinel_granules = granules[granules.sensor == "S30"][
            ["date_UTC", "tile", "granule"]].rename(columns={"granule": "sentinel"})
        landsat_granules = granules[granules.sensor == "L30"][
            ["date_UTC", "tile", "granule"]].rename(columns={"granule": "landsat"})

        sentinel_dates = set(sentinel_granules.date_UTC)
        landsat_dates = set(landsat_granules.date_UTC)
        
        # Build a DataFrame of all dates in the range
        dates = pd.DataFrame({
            "date_UTC": [
                (start_UTC + timedelta(days=day_offset)).strftime("%Y-%m-%d")
                for day_offset
                in range((end_UTC - start_UTC).days + 1)
            ],
            "tile": tile,
        })

        # Merge granule info with date list
        hls_granules = pd.merge(landsat_granules, sentinel_granules, how="outer")
        listing = pd.merge(dates, hls_granules, how="left")
        date_list = list(listing.date_UTC)

        # Sentinel missing/expected logic
        listing["sentinel_available"] = listing.sentinel.apply(lambda sentinel: not pd.isna(sentinel))

        sentinel_dates_expected = set()

        for d in date_list:
            if d in sentinel_dates:
                sentinel_dates_expected.add(d)

            if (parser.parse(d).date() - timedelta(days=SENTINEL_REPEAT_DAYS)).strftime(
                    "%Y-%m-%d") in sentinel_dates_expected:
                sentinel_dates_expected.add(d)

        listing["sentinel_expected"] = listing.date_UTC.apply(lambda date_UTC: date_UTC in sentinel_dates_expected)

        listing["sentinel_missing"] = listing.apply(
            lambda row: not row.sentinel_available and row.sentinel_expected and parser.parse(
                str(row.date_UTC)) >= parser.parse(str(giveup_date)),
            axis=1
        )

        listing["sentinel"] = listing.apply(lambda row: "missing" if row.sentinel_missing else row.sentinel, axis=1)

        # Landsat missing/expected logic
        listing["landsat_available"] = listing.landsat.apply(lambda landsat: not pd.isna(landsat))

        landsat_dates_expected = set()

        for d in date_list:
            if d in landsat_dates:
                landsat_dates_expected.add(d)

            if (parser.parse(d).date() - timedelta(days=LANDSAT_REPEAT_DAYS)).strftime(
                    "%Y-%m-%d") in landsat_dates_expected:
                landsat_dates_expected.add(d)

        listing["landsat_expected"] = listing.date_UTC.apply(lambda date_UTC: parser.parse(str(date_UTC)).date().strftime("%Y-%m-%d") in landsat_dates_expected)

        listing["landsat_missing"] = listing.apply(
            lambda row: not row.landsat_available and row.landsat_expected and parser.parse(
                str(row.date_UTC)) >= parser.parse(str(giveup_date)),
            axis=1
        )

        listing["landsat"] = listing.apply(lambda row: "missing" if row.landsat_missing else row.landsat, axis=1)
        listing = listing[["date_UTC", "tile", "sentinel", "landsat"]]

        logger.info(
            f"finished listing available HLS2 granules at tile {cl.place(tile)} from {cl.time(start_UTC)} to {cl.time(end_UTC)} ({timer})")

        # Store the listing, avoiding duplicates
        self._listing = pd.concat([self._listing, listing]).drop_duplicates(subset=["date_UTC", "tile"])

        return listing

    def sentinel_granule(self, tile: str, date_UTC: date | str) -> earthaccess.search.DataGranule:
        """
        Get the Sentinel granule for a given tile and date.
        Args:
            tile (str): Tile identifier.
            date_UTC (date or str): Date for the granule.
        Returns:
            earthaccess.search.DataGranule: The Sentinel granule object.
        Raises:
            HLSSentinelNotAvailable: If Sentinel is not available.
            HLSSentinelMissing: If Sentinel is missing on the server.
        """
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        listing = self.listing(tile=tile, start_UTC=date_UTC, end_UTC=date_UTC)
        granule = listing.iloc[-1].sentinel

        if isinstance(granule, float) and np.isnan(granule):
            self.mark_date_unavailable("Sentinel", tile, date_UTC)
            raise HLSSentinelNotAvailable(f"Sentinel is not available at tile {cl.place(tile)} on {cl.time(date_UTC)}")
        elif granule == "missing":
            raise HLSSentinelMissing(
                f"Sentinel is missing on remote server at tile {cl.place(tile)} on {cl.time(date_UTC)}")
        else:
            return granule

    def landsat_granule(self, tile: str, date_UTC: date | str) -> earthaccess.search.DataGranule:
        """
        Get the Landsat granule for a given tile and date.
        Args:
            tile (str): Tile identifier.
            date_UTC (date or str): Date for the granule.
        Returns:
            earthaccess.search.DataGranule: The Landsat granule object.
        Raises:
            HLSLandsatNotAvailable: If Landsat is not available.
            HLSLandsatMissing: If Landsat is missing on the server.
        """
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        listing = self.listing(tile=tile, start_UTC=date_UTC, end_UTC=date_UTC)
        granule = listing.iloc[-1].landsat

        if isinstance(granule, float) and np.isnan(granule):
            self.mark_date_unavailable("Landsat", tile, date_UTC)
            error_string = f"Landsat is not available at tile {cl.place(tile)} on {cl.time(date_UTC)}"
            most_recent_listing = listing[listing.landsat.apply(
                lambda landsat: not (landsat == "missing" or (isinstance(landsat, float) and np.isnan(landsat)))
            )]

            if len(most_recent_listing) > 0:
                most_recent = most_recent_listing.iloc[-1].landsat
                error_string += f" most recent granule: {cl.val(most_recent)}"

            raise HLSLandsatNotAvailable(error_string)
        elif granule == "missing":
            raise HLSLandsatMissing(
                f"Landsat is missing on remote server at tile {cl.place(tile)} on {cl.time(date_UTC)}")
        else:
            return granule

    def product(
            self,
            product: str,
            tile: str,
            date_UTC: date | str,
            geometry: RasterGeometry | None = None) -> Raster:
        """
        Retrieve a specific product (e.g., NDVI, albedo) for a given tile and date.
        Combines Sentinel and Landsat if both are available, otherwise uses whichever is available.
        Args:
            product (str): Product name (e.g., 'NDVI', 'albedo').
            tile (str): Tile identifier.
            date_UTC (date or str): Date for the product.
            geometry (RasterGeometry, optional): Target geometry for resampling.
        Returns:
            Raster: Raster for given data layer or derived variable
        Raises:
            HLSNotAvailable: If neither Sentinel nor Landsat data is available.
        """
        def _read_granule_attr(granule_obj):
            if hasattr(granule_obj, product):
                return getattr(granule_obj, product)
            return granule_obj.product(product)

        return self._compute_composite(
            tile=tile,
            date_UTC=date_UTC,
            product_name=product,
            sentinel_reader=_read_granule_attr,
            landsat_reader=_read_granule_attr,
            geometry=geometry
        )
