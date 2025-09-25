from typing import Union, List, Set

from time import sleep
from traceback import format_exception
from os.path import abspath, expanduser, join, exists
import logging
from datetime import date, datetime, time, timedelta
from dateutil import parser

import pandas as pd

import earthaccess

import colored_logging as cl

import numpy as np

import rasters as rt
from rasters import Raster, RasterGeometry

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

        # Download the granule using earthaccess
        logger.info(f"retrieving Sentinel tile {cl.name(tile)} on {cl.time(date_UTC)}: {directory}")
        file_paths = earthaccess.download(granule, abspath(expanduser(directory)))
        for download_file_path in file_paths:
            if isinstance(download_file_path, Exception):
                raise HLSDownloadFailed("Error when downloading HLS2 files") from download_file_path

        hls_granule = HLS2SentinelGranule(directory)

        return hls_granule

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
        file_paths = earthaccess.download(granule, abspath(expanduser(directory)))
        for download_file_path in file_paths:
            if isinstance(download_file_path, Exception):
                raise HLSDownloadFailed("Error when downloading HLS2 files") from download_file_path

        hls_granule = HLS2LandsatGranule(directory)

        return hls_granule

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
        target_tile = tile
        target_geometry = self.grid(target_tile)
        tile = tile[:5]
        geometry = self.grid(tile)

        try:
            sentinel = self.sentinel(tile=tile, date_UTC=date_UTC)
        except HLSSentinelNotAvailable:
            sentinel = None
        except HLSSentinelMissing as e:
            raise e

        try:
            landsat = self.landsat(tile=tile, date_UTC=date_UTC)
        except HLSLandsatNotAvailable:
            landsat = None
        except HLSLandsatMissing as e:
            raise e

        if sentinel is None and landsat is None:
            raise HLSNotAvailable(f"HLS2 is not available at {tile} on {date_UTC}")
        elif sentinel is not None and landsat is None:
            try:
                NDVI = sentinel.NDVI
            except HLSBandNotAcquired as e:
                logger.error(e)
                raise HLSNotAvailable(f"HLS2 S30 is not available at {tile} on {date_UTC}")
        elif sentinel is None and landsat is not None:
            try:
                NDVI = landsat.NDVI
            except HLSBandNotAcquired as e:
                logger.error(e)
                raise HLSNotAvailable(f"HLS2 L30 is not available at {tile} on {date_UTC}")
        else:
            # Average NDVI from both sensors if available
            NDVI = rt.Raster(np.nanmean(np.dstack([sentinel.NDVI, landsat.NDVI]), axis=2), geometry=sentinel.geometry)

        # Resample NDVI to target resolution if needed
        if self.target_resolution > 30:
            NDVI = NDVI.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            NDVI = NDVI.to_geometry(geometry, resampling="cubic")
        
        return NDVI

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
        target_tile = tile
        target_geometry = self.grid(target_tile)
        tile = tile[:5]
        geometry = self.grid(tile)

        try:
            sentinel = self.sentinel(tile=tile, date_UTC=date_UTC)
        except HLSSentinelNotAvailable:
            sentinel = None
        except HLSSentinelMissing as e:
            raise e

        try:
            landsat = self.landsat(tile=tile, date_UTC=date_UTC)
        except HLSLandsatNotAvailable:
            landsat = None
        except HLSLandsatMissing as e:
            raise e

        if sentinel is None and landsat is None:
            raise HLSNotAvailable(f"HLS2 is not available at {tile} on {date_UTC}")
        elif sentinel is not None and landsat is None:
            try:
                albedo = sentinel.albedo
            except HLSBandNotAcquired:
                raise HLSNotAvailable(f"HLS2 S30 is not available at {tile} on {date_UTC}")
        elif sentinel is None and landsat is not None:
            try:
                albedo = landsat.albedo
            except HLSBandNotAcquired:
                raise HLSNotAvailable(f"HLS2 L30 is not available at {tile} on {date_UTC}")
        else:
            # Average albedo from both sensors if available
            albedo = rt.Raster(np.nanmean(np.dstack([sentinel.albedo, landsat.albedo]), axis=2),
                               geometry=sentinel.geometry)

        # Resample albedo to target resolution if needed
        if self.target_resolution > 30:
            albedo = albedo.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            albedo = albedo.to_geometry(geometry, resampling="cubic")
        
        return albedo

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

        # If all dates are already listed, return the subset
        if set(date_range(start_UTC, end_UTC)) <= self.dates_listed(tile):
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
            most_recent_listing = listing[listing.landsat.apply(lambda landsat: not (landsat == "missing" or (isinstance(granule, float) and np.isnan(granule))))]

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
        target_tile = tile
        target_geometry = self.grid(target_tile)
        tile = tile[:5]

        if geometry is None:
            geometry = self.grid(tile)

        try:
            sentinel = self.sentinel(tile=tile, date_UTC=date_UTC)
        except HLSSentinelNotAvailable:
            sentinel = None
        except HLSSentinelMissing as e:
            raise e

        try:
            landsat = self.landsat(tile=tile, date_UTC=date_UTC)
        except HLSLandsatNotAvailable:
            landsat = None
        except HLSLandsatMissing as e:
            raise e


        if sentinel is None and landsat is None:
            raise HLSNotAvailable(f"HLS2 is not available at {tile} on {date_UTC}")
        elif sentinel is not None and landsat is None:
            try:
                image = sentinel.product(product)
            except Exception as e:
                raise HLSNotAvailable(f"Sentinel product '{product}' is not available at {tile} on {date_UTC}") from e
        elif sentinel is None and landsat is not None:
            try:
                image = landsat.product(product)
            except Exception as e:
                raise HLSNotAvailable(f"Landsat product '{product}' is not available at {tile} on {date_UTC}") from e
        else:
            # Average NDVI from both sensors if available (for NDVI product)
            try:
                sentinel_data = getattr(sentinel, product)
                landsat_data = getattr(landsat, product)
            except AttributeError as e:
                raise HLSNotAvailable(f"Product '{product}' is not available from both sensors at {tile} on {date_UTC}") from e
            image = rt.Raster(np.nanmean(np.dstack([sentinel_data, landsat_data]), axis=2), geometry=sentinel.geometry)

        # Resample image to target resolution if needed
        if self.target_resolution > 30:
            image = image.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            image = image.to_geometry(geometry, resampling="cubic")

        return image