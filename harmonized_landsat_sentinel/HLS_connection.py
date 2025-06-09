import logging
import os
import urllib
from abc import abstractmethod
from datetime import date, datetime, timedelta
from fnmatch import fnmatch
from glob import glob
from os import makedirs, system
from os.path import join, abspath, expanduser, exists, getsize, dirname
from shutil import move
from typing import Union, List
import logging
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from sentinel_tiles import SentinelTileGrid

import colored_logging as cl

import rasters as rt
from rasters import Raster, RasterGeometry, SpatialGeometry

from .HLS_landsat_granule import HLSLandsatGranule
from .HLS_sentinel_granule import HLSSentinelGranule
from .constants import *
from .exceptions import *
from .timer import Timer
from .daterange import date_range

logger = logging.getLogger(__name__)

class HLSConnection:
    logger = logging.getLogger(__name__)

    DEFAULT_WORKING_DIRECTORY = DEFAULT_WORKING_DIRECTORY
    DEFAULT_DOWNLOAD_DIRECTORY = DOWNLOAD_DIRECTORY
    DEFAULT_TARGET_RESOLUTION = DEFAULT_TARGET_RESOLUTION
    DEFAULT_TARGET_RESOLUTION = DEFAULT_TARGET_RESOLUTION

    def __init__(
            self,
            working_directory: str = None,
            download_directory: str = None,
            target_resolution: int = None):
        if target_resolution is None:
            target_resolution = self.DEFAULT_TARGET_RESOLUTION

        if working_directory is None:
            working_directory = self.DEFAULT_WORKING_DIRECTORY

        if download_directory is None:
            download_directory = join(working_directory, self.DEFAULT_DOWNLOAD_DIRECTORY)

        self.working_directory = working_directory
        self.download_directory = download_directory       
        self.target_resolution = target_resolution
        self.tile_grid = SentinelTileGrid(target_resolution=target_resolution)
        self._listings = {}
        self.unavailable_dates = {}
        self.remote = None

    def __repr__(self):
        return f'{self.__class__.__name__}(\n' + \
               f'\tworking_directory="{self.working_directory}",\n' + \
               f'\tdownload_directory="{self.download_directory}",\n' + \
               f'\tremote="{self.remote}"' + \
               '\n)'

    def grid(self, tile: str, cell_size: float = None, buffer=0):
        return self.tile_grid.grid(tile=tile, cell_size=cell_size, buffer=buffer)

    def mark_date_unavailable(self, sensor: str, tile: str, date_UTC: Union[date, str]):
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        date_UTC = date_UTC.strftime("%Y-%m-%d")

        tile = tile[:5]

        if sensor not in self.unavailable_dates:
            self.unavailable_dates[sensor] = {}

        if tile not in self.unavailable_dates[sensor]:
            self.unavailable_dates[sensor][tile] = []

        self.unavailable_dates[sensor][tile].append(date_UTC)

    def check_unavailable_date(self, sensor: str, tile: str, date_UTC: Union[date, str]) -> bool:
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

    def status(self, URL: str) -> int:
        logger.info(f"checking URL: {cl.URL(URL)}")

        try:
            response = requests.head(URL, headers=CONNECTION_CLOSE)
            status = response.status_code
            duration = response.elapsed.total_seconds()
        except Exception as e:
            logger.exception(e)
            raise HLSServerUnreachable(f"unable to connect to URL: {URL}")

        if status in (200, 301):
            logger.info(
                "URL verified with status " + cl.val(200) +
                " in " + cl.time(f"{duration:0.2f}") +
                " seconds: " + cl.URL(URL)
            )
        else:
            logger.warning(
                "URL not available with status " + cl.val(status) +
                " in " + cl.time(f"{duration:0.2f}") +
                " seconds: " + cl.URL(URL)
            )

        return status

    def check_remote(self):
        #FIXME re-try a couple times if you get 503

        logger.info(f"checking URL: {cl.URL(self.remote)}")

        try:
            response = requests.head(self.remote, headers=CONNECTION_CLOSE)
            status = response.status_code
            duration = response.elapsed.total_seconds()
        except Exception as e:
            logger.exception(e)
            raise HLSServerUnreachable(f"unable to connect to URL: {self.remote}")

        if status == 200:
            logger.info(
                "remote verified with status " + cl.val(200) +
                " in " + cl.time(f"{duration:0.2f}") +
                " seconds: " + cl.URL(self.remote))
        else:
            raise IOError(f"status: {status} URL: {self.remote}")

    def HTTP_text(self, URL: str) -> str:
        request = urllib.request.Request(URL)
        response = urllib.request.urlopen(request)
        body = response.read().decode()

        return body

    def HTTP_listing(self, URL: str, pattern: str = None) -> List[str]:
        if URL in self._listings:
            listing = self._listings[URL]
        else:
            text = self.HTTP_text(URL)
            soup = BeautifulSoup(text, 'html.parser')
            links = list(soup.find_all('a', href=True))

            # get directory names from links on http site
            listing = sorted([link['href'].replace('/', '') for link in links])
            self._listings[URL] = listing

        if pattern is not None:
            listing = sorted([
                item
                for item
                in listing
                if fnmatch(item, pattern)
            ])

        return listing

    @abstractmethod
    def sentinel_listing(self, tile: str, year: int) -> pd.DataFrame:
        pass

    @abstractmethod
    def landsat_listing(self, tile: str, year: int) -> pd.DataFrame:
        pass

    def year_listing(self, tile: str, year: int) -> pd.DataFrame:
        sentinel = self.sentinel_listing(tile=tile, year=year)
        landsat = self.landsat_listing(tile=tile, year=year)
        df = pd.merge(sentinel, landsat, how="outer")

        return df

    def listing(self, tile: str, start_UTC: Union[date, str], end_UTC: Union[date, str] = None) -> pd.DataFrame:
        SENTINEL_REPEAT_DAYS = 5
        LANDSAT_REPEAT_DAYS = 16
        GIVEUP_DAYS = 10

        tile = tile[:5]

        timer = Timer()
        logger.info(
            f"started listing available HLS2 granules at tile {cl.place(tile)} from {cl.time(start_UTC)} to {cl.time(end_UTC)}")

        if isinstance(start_UTC, str):
            start_UTC = parser.parse(start_UTC).date()

        if end_UTC is None:
            end_UTC = start_UTC

        if isinstance(end_UTC, str):
            end_UTC = parser.parse(end_UTC).date()

        giveup_date = datetime.utcnow().date() - timedelta(days=GIVEUP_DAYS)
        search_start = start_UTC - timedelta(days=max(SENTINEL_REPEAT_DAYS, LANDSAT_REPEAT_DAYS))
        start_year = search_start.year
        end_year = end_UTC.year
        listing = pd.concat([self.year_listing(tile=tile, year=year) for year in range(start_year, end_year + 1)])
        listing = listing[listing.date_UTC <= str(end_UTC)]
        sentinel_dates = set([timestamp.date() for timestamp in list(listing[~listing.sentinel.isna()].date_UTC)])

        if len(sentinel_dates) > 0:
            # for date_UTC in [dt.date() for dt in rrule(DAILY, dtstart=max(sentinel_dates), until=end)]:
            for date_UTC in date_range(max(sentinel_dates), end_UTC):
                previous_pass = date_UTC - timedelta(SENTINEL_REPEAT_DAYS)

                if previous_pass in sentinel_dates:
                    sentinel_dates.add(date_UTC)

        landsat_dates = set([timestamp.date() for timestamp in list(listing[~listing.landsat.isna()].date_UTC)])

        if len(landsat_dates) > 0:
            for date_UTC in date_range(max(landsat_dates), end_UTC):
                previous_pass = date_UTC - timedelta(LANDSAT_REPEAT_DAYS)

                if previous_pass in landsat_dates:
                    landsat_dates.add(date_UTC)

        listing = listing[listing.date_UTC >= str(start_UTC)]
        listing.date_UTC = listing.date_UTC.apply(
            lambda date_UTC: parser.parse(str(date_UTC)).date().strftime("%Y-%m-%d"))
        dates = pd.DataFrame(
            {"date_UTC": [date_UTC.strftime("%Y-%m-%d") for date_UTC in date_range(start_UTC, end_UTC)], "tile": tile})
        listing = pd.merge(dates, listing, how="left")
        listing.date_UTC = listing.date_UTC.apply(lambda date_UTC: parser.parse(date_UTC).date())
        listing = listing.sort_values(by="date_UTC")
        listing["sentinel_available"] = listing.apply(lambda row: not pd.isna(row.sentinel), axis=1)
        listing["sentinel_expected"] = listing.apply(
            lambda row: parser.parse(str(row.date_UTC)).date() in sentinel_dates, axis=1)

        listing["sentinel_missing"] = listing.apply(
            lambda row: not row.sentinel_available and row.sentinel_expected and row.date_UTC >= giveup_date,
            axis=1
        )

        listing["sentinel"] = listing.apply(lambda row: "missing" if row.sentinel_missing else row.sentinel, axis=1)
        listing["landsat_available"] = listing.apply(lambda row: not pd.isna(row.landsat), axis=1)
        listing["landsat_expected"] = listing.apply(lambda row: parser.parse(str(row.date_UTC)).date() in landsat_dates,
                                                    axis=1)

        listing["landsat_missing"] = listing.apply(
            lambda row: not row.landsat_available and row.landsat_expected and row.date_UTC >= giveup_date,
            axis=1
        )

        listing["landsat"] = listing.apply(lambda row: "missing" if row.landsat_missing else row.landsat, axis=1)
        listing = listing[["date_UTC", "tile", "sentinel", "landsat"]]
        logger.info(
            f"finished listing available HLS2 granules at tile {cl.place(tile)} from {cl.time(start_UTC)} to {cl.time(end_UTC)} ({timer})")

        return listing

    def sentinel_filename(self, tile: str, date_UTC: Union[date, str]):
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        listing = self.listing(tile=tile, start_UTC=(date_UTC - timedelta(days=5)), end_UTC=date_UTC)
        filename = str(listing.iloc[-1].sentinel)

        if filename == "nan":
            # logger.error(listing[["date_UTC", "sentinel"]])
            self.mark_date_unavailable("Sentinel", tile, date_UTC)
            raise HLSSentinelNotAvailable(f"Sentinel is not available at tile {cl.place(tile)} on {cl.time(date_UTC)}")
        elif filename == "missing":
            # logger.error(listing[["date_UTC", "sentinel"]])
            raise HLSSentinelMissing(
                f"Sentinel is missing on remote server at tile {cl.place(tile)} on {cl.time(date_UTC)}")
        else:
            return filename

    def landsat_filename(self, tile: str, date_UTC: Union[date, str]):
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        filename = str(self.listing(tile=tile, start_UTC=date_UTC, end_UTC=date_UTC).iloc[0].landsat)

        if filename == "nan":
            self.mark_date_unavailable("Landsat", tile, date_UTC)
            raise HLSLandsatNotAvailable(f"Landsat is not available at tile {cl.place(tile)} on {cl.time(date_UTC)}")
        elif filename == "missing":
            raise HLSLandsatMissing(
                f"Landsat is missing on remote server at tile {cl.place(tile)} on {cl.time(date_UTC)}")
        else:
            return filename

    def download_file(self, URL: str, filename: str):
        if exists(filename) and getsize(filename) == 0:
            logger.warning(f"removing zero-size corrupted HLS2 file: {filename}")
            os.remove(filename)

        if exists(filename):
            logger.info(f"file already downloaded: {cl.file(filename)}")
            return filename

        logger.info(f"downloading: {cl.URL(URL)} -> {cl.file(filename)}")
        directory = dirname(filename)
        makedirs(directory, exist_ok=True)
        partial_filename = f"{filename}.download"
        command = f'wget -c -O "{partial_filename}" "{URL}"'
        timer = Timer()
        system(command)
        logger.info(f"completed download in {cl.time(timer)} seconds: " + cl.file(filename))

        if not exists(partial_filename):
            raise HLSDownloadFailed(f"unable to download URL: {URL}")
        elif exists(partial_filename) and getsize(partial_filename) == 0:
            logger.warning(f"removing zero-size corrupted HLS2 file: {partial_filename}")
            os.remove(partial_filename)
            raise HLSDownloadFailed(f"unable to download URL: {URL}")

        move(partial_filename, filename)

        if not exists(filename):
            raise HLSDownloadFailed(f"failed to download file: {filename}")

        return filename

    def local_directory(self, date_UTC: Union[date, str]) -> str:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        directory = join(self.download_directory, f"{date_UTC:%Y.%m.%d}")

        return directory

    def local_sentinel_filename(self, tile: str, date_UTC: Union[date, str]) -> str:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        if self.check_unavailable_date("Sentinel", tile, date_UTC):
            raise HLSSentinelNotAvailable(f"Sentinel is not available at tile {cl.place(tile)} on {cl.time(date_UTC)}")

        directory = self.local_directory(date_UTC=date_UTC)
        pattern = join(directory, f"HLS.S30.T{tile[:5]}.{date_UTC:%Y%j}.*.hdf")
        candidates = sorted(glob(pattern))

        if len(candidates) > 0:
            filename = candidates[-1]
            logger.info(f"found HLS2 Landsat file: {cl.file(filename)}")
            return filename

        filename_base = self.sentinel_filename(tile=tile, date_UTC=date_UTC)
        filename = join(directory, filename_base)

        return filename

    def local_landsat_filename(self, tile: str, date_UTC: Union[date, str]) -> str:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        if self.check_unavailable_date("Landsat", tile, date_UTC):
            # logger.error(self.unavailable_dates["Landsat"][tile])
            raise HLSLandsatNotAvailable(f"Landsat is not available at tile {cl.place(tile)} on {cl.time(date_UTC)}")

        directory = self.local_directory(date_UTC=date_UTC)
        candidates = sorted(glob(join(directory, f"HLS.L30.T{tile[:5]}.{date_UTC:%Y%j}.*.hdf")))

        if len(candidates) > 0:
            filename = candidates[-1]
            logger.info(f"found HLS2 Landsat file: {cl.file(filename)}")
            return filename

        filename_base = self.landsat_filename(tile=tile, date_UTC=date_UTC)
        filename = join(directory, filename_base)

        return filename

    def sentinel(self, tile: str, date_UTC: Union[date, str]) -> HLSSentinelGranule:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        logger.info(f"searching for Sentinel tile {cl.name(tile)} on {cl.time(date_UTC)}")
        filename = self.local_sentinel_filename(tile=tile, date_UTC=date_UTC)

        if exists(filename):
            logger.info(f"Sentinel tile {cl.name(tile)} found on {cl.time(date_UTC)}: {filename}")
        if not exists(filename):
            logger.info(f"retrieving Sentinel tile {cl.name(tile)} on {cl.time(date_UTC)}: {filename}")
            URL = self.sentinel_URL(tile=tile, date_UTC=date_UTC)
            self.download_file(URL, filename)

        granule = HLSSentinelGranule(filename)

        return granule

    def landsat(self, tile: str, date_UTC: Union[date, str]) -> HLSLandsatGranule:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        logger.info(f"searching for Landsat tile {cl.name(tile)} on {cl.time(date_UTC)}")
        filename = self.local_landsat_filename(tile=tile, date_UTC=date_UTC)

        if exists(filename):
            logger.info(f"Landsat tile {cl.name(tile)} found on {cl.time(date_UTC)}: {filename}")
        if not exists(filename):
            logger.info(f"retrieving Landsat tile {cl.name(tile)} on {cl.time(date_UTC)}: {filename}")
            URL = self.landsat_URL(tile=tile, date_UTC=date_UTC)
            self.download_file(URL, filename)

        granule = HLSLandsatGranule(filename)

        return granule

    def NDVI(
            self,
            tile: str,
            date_UTC: Union[date, str]) -> Union[Raster, str]:
        target_tile = tile
        target_geometry = self.grid(target_tile)
        tile = tile[:5]
        geometry = self.grid(tile)

        if product_filename is None:
            product_filename = self.product_filename(
                product="NDVI",
                date_UTC=date_UTC,
                tile=tile
            )

        if preview_filename is None:
            preview_filename = product_filename.replace(".tif", ".jpeg")

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
            NDVI = sentinel.NDVI
        elif sentinel is None and landsat is not None:
            NDVI = landsat.NDVI
        else:
            NDVI = rt.Raster(np.nanmean(np.dstack([sentinel.NDVI, landsat.NDVI]), axis=2), geometry=sentinel.geometry)

        if self.target_resolution > 30:
            NDVI = NDVI.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            NDVI = NDVI.to_geometry(geometry, resampling="cubic")
        
        return NDVI

    def product(
            self,
            product: str,
            tile: str,
            date_UTC: Union[date, str],
            geometry: RasterGeometry = None) -> Union[Raster, str]:
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
            image = sentinel.product(product)
        elif sentinel is None and landsat is not None:
            image = landsat.product(product)
        else:
            image = rt.Raster(np.nanmean(np.dstack([sentinel.NDVI, landsat.NDVI]), axis=2), geometry=sentinel.geometry)

        if self.target_resolution > 30:
            image = image.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            image = image.to_geometry(geometry, resampling="cubic")

        return image

    def process(
            self,
            start: Union[date, str],
            end: Union[date, str],
            target: str,
            target_geometry: Union[SpatialGeometry, str] = None,
            product_names: List[str] = None):
        if product_names is None:
            product_names = DEFAULT_PRODUCTS

        for date_UTC in date_range(start, end):
            self.product()

    def albedo(
            self,
            tile: str,
            date_UTC: Union[date, str]) -> Union[Raster, str]:
        target_tile = tile
        target_geometry = self.grid(target_tile)
        tile = tile[:5]
        geometry = self.grid(tile)

        if product_filename is None:
            product_filename = self.product_filename(
                product="albedo",
                date_UTC=date_UTC,
                tile=tile
            )

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
            albedo = sentinel.albedo
        elif sentinel is None and landsat is not None:
            albedo = landsat.albedo
        else:
            albedo = rt.Raster(np.nanmean(np.dstack([sentinel.albedo, landsat.albedo]), axis=2),
                               geometry=sentinel.geometry)

        if self.target_resolution > 30:
            albedo = albedo.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            albedo = albedo.to_geometry(geometry, resampling="cubic")

        return albedo
