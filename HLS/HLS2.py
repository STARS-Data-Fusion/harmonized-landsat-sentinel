import json
import logging
import os
import posixpath
from abc import abstractmethod
from datetime import date, timedelta, time
from datetime import datetime
from glob import glob
from os import makedirs, system
from os.path import exists, dirname, abspath, join, getsize, isdir, basename, expanduser
from shutil import move
from time import sleep
from typing import List, Union, Set

import numpy as np
import pandas as pd
import requests
from ETtoolbox.ERS_credentials import get_ERS_credentials
from dateutil import parser
from pystac import Item
from pystac_client import Client
from shapely.geometry import Polygon, Point, mapping, shape

import colored_logging
import rasters as rt
from ETtoolbox.HLS import HLSGranule, HLSGranuleID, HLSSentinelGranule, CLOUD_CMAP, \
    WATER_CMAP, HLS, HLSLandsatGranule, HLSNotAvailable, HLSLandsatMissing, HLSLandsatNotAvailable, HLSSentinelMissing, \
    HLSSentinelNotAvailable, HLSDownloadFailed, HLSServerUnreachable
from ETtoolbox.daterange import date_range
from rasters import Raster
from ETtoolbox.timer import Timer

with open(join(abspath(dirname(__file__)), "version.txt")) as f:
    version = f.read()

__version__ = version
__author__ = "Gregory H. Halverson"

CMR_STAC_URL = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"
WORKING_DIRECTORY = "."
DOWNLOAD_DIRECTORY = "HLS2_download"
PRODUCTS_DIRECTORY = "HLS2_products"
TARGET_RESOLUTION = 30
COLLECTIONS = ["HLSS30.v2.0", "HLSL30.v2.0"]
DEFAULT_RETRIES = 3
DEFAULT_WAIT_SECONDS = 20
DEFAULT_DOWNLOAD_RETRIES = 3
DEFAULT_DOWNLOAD_WAIT_SECONDS = 60
L30_CONCEPT = "C2021957657-LPCLOUD"
S30_CONCEPT = "C2021957295-LPCLOUD"
PAGE_SIZE = 2000
CMR_SEARCH_URL = "https://cmr.earthdata.nasa.gov/search"
CMR_GRANULES_JSON_URL = f"{CMR_SEARCH_URL}/granules.json"

logger = logging.getLogger(__name__)


class HLSBandNotAcquired(IOError):
    pass


class HLS2Granule(HLSGranule):
    def __init__(self, directory: str, connection=None):
        super(HLS2Granule, self).__init__(directory)
        self.directory = directory
        self.ID = HLSGranuleID(basename(directory))
        self.connection = connection

    def __repr__(self) -> str:
        return f"HLS2Granule({self.directory})"

    @property
    def filenames(self) -> List[str]:
        return sorted(glob(join(self.directory, f"*.*")))

    def band_filename(self, band: str) -> str:
        band = self.band_name(band)
        pattern = join(self.directory, f"*.{band}.tif")
        filenames = sorted(glob(pattern))

        if len(filenames) == 0:
            raise HLSBandNotAcquired(f"no file found for band {band} for granule {self.ID}")

        return filenames[-1]

    def DN(self, band: str) -> Raster:
        if band in self.band_images:
            return self.band_images[band]

        filename = self.band_filename(band)
        image = Raster.open(filename)
        self.band_images[band] = image

        return image

    @property
    def Fmask(self) -> Raster:
        return self.DN("Fmask")

    @property
    def QA(self) -> Raster:
        return self.Fmask

    @property
    def geometry(self):
        return self.QA.geometry

    @property
    def cloud(self) -> Raster:
        return (self.QA & 15 > 0).color(CLOUD_CMAP)

    @property
    def water(self) -> Raster:
        return ((self.QA >> 5) & 1 == 1).color(WATER_CMAP)

    def band(self, band: str, apply_scale: bool = True, apply_cloud: bool = True) -> Raster:
        image = self.DN(band)

        if apply_scale:
            image = rt.where(image == -1000, np.nan, image * 0.0001)
            image = rt.where(image < 0, np.nan, image)
            image.nodata = np.nan

        if apply_cloud:
            image = rt.where(self.cloud, np.nan, image)

        return image


class HLS2SentinelGranule(HLS2Granule, HLSSentinelGranule):
    pass


class HLS2LandsatGranule(HLS2Granule, HLSLandsatGranule):
    pass

class HLS2(HLS):
    @classmethod
    def parse_ID(cls, ID: str) -> HLSGranuleID:
        return HLSGranuleID(ID)

    def date_directory(self, date_UTC: Union[date, str]) -> str:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        directory = join(self.download_directory, f"{date_UTC:%Y.%m.%d}")

        return directory

    def sentinel_ID(self, tile: str, date_UTC: Union[date, str]) -> str:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        listing = self.listing(tile=tile, start_UTC=(date_UTC - timedelta(days=5)), end_UTC=date_UTC)
        ID = str(listing.iloc[-1].sentinel)

        if ID == "nan":
            self.mark_date_unavailable("Sentinel", tile, date_UTC)
            raise HLSSentinelNotAvailable(f"Sentinel is not available at tile {colored_logging.place(tile)} on {colored_logging.time(date_UTC)}")
        elif ID == "missing":
            raise HLSSentinelMissing(
                f"Sentinel is missing on remote server at tile {colored_logging.place(tile)} on {colored_logging.time(date_UTC)}")
        else:
            return ID

    def landsat_ID(self, tile: str, date_UTC: Union[date, str]) -> str:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        listing = self.listing(tile=tile, start_UTC=(date_UTC - timedelta(days=5)), end_UTC=date_UTC)
        ID = str(listing.iloc[-1].landsat)

        if ID == "nan":
            self.mark_date_unavailable("Landsat", tile, date_UTC)
            error_string = f"Landsat is not available at tile {colored_logging.place(tile)} on {colored_logging.time(date_UTC)}"
            most_recent_listing = listing[listing.landsat.apply(lambda landsat: landsat not in ("nan", "missing"))]

            if len(most_recent_listing) > 0:
                most_recent = most_recent_listing.iloc[-1].landsat
                error_string += f" most recent granule: {colored_logging.val(most_recent)}"

            raise HLSLandsatNotAvailable(error_string)
        elif ID == "missing":
            raise HLSLandsatMissing(
                f"Landsat is missing on remote server at tile {colored_logging.place(tile)} on {colored_logging.time(date_UTC)}")
        else:
            return ID

    def sentinel_directory(self, tile: str, date_UTC: Union[date, str]) -> str:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        if self.check_unavailable_date("Sentinel", tile, date_UTC):
            raise HLSSentinelNotAvailable(f"Sentinel is not available at tile {colored_logging.place(tile)} on {colored_logging.time(date_UTC)}")

        date_directory = self.date_directory(date_UTC=date_UTC)
        pattern = join(date_directory, f"HLS.S30.T{tile[:5]}.{date_UTC:%Y%j}*")
        candidates = [item for item in sorted(glob(pattern)) if isdir(item)]

        if len(candidates) > 0:
            granule_directory = candidates[-1]
            logger.info(f"found HLS2 Sentinel directory: {colored_logging.file(granule_directory)}")
            return granule_directory

        granule_directory = join(date_directory, self.sentinel_ID(tile=tile, date_UTC=date_UTC))

        return granule_directory

    def landsat_directory(self, tile: str, date_UTC: Union[date, str]) -> str:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        if self.check_unavailable_date("Landsat", tile, date_UTC):
            raise HLSLandsatNotAvailable(f"Landsat is not available at tile {colored_logging.place(tile)} on {colored_logging.time(date_UTC)}")

        date_directory = self.date_directory(date_UTC=date_UTC)
        pattern = join(date_directory, f"HLS.L30.T{tile[:5]}.{date_UTC:%Y%j}*")
        candidates = [item for item in sorted(glob(pattern)) if isdir(item)]

        if len(candidates) > 0:
            granule_directory = candidates[-1]
            logger.info(f"found HLS2 Landsat directory: {colored_logging.file(granule_directory)}")
            return granule_directory

        granule_directory = join(date_directory, self.landsat_ID(tile=tile, date_UTC=date_UTC))

        return granule_directory

    @abstractmethod
    def bands(self, ID: str, bands: List[str] = None):
        pass

    def sentinel(self, tile: str, date_UTC: Union[date, str], bands: List[str] = None) -> HLS2SentinelGranule:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        logger.info(f"searching for Sentinel tile {colored_logging.name(tile)} on {colored_logging.time(date_UTC)}")
        directory = self.sentinel_directory(tile=tile, date_UTC=date_UTC)
        logger.info(f"retrieving Sentinel tile {colored_logging.name(tile)} on {colored_logging.time(date_UTC)}: {directory}")
        ID = self.sentinel_ID(tile=tile, date_UTC=date_UTC)
        band_URL_df = self.bands(ID=ID, bands=bands)

        for URL in band_URL_df.URL:
            filename = join(directory, posixpath.basename(URL))
            self.download_file(URL, filename)

        granule = HLS2SentinelGranule(directory)

        return granule

    def landsat(self, tile: str, date_UTC: Union[date, str], bands: List[str] = None) -> HLS2LandsatGranule:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        logger.info(f"searching for Landsat tile {colored_logging.name(tile)} on {colored_logging.time(date_UTC)}")
        directory = self.landsat_directory(tile=tile, date_UTC=date_UTC)
        logger.info(f"retrieving Landsat tile {colored_logging.name(tile)} on {colored_logging.time(date_UTC)}: {directory}")
        ID = self.landsat_ID(tile=tile, date_UTC=date_UTC)
        band_URL_df = self.bands(ID=ID, bands=bands)

        for URL in band_URL_df.URL:
            filename = join(directory, posixpath.basename(URL))
            self.download_file(URL, filename)

        granule = HLS2LandsatGranule(directory)

        return granule

    def NDVI(
            self,
            tile: str,
            date_UTC: Union[date, str],
            product_filename: str = None,
            preview_filename: str = None,
            save_data: bool = False,
            save_preview: bool = False,
            return_filename: bool = False) -> Union[Raster, str]:
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

        if exists(product_filename):
            if return_filename:
                return product_filename
            else:
                self.logger.info(f"loading HLS2 NDVI: {colored_logging.file(product_filename)}")
                return Raster.open(product_filename, geometry=target_geometry)

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
            except HLSBandNotAcquired:
                raise HLSNotAvailable(f"HLS2 S30 is not available at {tile} on {date_UTC}")
        elif sentinel is None and landsat is not None:
            try:
                NDVI = landsat.NDVI
            except HLSBandNotAcquired:
                raise HLSNotAvailable(f"HLS2 L30 is not available at {tile} on {date_UTC}")
        else:
            NDVI = rt.Raster(np.nanmean(np.dstack([sentinel.NDVI, landsat.NDVI]), axis=2), geometry=sentinel.geometry)

        if self.target_resolution > 30:
            NDVI = NDVI.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            NDVI = NDVI.to_geometry(geometry, resampling="cubic")

        if (save_data or return_filename) and not exists(product_filename):
            self.logger.info(f"saving HLS2 NDVI: {colored_logging.file(product_filename)}")
            NDVI.to_COG(product_filename)

            if save_preview:
                self.logger.info(f"saving HLS2 NDVI preview: {colored_logging.file(preview_filename)}")
                NDVI.to_geojpeg(preview_filename)

        NDVI = NDVI.to_geometry(target_geometry)

        if return_filename:
            return product_filename
        else:
            return NDVI

    def product_directory(self, product: str, date_UTC: Union[date, str]):
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        return join(self.products_directory, product, f"{date_UTC:%Y.%m.%d}")

    def product_filename(self, product: str, date_UTC: Union[date, str], tile: str):
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        directory = self.product_directory(product=product, date_UTC=date_UTC)
        filename = join(directory, f"HLS_{tile}_{date_UTC:%Y%m%d}_{product}.tif")

        return filename

    def albedo(
            self,
            tile: str,
            date_UTC: Union[date, str],
            product_filename: str = None,
            preview_filename: str = None,
            save_data: bool = False,
            save_preview: bool = False,
            return_filename: bool = False) -> Union[Raster, str]:

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

        if preview_filename is None:
            preview_filename = product_filename.replace(".tif", ".jpeg")

        if exists(product_filename):
            if return_filename:
                return product_filename
            else:
                self.logger.info(f"loading HLS2 albedo: {colored_logging.file(product_filename)}")
                return Raster.open(product_filename, geometry=target_geometry)

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
            albedo = rt.Raster(np.nanmean(np.dstack([sentinel.albedo, landsat.albedo]), axis=2),
                               geometry=sentinel.geometry)

        if self.target_resolution > 30:
            albedo = albedo.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            albedo = albedo.to_geometry(geometry, resampling="cubic")

        if (save_data and return_filename) and not exists(product_filename):
            self.logger.info(f"saving HLS2 albedo: {colored_logging.file(product_filename)}")
            albedo.to_COG(product_filename)

            if save_preview:
                self.logger.info(f"saving HLS2 albedo preview: {colored_logging.file(preview_filename)}")
                albedo.to_geojpeg(preview_filename)

        albedo = albedo.to_geometry(target_geometry)

        if return_filename:
            return product_filename

        return albedo

    def download_file(self, URL: str, filename: str, retries: int = DEFAULT_DOWNLOAD_RETRIES, wait_seconds: int = DEFAULT_DOWNLOAD_WAIT_SECONDS):
        attempts = 0
        while attempts < retries:
            try:
                if exists(filename) and getsize(filename) == 0:
                    logger.warning(f"removing zero-size corrupted HLS2 file: {filename}")
                    os.remove(filename)

                if exists(filename):
                    self.logger.info(f"file already downloaded: {colored_logging.file(filename)}")
                    return filename

                self.logger.info(f"downloading: {colored_logging.URL(URL)} -> {colored_logging.file(filename)}")
                directory = dirname(filename)
                makedirs(directory, exist_ok=True)
                partial_filename = f"{filename}.download"
                command = f'wget -c --user {self._username} --password {self._password} -O "{partial_filename}" "{URL}"'
                # FIXME remove hard-coded credentials
                timer = Timer()
                system(command)
                self.logger.info(f"completed download in {colored_logging.time(timer)} seconds: " + colored_logging.file(filename))

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
            except Exception as e:
                logger.exception(e)
                logger.warning(f"waiting for {wait_seconds} to retry download")
                sleep(wait_seconds)
                attempts += 1
                continue


class HLS2CMRSTAC(HLS2):
    logger = logging.getLogger(__name__)
    URL = CMR_STAC_URL
    WORKING_DIRECTORY = WORKING_DIRECTORY
    DOWNLOAD_DIRECTORY = DOWNLOAD_DIRECTORY
    PRODUCTS_DIRECTORY = PRODUCTS_DIRECTORY
    TARGET_RESOLUTION = TARGET_RESOLUTION

    def __init__(
            self,
            working_directory: str = None,
            download_directory: str = None,
            products_directory: str = None,
            target_resolution: int = None,
            username: str = None,
            password: str = None,
            ERS_credentials_filename: str = None,
            remote: str = None,
            retries: int = DEFAULT_RETRIES,
            wait_seconds: float = DEFAULT_WAIT_SECONDS):
        if target_resolution is None:
            target_resolution = self.DEFAULT_TARGET_RESOLUTION

        if remote is None:
            remote = self.URL

        if username is None or password is None:
            credentials = get_ERS_credentials(filename=ERS_credentials_filename)
            username = credentials["username"]
            password = credentials["password"]

        self._username = username
        self._password = password

        logger.info(f"HLS 2.0 CMR STAC URL: {colored_logging.URL(remote)}")
        logger.info(f"HLS 2.0 working directory: {colored_logging.dir(working_directory)}")
        logger.info(f"HLS 2.0 download directory: {colored_logging.dir(download_directory)}")
        logger.info(f"HLS 2.0 products directory: {colored_logging.dir(products_directory)}")

        super(HLS2CMRSTAC, self).__init__(
            working_directory=working_directory,
            download_directory=download_directory,
            products_directory=products_directory,
            target_resolution=target_resolution
        )

        self.retries = retries
        self.wait_seconds = wait_seconds

        attempt_count = 0

        while attempt_count < self.retries:
            attempt_count += 1

            try:
                self.stac = Client.open(remote)
                self.remote = remote
                self.check_remote()
                break
            except Exception as e:
                logger.warning(e)
                logger.warning(f"HLS connection attempt {attempt_count} failed: {self.remote}")

                if attempt_count < self.retries:
                    sleep(self.wait_seconds)
                    logger.warning(f"re-trying HLS server: {self.remote}")
                    continue
                else:
                    raise HLSServerUnreachable(f"HLS server un-reachable: {self.remote}")

    def sentinel_listing(self, tile: str, year: int) -> pd.DataFrame:
        listing = self.search(
            tile=tile,
            start_UTC=date(year, 1, 1),
            end_UTC=date(year, 12, 31),
            collections=["HLSS30.v2.0"]
        )

        listing = listing.rename({"ID": "sentinel"}, axis="columns")

        return listing

    def landsat_listing(self, tile: str, year: int) -> pd.DataFrame:
        listing = self.search(
            tile=tile,
            start_UTC=date(year, 1, 1),
            end_UTC=date(year, 12, 31),
            collections=["HLSL30.v2.0"]
        )

        listing = listing.rename({"ID": "landsat"}, axis="columns")

        return listing

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
        self.logger.info(
            f"started listing available HLS2 granules at tile {colored_logging.place(tile)} from {colored_logging.time(start_UTC)} to {colored_logging.time(end_UTC)}")

        if isinstance(start_UTC, str):
            start_UTC = parser.parse(start_UTC).date()

        if end_UTC is None:
            end_UTC = start_UTC

        if isinstance(end_UTC, str):
            end_UTC = parser.parse(end_UTC).date()

        giveup_date = datetime.utcnow().date() - timedelta(days=GIVEUP_DAYS)
        search_start = start_UTC - timedelta(days=max(SENTINEL_REPEAT_DAYS, LANDSAT_REPEAT_DAYS))
        search_end = end_UTC + timedelta(days=max(SENTINEL_REPEAT_DAYS, LANDSAT_REPEAT_DAYS))
        start_year = search_start.year
        end_year = search_end.year
        listing = pd.concat([self.year_listing(tile=tile, year=year) for year in range(start_year, end_year + 1)])
        # listing = listing[listing.date_UTC <= str(end)]
        listing = listing[listing.date_UTC.apply(lambda date_UTC: str(date_UTC) <= str(search_end))]

        if len(listing) == 0:
            raise HLSNotAvailable(f"no search results for HLS tile {tile}")

        sentinel_dates = set(
            [parser.parse(str(timestamp)).date() for timestamp in list(listing[~listing.sentinel.isna()].date_UTC)])

        if len(sentinel_dates) > 0:
            # for date_UTC in [dt.date() for dt in rrule(DAILY, dtstart=max(sentinel_dates), until=end)]:
            for date_UTC in date_range(max(sentinel_dates), search_end):
                previous_pass = date_UTC - timedelta(SENTINEL_REPEAT_DAYS)

                if previous_pass in sentinel_dates and date_UTC not in sentinel_dates:
                    if str(date_UTC) >= str(start_UTC) and str(date_UTC) <= str(end_UTC):
                        logger.info(f"expecting Sentinel overpass on {colored_logging.time(date_UTC)} based on {colored_logging.val(SENTINEL_REPEAT_DAYS)} days repeat from known previous overpass {colored_logging.time(previous_pass)}")
                    
                    sentinel_dates.add(date_UTC)

        landsat_dates = set(
            [parser.parse(str(timestamp)).date() for timestamp in list(listing[~listing.landsat.isna()].date_UTC)])

        if len(landsat_dates) > 0:
            for date_UTC in date_range(max(landsat_dates), search_end):
                previous_pass = date_UTC - timedelta(LANDSAT_REPEAT_DAYS)

                if previous_pass in landsat_dates and date_UTC not in landsat_dates:
                    if str(date_UTC) >= str(start_UTC) and str(date_UTC) <= str(end_UTC):
                        logger.info(f"expecting Landsat overpass on {colored_logging.time(date_UTC)} based on {colored_logging.val(LANDSAT_REPEAT_DAYS)} days repeat from known previous overpass {colored_logging.time(previous_pass)}")
                    
                    landsat_dates.add(date_UTC)

        listing = listing[listing.date_UTC.apply(lambda date_UTC: str(date_UTC) >= str(search_start))]
        listing.date_UTC = listing.date_UTC.apply(
            lambda date_UTC: parser.parse(str(date_UTC)).date().strftime("%Y-%m-%d"))
        dates = pd.DataFrame(
            {"date_UTC": [date_UTC.strftime("%Y-%m-%d") for date_UTC in date_range(search_start, search_end)], "tile": tile})
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

        listing = listing[listing.date_UTC.apply(lambda date_UTC: str(date_UTC) <= str(end_UTC))]
        listing = listing[listing.date_UTC.apply(lambda date_UTC: str(date_UTC) >= str(start_UTC))]

        self.logger.info(
            f"finished listing available HLS2 granules at tile {colored_logging.place(tile)} from {colored_logging.time(start_UTC)} to {colored_logging.time(end_UTC)} ({timer})")

        return listing

    def landsat_URL(self, tile: str, date_UTC: Union[date, str]):
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        directory = self.landsat_directory(tile=tile, year=date_UTC.year)
        filename = self.landsat_ID(tile=tile, date_UTC=date_UTC)
        URL = posixpath.join(directory, filename)

        return URL

    def search(
            self,
            tile: str = None,
            start_UTC: Union[date, datetime, str] = None,
            end_UTC: Union[date, datetime, str] = None,
            collections: List[str] = None,
            IDs: List[str] = None,
            geometry: Union[Polygon, Point, str] = None):
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

        if start_UTC is not None and end_UTC is not None:
            date_search_string = f"{start_UTC}/{end_UTC}"
        else:
            date_search_string = None

        if collections is None:
            collections = COLLECTIONS

        if isinstance(geometry, (Polygon, Point)):
            geometry = json.dumps(mapping(geometry))
        elif isinstance(geometry, str):
            geometry = json.dumps(mapping(shape(geometry)))

        if geometry is None and tile is not None:
            geometry = self.tile_grid.centroid(tile)

        if IDs is None:
            ID_message = ""
        else:
            ID_message = f" with IDs: {', '.join(IDs)}"

        logger.info(f"searching {', '.join(collections)} at {tile} from {start_UTC} to {end_UTC}{ID_message}")

        attempt_count = 0

        while attempt_count < self.retries:
            attempt_count += 1

            try:
                search = self.stac.search(
                    collections=collections,
                    ids=IDs,
                    datetime=date_search_string,
                    intersects=geometry
                )

                items = search.get_all_items()
                break
            except Exception as e:
                logger.warning(e)
                logger.warning(f"HLS connection attempt {attempt_count} failed: {self.remote}")

                if attempt_count < self.retries:
                    sleep(self.wait_seconds)
                    logger.warning(f"re-trying HLS server: {self.remote}")
                    continue
                else:
                    raise HLSServerUnreachable(f"HLS server un-reachable: {self.remote}")

        IDs = [item.id for item in items]
        tiles = [ID.split(".")[2][1:] for ID in IDs]
        dates = [datetime.strptime(ID.split(".")[3].split("T")[0], "%Y%j").date() for ID in IDs]

        df = pd.DataFrame({"date_UTC": dates, "tile": tiles, "ID": IDs})
        df = df.sort_values(by=["date_UTC", "tile"])

        return df

    def item(self, ID: str) -> Item:
        attempt_count = 0

        while attempt_count < self.retries:
            attempt_count += 1

            try:
                search = self.stac.search(ids=[ID])
                items = search.get_all_items()

                for item in items:
                    if item.id == ID:
                        return item

                raise HLSServerUnreachable(f"ID not found: {ID}")

            except Exception as e:
                logger.warning(e)
                logger.warning(f"HLS connection attempt {attempt_count} failed: {self.remote}")

                if attempt_count < self.retries:
                    sleep(self.wait_seconds)
                    logger.warning(f"re-trying HLS server: {self.remote}")
                    continue
                else:
                    raise HLSServerUnreachable(f"HLS server un-reachable: {self.remote}")

    def assets(self, ID: str):
        return self.item(ID).assets

    def bands(self, ID: str, bands: List[str] = None):
        df = pd.DataFrame([(key, value.href) for key, value in self.assets(ID).items()], columns=["band", "URL"])

        if bands is not None:
            df = df[df.band.apply(lambda band: band in bands)]

        return df


def generate_CMR_date_range(start_date: Union[date, str], end_date: Union[date, str]) -> str:
    """
    function to generate CMR date-range query string
    """
    if isinstance(start_date, str):
        start_date = parser.parse(start_date)

    if isinstance(end_date, str):
        end_date = parser.parse(end_date)

    start_date_string = start_date.strftime("%Y-%m-%d")
    end_date_string = end_date.strftime("%Y-%m-%d")
    date_range_string = f"{start_date_string}T00:00:00Z/{end_date_string}T23:59:59Z"

    return date_range_string


"function to generate CMR API URL to search for HLS"


def generate_CMR_query_URL(
        concept_ID: str,
        tile: str,
        start_date: Union[date, str],
        end_date: Union[date, str],
        page_size: int = PAGE_SIZE) -> str:
    date_range_string = generate_CMR_date_range(start_date, end_date)
    URL = f"{CMR_GRANULES_JSON_URL}?concept_id={concept_ID}&temporal={date_range_string}&page_size={page_size}&producer_granule_id=*.T{tile}.*&options[producer_granule_id][pattern]=true"

    return URL

def CMR_query_URLs(
        concept_ID: str,
        tile: str,
        start_date: Union[date, str],
        end_date: Union[date, str],
        page_size: int = PAGE_SIZE) -> List[str]:
    # generate CMR URL to search for Sentinel tile in date range for given concept ID
    query_URL = generate_CMR_query_URL(
        concept_ID=concept_ID,
        tile=tile,
        start_date=start_date,
        end_date=end_date,
        page_size=page_size
    )

    # send get request for the CMR query URL
    logger.info(
        f"CMR API query for concept ID {concept_ID} at tile {tile} from {start_date} to {end_date} with URL: {query_URL}")
    response = requests.get(query_URL)

    # check the status code of the response and make sure it's 200 before parsing JSON

    status = response.status_code

    if status != 200:
        logger.error(f"CMR API status {status} for URL: {query_URL}")

    # parse JSON response from successful (200) CMR query
    response_dict = json.loads(response.text)

    # build list of URLs returned by CMR API
    URLs = []

    for entry in response_dict["feed"]["entry"]:
        for link in entry["links"]:
            URLs.append(link["href"])

    return URLs

# need to include header with client ID and make "Client-Id: HLS.jl" the default header
"function to search for HLS at tile within data range, given concept ID"
def CMR_query(
        concept_ID: str,
        tile: str,
        start_date: Union[date, str],
        end_date: Union[date, str],
        page_size: int = PAGE_SIZE) -> pd.DataFrame:
    URLs = CMR_query_URLs(
        concept_ID=concept_ID,
        tile=tile,
        start_date=start_date,
        end_date=end_date,
        page_size=page_size
    )

    URL_count = len(URLs)
    logger.info(f"CMR API query for concept ID {concept_ID} at tile {tile} from {start_date} to {end_date} included {URL_count} URLs")

    https_rows = []
    s3_rows = []

    # https_df = pd.DataFrame({
    #     "granule_ID": [],
    #     "sensor": [],
    #     "tile": [],
    #     "date": [],
    #     "time": [],
    #     "band": [],
    #     "https": []
    # })
    #
    # s3_df = pd.DataFrame({
    #     "granule_ID": [],
    #     "sensor": [],
    #     "tile": [],
    #     "date": [],
    #     "time": [],
    #     "band": [],
    #     "s3": []
    # })

    for URL in URLs:
        # parse URL
        logger.info(f"parsing URL: {URL}")
        protocol = URL.split(":")[0]
        filename_base = URL.split("/")[-1]

        if not filename_base.startswith("HLS."):
            logger.info(f"skipping URL: {URL}")
            continue

        granule_ID = ".".join(filename_base.split(".")[:6])
        sensor = filename_base.split(".")[1]
        tile = filename_base.split(".")[2][1:]
        timestamp = filename_base.split(".")[3].replace("T", "")
        year = int(timestamp[:4])
        doy = int(timestamp[4:7])
        hour = int(timestamp[7:9])
        minute = int(timestamp[9:11])
        second = int(timestamp[11:13])
        dt = datetime(year, 1, 1, hour, minute, second) + timedelta(days=(doy - 1))
        d = dt.date()
        band = filename_base.split(".")[-2]

        # filter out invalid URLs
        if band in ["0_stac", "cmr", "0"]:
            continue

        logger.info(f"protocol: {protocol}")

        if protocol == "https":
            https_rows.append([granule_ID, sensor, tile, d, dt, band, URL])
            # https_df.append(pd.DataFrame({
            #     "granule_ID": [granule_ID],
            #     "sensor": [sensor],
            #     "tile": [tile],
            #     "date": [date],
            #     "time": [time],
            #     "band": [band],
            #     "https": [URL]
            # }))
        elif protocol == "s3":
            s3_rows.append([granule_ID, sensor, tile, d, dt, band, URL])
            # s3_df.append(pd.DataFrame({
            #     "granule_ID": [granule_ID],
            #     "sensor": [sensor],
            #     "tile": [tile],
            #     "date": [date],
            #     "time": [time],
            #     "band": [band],
            #     "s3": [URL]
            # }))

    https_df = pd.DataFrame(https_rows, columns=["ID", "sensor", "tile", "date_UTC", "time", "band", "https"])
    logger.info(f"collected {len(https_df)} HTTPS results")
    s3_df = pd.DataFrame(https_rows, columns=["ID", "sensor", "tile", "date_UTC", "time", "band", "s3"])
    logger.info(f"collected {len(s3_df)} S3 results")
    df = pd.merge(https_df, s3_df, how="outer")

    return df

"function to search for HLS L30 Landsat product at tile in date range"
def L30_CMR_query(
        tile: str,
        start_date: Union[date, str],
        end_date: Union[date, str],
        page_size: int = PAGE_SIZE) -> pd.DataFrame:
    logger.info(f"CMR API query for HLS Landsat L30 at tile {tile} from {start_date} to {end_date}")

    return CMR_query(
        L30_CONCEPT,
        tile,
        start_date,
        end_date,
        page_size
    )

"function to search for HLS L30 Landsat product at tile in date range"
def S30_CMR_query(
        tile: str,
        start_date: Union[date, str],
        end_date: Union[date, str],
        page_size: int = PAGE_SIZE) -> pd.DataFrame:
    logger.info(f"CMR API query for HLS Landsat S30 at tile {tile} from {start_date} to {end_date}")

    return CMR_query(
        S30_CONCEPT,
        tile,
        start_date,
        end_date,
        page_size
    )

"function to search for HLS at tile in date range"
def HLS_CMR_query(
        tile: str,
        start_date: Union[date, str],
        end_date: Union[date, str],
        page_size: int = PAGE_SIZE) -> pd.DataFrame:
    S30_listing = S30_CMR_query(
        tile=tile,
        start_date=start_date,
        end_date=end_date,
        page_size=page_size
    )

    L30_listing = L30_CMR_query(
        tile=tile,
        start_date=start_date,
        end_date=end_date,
        page_size=page_size
    )

    logger.info(f"collected {len(S30_listing)} Sentinel results")
    logger.info(f"collected {len(L30_listing)} Landsat results")
    listing = pd.concat([S30_listing, L30_listing])
    listing = listing.sort_values(by="time")

    return listing


class HLS2CMR(HLS2):
    URL = CMR_SEARCH_URL

    def __init__(
            self,
            working_directory: str = None,
            download_directory: str = None,
            products_directory: str = None,
            target_resolution: int = None,
            username: str = None,
            password: str = None,
            ERS_credentials_filename: str = None,
            remote: str = None,
            retries: int = DEFAULT_RETRIES,
            wait_seconds: float = DEFAULT_WAIT_SECONDS):
        if target_resolution is None:
            target_resolution = self.DEFAULT_TARGET_RESOLUTION

        if remote is None:
            remote = self.URL

        logger.info(f"HLS 2.0 CMR URL: {colored_logging.URL(remote)}")

        if working_directory is None:
            working_directory = abspath("")

        working_directory = expanduser(working_directory)
        logger.info(f"HLS 2.0 working directory: {colored_logging.dir(working_directory)}")

        if download_directory is None:
            download_directory = join(working_directory, DOWNLOAD_DIRECTORY)

        logger.info(f"HLS 2.0 download directory: {colored_logging.dir(download_directory)}")

        if products_directory is None:
            products_directory = join(working_directory, PRODUCTS_DIRECTORY)

        logger.info(f"HLS 2.0 products directory: {colored_logging.dir(products_directory)}")

        if username is None or password is None:
            credentials = get_ERS_credentials(filename=ERS_credentials_filename)
            username = credentials["username"]
            password = credentials["password"]

        self._username = username
        self._password = password

        super(HLS2CMR, self).__init__(
            working_directory=working_directory,
            download_directory=download_directory,
            products_directory=products_directory,
            target_resolution=target_resolution
        )

        self.retries = retries
        self.wait_seconds = wait_seconds

        attempt_count = 0

        while attempt_count < self.retries:
            attempt_count += 1

            try:
                self.remote = remote
                self.check_remote()
                break
            except Exception as e:
                logger.warning(e)
                logger.warning(f"HLS connection attempt {attempt_count} failed: {self.remote}")

                if attempt_count < self.retries:
                    sleep(self.wait_seconds)
                    logger.warning(f"re-trying HLS server: {self.remote}")
                    continue
                else:
                    raise HLSServerUnreachable(f"HLS server un-reachable: {self.remote}")

        self._listing = pd.DataFrame([], columns=["date_UTC", "tile", "sentinel", "landsat"])
        self._URLs = pd.DataFrame([], columns=["ID", "sensor", "tile", "date_UTC", "time", "band", "https", "s3"])

    def search(
            self,
            tile: str = None,
            start_UTC: Union[date, datetime, str] = None,
            end_UTC: Union[date, datetime, str] = None,
            collections: List[str] = None,
            IDs: List[str] = None,
            geometry: Union[Polygon, Point, str] = None,
            page_size: int = PAGE_SIZE):
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

        if start_UTC is not None and end_UTC is not None:
            date_search_string = f"{start_UTC}/{end_UTC}"
        else:
            date_search_string = None

        if collections is None:
            collections = COLLECTIONS

        if isinstance(geometry, (Polygon, Point)):
            geometry = json.dumps(mapping(geometry))
        elif isinstance(geometry, str):
            geometry = json.dumps(mapping(shape(geometry)))

        if geometry is None and tile is not None:
            geometry = self.tile_grid.centroid(tile)

        if IDs is None:
            ID_message = ""
        else:
            ID_message = f" with IDs: {', '.join(IDs)}"

        logger.info(f"searching {', '.join(collections)} at {tile} from {start_UTC} to {end_UTC}{ID_message}")

        attempt_count = 0

        while attempt_count < self.retries:
            attempt_count += 1

            try:
                search_results = HLS_CMR_query(
                    tile=tile,
                    start_date=start_UTC,
                    end_date=end_UTC,
                    page_size=page_size
                )
                break
            except Exception as e:
                logger.warning(e)
                logger.warning(f"HLS connection attempt {attempt_count} failed: {self.remote}")

                if attempt_count < self.retries:
                    sleep(self.wait_seconds)
                    logger.warning(f"re-trying HLS server: {self.remote}")
                    continue
                else:
                    raise HLSServerUnreachable(f"HLS server un-reachable: {self.remote}")

        self._URLs = pd.concat([self._URLs, search_results]).drop_duplicates()

        return search_results

    def dates_listed(self, tile: str) -> Set[date]:
        return set(self._listing[self._listing.tile == tile].date_UTC.apply(lambda date_UTC: parser.parse(str(date_UTC)).date()))

    def listing(
            self,
            tile: str,
            start_UTC: Union[date, str],
            end_UTC: Union[date, str] = None,
            page_size: int = PAGE_SIZE) -> (pd.DataFrame, pd.DataFrame):
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

        if set(date_range(start_UTC, end_UTC)) <= self.dates_listed(tile):
            listing_subset = self._listing[self._listing.tile == tile]
            listing_subset = listing_subset[listing_subset.date_UTC.apply(lambda date_UTC: parser.parse(str(date_UTC)).date() >= start_UTC and parser.parse(str(date_UTC)).date() <= end_UTC)]
            listing_subset = listing_subset.sort_values(by="date_UTC")

            return listing_subset

        self.logger.info(
            f"started listing available HLS2 granules at tile {colored_logging.place(tile)} from {colored_logging.time(start_UTC)} to {colored_logging.time(end_UTC)}")

        giveup_date = datetime.utcnow().date() - timedelta(days=GIVEUP_DAYS)
        search_start = start_UTC - timedelta(days=max(SENTINEL_REPEAT_DAYS, LANDSAT_REPEAT_DAYS))
        search_end = end_UTC

        URLs = self.search(
            tile=tile,
            start_UTC=search_start,
            end_UTC=search_end,
            page_size=page_size
        )

        sentinel_IDs = URLs[URLs.sensor == "S30"].groupby("ID").first().reset_index()[
            ["date_UTC", "tile", "ID"]].rename(columns={"ID": "sentinel"})
        sentinel_IDs.date_UTC = sentinel_IDs.date_UTC.apply(
            lambda date_UTC: parser.parse(str(date_UTC)).strftime("%Y-%m-%d"))
        sentinel_dates = set(
            sentinel_IDs.date_UTC.apply(lambda date_UTC: parser.parse(str(date_UTC)).date().strftime("%Y-%m-%d")))
        landsat_IDs = URLs[URLs.sensor == "L30"].groupby("ID").first().reset_index()[["date_UTC", "tile", "ID"]].rename(
            columns={"ID": "landsat"})
        landsat_IDs.date_UTC = landsat_IDs.date_UTC.apply(
            lambda date_UTC: parser.parse(str(date_UTC)).strftime("%Y-%m-%d"))
        landsat_dates = set(
            landsat_IDs.date_UTC.apply(lambda date_UTC: parser.parse(str(date_UTC)).date().strftime("%Y-%m-%d")))
        dates = pd.DataFrame(
            {"date_UTC": [start_UTC + timedelta(days=days) for days in range((end_UTC - start_UTC).days + 1)],
             "tile": tile})
        dates.date_UTC = dates.date_UTC.apply(lambda date_UTC: parser.parse(str(date_UTC)).strftime("%Y-%m-%d"))
        HLS_IDs = pd.merge(landsat_IDs, sentinel_IDs, how="outer")
        listing = pd.merge(dates, HLS_IDs, how="left")
        listing["sentinel_available"] = listing.apply(lambda row: not pd.isna(row.sentinel), axis=1)

        date_list = list(listing.date_UTC)
        sentinel_dates_expected = set()

        for d in date_list:
            if d in sentinel_dates:
                sentinel_dates_expected.add(d)

            if (parser.parse(d).date() - timedelta(days=SENTINEL_REPEAT_DAYS)).strftime(
                    "%Y-%m-%d") in sentinel_dates_expected:
                sentinel_dates_expected.add(d)

        listing["sentinel_expected"] = listing.apply(
            lambda row: parser.parse(str(row.date_UTC)).date().strftime("%Y-%m-%d") in sentinel_dates_expected, axis=1)
        listing["sentinel_missing"] = listing.apply(
            lambda row: not row.sentinel_available and row.sentinel_expected and parser.parse(
                str(row.date_UTC)) >= parser.parse(str(giveup_date)),
            axis=1
        )
        listing["sentinel"] = listing.apply(lambda row: "missing" if row.sentinel_missing else row.sentinel, axis=1)
        listing["landsat_available"] = listing.apply(lambda row: not pd.isna(row.landsat), axis=1)

        date_list = list(listing.date_UTC)
        landsat_dates_expected = set()

        for d in date_list:
            if d in landsat_dates:
                landsat_dates_expected.add(d)

            if (parser.parse(d).date() - timedelta(days=LANDSAT_REPEAT_DAYS)).strftime(
                    "%Y-%m-%d") in landsat_dates_expected:
                landsat_dates_expected.add(d)

        listing["landsat_expected"] = listing.apply(
            lambda row: parser.parse(str(row.date_UTC)).date().strftime("%Y-%m-%d") in landsat_dates_expected, axis=1)
        listing["landsat_missing"] = listing.apply(
            lambda row: not row.landsat_available and row.landsat_expected and parser.parse(
                str(row.date_UTC)) >= parser.parse(str(giveup_date)),
            axis=1
        )

        listing["landsat"] = listing.apply(lambda row: "missing" if row.landsat_missing else row.landsat, axis=1)
        listing = listing[["date_UTC", "tile", "sentinel", "landsat"]]

        self.logger.info(
            f"finished listing available HLS2 granules at tile {colored_logging.place(tile)} from {colored_logging.time(start_UTC)} to {colored_logging.time(end_UTC)} ({timer})")

        self._listing = pd.concat([self._listing, listing]).drop_duplicates()

        return listing

    def bands(self, ID: str, bands: List[str] = None, protocol: str = "https"):
        # df = pd.DataFrame([(key, value.href) for key, value in self.assets(ID).items()], columns=["band", "URL"])
        # FIXME
        df = self._URLs[self._URLs.ID == ID][["band", "https"]].rename(columns={protocol: "URL"})

        if bands is not None:
            df = df[df.band.apply(lambda band: band in bands)]

        return df

    def sentinel_ID(self, tile: str, date_UTC: Union[date, str]) -> str:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        # print(date_UTC)
        listing = self.listing(tile=tile, start_UTC=date_UTC, end_UTC=date_UTC)
        # print(listing)
        ID = str(listing.iloc[-1].sentinel)
        # print(ID)

        if ID == "nan":
            self.mark_date_unavailable("Sentinel", tile, date_UTC)
            raise HLSSentinelNotAvailable(f"Sentinel is not available at tile {colored_logging.place(tile)} on {colored_logging.time(date_UTC)}")
        elif ID == "missing":
            raise HLSSentinelMissing(
                f"Sentinel is missing on remote server at tile {colored_logging.place(tile)} on {colored_logging.time(date_UTC)}")
        else:
            return ID

    def landsat_ID(self, tile: str, date_UTC: Union[date, str]) -> str:
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        listing = self.listing(tile=tile, start_UTC=date_UTC, end_UTC=date_UTC)
        ID = str(listing.iloc[-1].landsat)

        if ID == "nan":
            self.mark_date_unavailable("Landsat", tile, date_UTC)
            error_string = f"Landsat is not available at tile {colored_logging.place(tile)} on {colored_logging.time(date_UTC)}"
            most_recent_listing = listing[listing.landsat.apply(lambda landsat: landsat not in ("nan", "missing"))]

            if len(most_recent_listing) > 0:
                most_recent = most_recent_listing.iloc[-1].landsat
                error_string += f" most recent granule: {colored_logging.val(most_recent)}"

            raise HLSLandsatNotAvailable(error_string)
        elif ID == "missing":
            raise HLSLandsatMissing(
                f"Landsat is missing on remote server at tile {colored_logging.place(tile)} on {colored_logging.time(date_UTC)}")
        else:
            return ID