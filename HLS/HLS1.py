import logging
import os
import posixpath
from abc import abstractmethod
from datetime import datetime, date, timedelta
from glob import glob
from os import makedirs, system
from os.path import exists, dirname, abspath, join, getsize, basename
from shutil import move
from typing import List, Union

import numpy as np
import pandas as pd
import rasterio
from dateutil import parser

import colored_logging as cl
import rasters as rt
from HLS import HLS, HLSGranule, HLSSentinelGranule, HLSLandsatGranule, HLSGranuleID, HLSTileNotAvailable, \
    HLSLandsatNotAvailable, HLSLandsatMissing, HLSDownloadFailed, HLSSentinelNotAvailable, HLSSentinelMissing, \
    HLSNotAvailable
from daterange import date_range
from rasters import Raster, MultiRaster
from timer import Timer

with open(join(abspath(dirname(__file__)), "version.txt")) as f:
    version = f.read()

__version__ = version
__author__ = "Gregory H. Halverson"

DEFAULT_REMOTE = "https://hls.gsfc.nasa.gov/data/v1.4/"
DEFAULT_WORKING_DIRECTORY = "."
DEFAULT_DOWNLOAD_DIRECTORY = "HLS1_download"
DEFAULT_PRODUCTS_DIRECTORY = "HLS1_products"
DEFAULT_TARGET_RESOLUTION = 30

logger = logging.getLogger(__name__)


class HLS1Granule(HLSGranule):
    def __init__(self, filename: str):
        super(HLS1Granule, self).__init__(filename)
        self.directory = filename
        self.ID = HLSGranuleID(basename(filename))

    def __repr__(self) -> str:
        return f"HLSGranule({self.filename})"

    def _repr_png_(self) -> bytes:
        return self.RGB._repr_png_()

    @property
    def subdatasets(self) -> List[str]:
        with rasterio.open(self.filename) as file:
            return sorted(list(file.subdatasets))

    def URI(self, band: str) -> str:
        return f'HDF4_EOS:EOS_GRID:"{self.filename}":Grid:{band}'

    def DN(self, band: str) -> Raster:
        if band in self.band_images:
            return self.band_images[band]

        image = Raster.open(self.URI(band))
        self.band_images[band] = image

        return image

    @property
    def QA(self) -> Raster:
        return self.DN("QA")

    @property
    def geometry(self):
        return self.QA.geometry

    @property
    def cloud(self) -> Raster:
        return (self.QA >> 1) & 1 == 1

    def band(self, band: str, apply_scale: bool = True, apply_cloud: bool = True) -> Raster:
        image = self.DN(band)

        if apply_scale:
            image = rt.where(image == -1000, np.nan, image * 0.0001)
            image = rt.where(image < 0, np.nan, image)

        if apply_cloud:
            image = rt.where(self.cloud, np.nan, image)

        return image

    @property
    @abstractmethod
    def red(self) -> Raster:
        pass

    @property
    @abstractmethod
    def green(self) -> Raster:
        pass

    @property
    @abstractmethod
    def blue(self) -> Raster:
        pass

    @property
    @abstractmethod
    def NIR(self) -> Raster:
        pass

    @property
    def RGB(self) -> MultiRaster:
        return MultiRaster.stack([self.red, self.green, self.blue])

    @property
    def NDVI(self) -> Raster:
        return (self.NIR - self.red) / (self.NIR + self.red)

    @property
    @abstractmethod
    def albedo(self) -> Raster:
        pass


class HLS1SentinelGranule(HLS1Granule, HLSSentinelGranule):
    pass


class HLS1LandsatGranule(HLS1Granule, HLSLandsatGranule):
    def band_name(self, band: Union[str, int]) -> str:
        if isinstance(band, int):
            band = f"band{band:02d}"

        return band


class HLS1(HLS):
    logger = logging.getLogger(__name__)
    DEFAULT_REMOTE = DEFAULT_REMOTE
    DEFAULT_WORKING_DIRECTORY = DEFAULT_WORKING_DIRECTORY
    DEFAULT_DOWNLOAD_DIRECTORY = DEFAULT_DOWNLOAD_DIRECTORY
    DEFAULT_PRODUCTS_DIRECTORY = DEFAULT_PRODUCTS_DIRECTORY
    DEFAULT_TARGET_RESOLUTION = DEFAULT_TARGET_RESOLUTION

    def __init__(
            self,
            working_directory: str = None,
            download_directory: str = None,
            products_directory: str = None,
            target_resolution: int = None,
            remote: str = DEFAULT_REMOTE):
        if target_resolution is None:
            target_resolution = self.DEFAULT_TARGET_RESOLUTION

        super(HLS1, self).__init__(
            working_directory=working_directory,
            download_directory=download_directory,
            products_directory=products_directory,
            target_resolution=target_resolution
        )

        self.remote = remote
        self.check_remote()

    def year_directory(self, year: int, sensor: str) -> str:
        return posixpath.join(self.remote, sensor, f"{year:04d}")

    def tile_directory(self, tile: str, year: int, sensor: str) -> str:
        UTM_zone = int(tile[:2])
        first_letter = tile[2]
        second_letter = tile[3]
        third_letter = tile[4]
        year_directory = self.year_directory(year=year, sensor=sensor)
        tile_directory = posixpath.join(year_directory, f"{UTM_zone:02d}", first_letter, second_letter,
                                        third_letter)

        okay_statuses = (200, 301)

        if self.status(year_directory) in okay_statuses and self.status(tile_directory) not in okay_statuses:
            raise HLSTileNotAvailable(f"HLS2 tile not available tile: {tile} year: {year} sensor: {sensor}")

        return tile_directory

    def sentinel_directory(self, tile: str, year: int) -> str:
        return self.tile_directory(tile=tile, year=year, sensor="S30")

    def landsat_directory(self, tile: str, year: int) -> str:
        return self.tile_directory(tile=tile, year=year, sensor="L30")

    def directory_listing(self, tile: str, year: int, sensor: str) -> pd.DataFrame:
        tile_directory = self.tile_directory(tile=tile, year=year, sensor=sensor)
        filenames = self.HTTP_listing(tile_directory, "*.hdf")
        df = pd.DataFrame({"filename": filenames})
        df["tile"] = df.filename.apply(lambda sentinel: sentinel.split(".")[2][1:])
        df["date_UTC"] = df.filename.apply(lambda sentinel: datetime.strptime(sentinel.split(".")[3], "%Y%j"))
        df = df[["date_UTC", "tile", "filename"]]

        return df

    def sentinel_listing(self, tile: str, year: int) -> pd.DataFrame:
        return self.directory_listing(tile=tile, year=year, sensor="S30").rename({"filename": "sentinel"},
                                                                                 axis="columns")

    def landsat_listing(self, tile: str, year: int) -> pd.DataFrame:
        return self.directory_listing(tile=tile, year=year, sensor="L30").rename({"filename": "landsat"},
                                                                                 axis="columns")

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
        self.logger.info(
            f"finished listing available HLS2 granules at tile {cl.place(tile)} from {cl.time(start_UTC)} to {cl.time(end_UTC)} ({timer})")

        return listing

    def sentinel_filename(self, tile: str, date_UTC: Union[date, str]):
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        listing = self.listing(tile=tile, start_UTC=(date_UTC - timedelta(days=5)), end_UTC=date_UTC)
        filename = str(listing.iloc[-1].sentinel)

        if filename == "nan":
            # self.logger.error(listing[["date_UTC", "sentinel"]])
            self.mark_date_unavailable("Sentinel", tile, date_UTC)
            raise HLSSentinelNotAvailable(f"Sentinel is not available at tile {cl.place(tile)} on {cl.time(date_UTC)}")
        elif filename == "missing":
            # self.logger.error(listing[["date_UTC", "sentinel"]])
            raise HLSSentinelMissing(
                f"Sentinel is missing on remote server at tile {cl.place(tile)} on {cl.time(date_UTC)}")
        else:
            return filename

    def sentinel_URL(self, tile: str, date_UTC: Union[date, str]):
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        directory = self.sentinel_directory(tile=tile, year=date_UTC.year)
        filename = self.sentinel_filename(tile=tile, date_UTC=date_UTC)
        URL = posixpath.join(directory, filename)

        return URL

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

    def landsat_URL(self, tile: str, date_UTC: Union[date, str]):
        if isinstance(date_UTC, str):
            date_UTC = parser.parse(date_UTC).date()

        directory = self.landsat_directory(tile=tile, year=date_UTC.year)
        filename = self.landsat_filename(tile=tile, date_UTC=date_UTC)
        URL = posixpath.join(directory, filename)

        return URL

    def download_file(self, URL: str, filename: str):
        if exists(filename) and getsize(filename) == 0:
            logger.warning(f"removing zero-size corrupted HLS2 file: {filename}")
            os.remove(filename)

        if exists(filename):
            self.logger.info(f"file already downloaded: {cl.file(filename)}")
            return filename

        self.logger.info(f"downloading: {cl.URL(URL)} -> {cl.file(filename)}")
        directory = dirname(filename)
        makedirs(directory, exist_ok=True)
        partial_filename = f"{filename}.download"
        command = f'wget -c -O "{partial_filename}" "{URL}"'
        timer = Timer()
        system(command)
        self.logger.info(f"completed download in {cl.time(timer)} seconds: " + cl.file(filename))

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

    def sentinel(self, tile: str, date_UTC: Union[date, str]) -> HLS1SentinelGranule:
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

        granule = HLS1SentinelGranule(filename)

        return granule

    def landsat(self, tile: str, date_UTC: Union[date, str]) -> HLS1LandsatGranule:
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

        granule = HLS1LandsatGranule(filename)

        return granule

    def NDVI(
            self,
            tile: str,
            date_UTC: Union[date, str],
            product_filename: str = None,
            preview_filename: str = None,
            save_data: bool = True,
            save_preview: bool = True,
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
                self.logger.info(f"loading HLS2 NDVI: {cl.file(product_filename)}")
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
            NDVI = sentinel.NDVI
        elif sentinel is None and landsat is not None:
            NDVI = landsat.NDVI
        else:
            NDVI = rt.Raster(np.nanmean(np.dstack([sentinel.NDVI, landsat.NDVI]), axis=2), geometry=sentinel.geometry)

        if self.target_resolution > 30:
            NDVI = NDVI.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            NDVI = NDVI.to_geometry(geometry, resampling="cubic")

        if (save_data or return_filename) and not exists(product_filename):
            self.logger.info(f"saving HLS2 NDVI: {cl.file(product_filename)}")
            NDVI.to_COG(product_filename)

            if save_preview:
                self.logger.info(f"saving HLS2 NDVI preview: {cl.file(preview_filename)}")
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
            save_data: bool = True,
            save_preview: bool = True,
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
                self.logger.info(f"loading HLS2 albedo: {cl.file(product_filename)}")
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

        if (save_data or return_filename) and not exists(product_filename):
            self.logger.info(f"saving HLS2 albedo: {cl.file(product_filename)}")
            albedo.to_COG(product_filename)

            if save_preview:
                self.logger.info(f"saving HLS2 albedo preview: {cl.file(preview_filename)}")
                albedo.to_geojpeg(preview_filename)

        albedo = albedo.to_geometry(target_geometry)

        if return_filename:
            return product_filename

        return albedo
