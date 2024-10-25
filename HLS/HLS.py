import logging
import os
import urllib
import warnings
from abc import abstractmethod
from datetime import datetime, date, timedelta
from fnmatch import fnmatch
from glob import glob
from os import makedirs, system
from os.path import exists, dirname, abspath, expanduser, join, getsize
from shutil import move
from typing import List, Union

import numpy as np
import pandas as pd
import rasterio
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from matplotlib.colors import LinearSegmentedColormap

import colored_logging as cl
import rasters as rt
from daterange import date_range
from rasters import Raster, MultiRaster, SpatialGeometry, RasterGeometry
from sentinel_tile_grid import SentinelTileGrid
from timer import Timer
from DAAC.HTTP import CONNECTION_CLOSE

with open(join(abspath(dirname(__file__)), "version.txt")) as f:
    version = f.read()

__version__ = version
__author__ = "Gregory H. Halverson"

DEFAULT_REMOTE = "https://hls.gsfc.nasa.gov/data/v1.4/"
DEFAULT_WORKING_DIRECTORY = "."
DEFAULT_DOWNLOAD_DIRECTORY = "HLS_download"
DEFAULT_PRODUCTS_DIRECTORY = "HLS_products"
DEFAULT_TARGET_RESOLUTION = 30
DEFAULT_PRODUCTS = ["NIR", "red"]

NDVI_CMAP = LinearSegmentedColormap.from_list(
    name="NDVI",
    colors=[
        (0, "#0000ff"),
        (0.4, "#000000"),
        (0.5, "#745d1a"),
        (0.6, "#e1dea2"),
        (0.8, "#45ff01"),
        (1, "#325e32")
    ]
)

ALBEDO_CMAP = LinearSegmentedColormap.from_list(name="albedo", colors=["black", "white"])
RED_CMAP = LinearSegmentedColormap.from_list(name="red", colors=["black", "red"])
BLUE_CMAP = LinearSegmentedColormap.from_list(name="blue", colors=["black", "blue"])
GREEN_CMAP = LinearSegmentedColormap.from_list(name="green", colors=["black", "green"])
WATER_CMAP = LinearSegmentedColormap.from_list(name="water", colors=["black", "#0eeded"])
CLOUD_CMAP = LinearSegmentedColormap.from_list(name="water", colors=["black", "white"])

logger = logging.getLogger(__name__)

class HLSServerUnreachable(ConnectionError):
    pass


class HLSNotAvailable(Exception):
    pass


class HLSSentinelNotAvailable(Exception):
    pass


class HLSSentinelMissing(Exception):
    pass


class HLSLandsatNotAvailable(Exception):
    pass


class HLSLandsatMissing(Exception):
    pass


class HLSTileNotAvailable(Exception):
    pass


class HLSDownloadFailed(ConnectionError):
    pass


class HLSGranuleID:
    def __init__(self, ID: str):
        parts = ID.split(".")
        self.sensor = parts[1]
        self.tile = parts[2][1:]
        self.timestamp = parts[3]
        self.version = ".".join(parts[4:])

    def __repr__(self) -> str:
        return f"{self.sensor}.T{self.tile}.{self.timestamp}.{self.version}"


class HLSGranule:
    def __init__(self, filename: str):
        self.filename = filename
        self.band_images = {}

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

    def band_name(self, band: Union[str, int]) -> str:
        if isinstance(band, int):
            band = f"B{band:02d}"

        return band

    def band(self, band: Union[str, int], apply_scale: bool = True, apply_cloud: bool = True) -> Raster:
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
    @abstractmethod
    def SWIR1(self) -> Raster:
        pass

    @property
    @abstractmethod
    def SWIR2(self) -> Raster:
        pass

    @property
    def RGB(self) -> MultiRaster:
        return MultiRaster.stack([self.red, self.green, self.blue])

    @property
    def true(self) -> MultiRaster:
        return self.RGB

    @property
    def false_urban(self) -> MultiRaster:
        return MultiRaster.stack([self.SWIR2, self.SWIR1, self.red])

    @property
    def false_vegetation(self) -> MultiRaster:
        return MultiRaster.stack([self.NIR, self.red, self.green])

    @property
    def false_healthy(self) -> MultiRaster:
        return MultiRaster.stack([self.NIR, self.SWIR1, self.blue])

    @property
    def false_agriculture(self) -> MultiRaster:
        return MultiRaster.stack([self.SWIR1, self.NIR, self.blue])

    @property
    def false_water(self) -> MultiRaster:
        return MultiRaster.stack([self.NIR, self.SWIR1, self.red])

    @property
    def false_geology(self) -> MultiRaster:
        return MultiRaster.stack([self.SWIR2, self.SWIR1, self.blue])

    @property
    def NDVI(self) -> Raster:
        image = (self.NIR - self.red) / (self.NIR + self.red)
        image.cmap = NDVI_CMAP

        return image

    @property
    @abstractmethod
    def albedo(self) -> Raster:
        pass

    @property
    def NDSI(self) -> Raster:
        warnings.filterwarnings("ignore")
        NDSI = (self.green - self.SWIR1) / (self.green + self.SWIR1)
        NDSI = rt.clip(NDSI, -1, 1)
        NDSI = NDSI.astype(np.float32)
        NDSI = NDSI.color("jet")

        return NDSI

    @property
    def MNDWI(self) -> Raster:
        warnings.filterwarnings("ignore")
        MNDWI = (self.green - self.SWIR1) / (self.green + self.SWIR1)
        MNDWI = rt.clip(MNDWI, -1, 1)
        MNDWI = MNDWI.astype(np.float32)
        MNDWI = MNDWI.color("jet")

        return MNDWI

    @property
    def NDWI(self) -> Raster:
        warnings.filterwarnings("ignore")
        NDWI = (self.green - self.NIR) / (self.green + self.NIR)
        NDWI = rt.clip(NDWI, -1, 1)
        NDWI = NDWI.astype(np.float32)
        NDWI = NDWI.color("jet")

        return NDWI

    # @property
    # def WRI(self) -> Raster:
    #     warnings.filterwarnings("ignore")
    #     WRI = (self.green + self.red) / (self.NIR + self.SWIR1)
    #     WRI = WRI.astype(np.float32)
    #     WRI = WRI.color("jet")
    #
    #     return WRI

    @property
    def moisture(self) -> Raster:
        warnings.filterwarnings("ignore")
        moisture = (self.NIR - self.SWIR1) / (self.NIR + self.SWIR1)
        moisture = rt.clip(moisture, -1, 1)
        moisture = moisture.astype(np.float32)
        moisture = moisture.color("jet")

        return moisture

    def product(self, product: str) -> Raster:
        return getattr(self, product)

class HLSSentinelGranule(HLSGranule):
    @property
    def coastal_aerosol(self) -> Raster:
        return self.band(1)

    @property
    def blue(self) -> Raster:
        return self.band(2).color(BLUE_CMAP)

    @property
    def green(self) -> Raster:
        return self.band(3).color(GREEN_CMAP)

    @property
    def red(self) -> Raster:
        return self.band(4).color(RED_CMAP)

    @property
    def rededge1(self) -> Raster:
        return self.band(5)

    @property
    def rededge2(self) -> Raster:
        return self.band(6)

    @property
    def rededge3(self) -> Raster:
        return self.band(7)

    @property
    def NIR_broad(self) -> Raster:
        return self.band(8)

    @property
    def NIR(self) -> Raster:
        return self.band("B8A")

    @property
    def SWIR1(self) -> Raster:
        return self.band(11)

    @property
    def SWIR2(self) -> Raster:
        return self.band(12)

    @property
    def water_vapor(self) -> Raster:
        return self.band(9)

    @property
    def cirrus(self) -> Raster:
        return self.band(10)

    @property
    def albedo(self) -> Raster:
        albedo = \
            0.1324 * self.blue + \
            0.1269 * self.green + \
            0.1051 * self.red + \
            0.0971 * self.rededge1 + \
            0.0890 * self.rededge2 + \
            0.0818 * self.rededge3 + \
            0.0722 * self.NIR_broad + \
            0.0167 * self.SWIR1 + \
            0.0002 * self.SWIR2

        albedo.cmap = ALBEDO_CMAP

        return albedo

    @property
    def false_bathymetric(self) -> MultiRaster:
        return MultiRaster.stack([self.red, self.green, self.coastal_aerosol])


class HLSLandsatGranule(HLSGranule):
    @property
    def coastal_aerosol(self) -> Raster:
        return self.band(1)

    @property
    def blue(self) -> Raster:
        blue = self.band(2)
        blue.cmap = BLUE_CMAP

        return blue

    @property
    def green(self) -> Raster:
        green = self.band(3)
        green.cmap = GREEN_CMAP

        return green

    @property
    def red(self) -> Raster:
        red = self.band(4)
        red.cmap = RED_CMAP

        return red

    @property
    def NIR(self) -> Raster:
        return self.band(5)

    @property
    def SWIR1(self) -> Raster:
        return self.band(6)

    @property
    def SWIR2(self) -> Raster:
        return self.band(7)

    @property
    def cirrus(self) -> Raster:
        return self.band(9)

    @property
    def albedo(self) -> Raster:
        albedo = ((0.356 * self.blue) + (0.130 * self.green) + (0.373 * self.red) + (0.085 * self.NIR) + (
                0.072 * self.SWIR1) - 0.018) / 1.016
        albedo.cmap = ALBEDO_CMAP

        return albedo


class HLS:
    logger = logging.getLogger(__name__)

    DEFAULT_WORKING_DIRECTORY = DEFAULT_WORKING_DIRECTORY
    DEFAULT_DOWNLOAD_DIRECTORY = DEFAULT_DOWNLOAD_DIRECTORY
    DEFAULT_PRODUCTS_DIRECTORY = DEFAULT_PRODUCTS_DIRECTORY
    DEFAULT_TARGET_RESOLUTION = DEFAULT_TARGET_RESOLUTION
    DEFAULT_TARGET_RESOLUTION = DEFAULT_TARGET_RESOLUTION

    def __init__(
            self,
            working_directory: str = None,
            download_directory: str = None,
            products_directory: str = None,
            target_resolution: int = None):
        if target_resolution is None:
            target_resolution = self.DEFAULT_TARGET_RESOLUTION

        if working_directory is None:
            working_directory = self.DEFAULT_WORKING_DIRECTORY

        if download_directory is None:
            download_directory = join(working_directory, self.DEFAULT_DOWNLOAD_DIRECTORY)

        if products_directory is None:
            products_directory = join(working_directory, self.DEFAULT_PRODUCTS_DIRECTORY)

        self.working_directory = abspath(expanduser(working_directory))
        self.download_directory = abspath(expanduser(download_directory))
        self.products_directory = abspath(expanduser(products_directory))
        self.target_resolution = target_resolution
        self.tile_grid = SentinelTileGrid(target_resolution=target_resolution)
        self._listings = {}
        self.unavailable_dates = {}
        self.remote = None

    def __repr__(self):
        return f'{self.__class__.__name__}(\n' + \
               f'\tworking_directory="{self.working_directory}",\n' + \
               f'\tdownload_directory="{self.download_directory}",\n' + \
               f'\tproducts_directory="{self.products_directory}",\n' + \
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
        self.logger.info(f"checking URL: {cl.URL(URL)}")

        try:
            response = requests.head(URL, headers=CONNECTION_CLOSE)
            status = response.status_code
            duration = response.elapsed.total_seconds()
        except Exception as e:
            self.logger.exception(e)
            raise HLSServerUnreachable(f"unable to connect to URL: {URL}")

        if status in (200, 301):
            self.logger.info(
                "URL verified with status " + cl.val(200) +
                " in " + cl.time(f"{duration:0.2f}") +
                " seconds: " + cl.URL(URL)
            )
        else:
            self.logger.warning(
                "URL not available with status " + cl.val(status) +
                " in " + cl.time(f"{duration:0.2f}") +
                " seconds: " + cl.URL(URL)
            )

        return status

    def check_remote(self):
        #FIXME re-try a couple times if you get 503

        self.logger.info(f"checking URL: {cl.URL(self.remote)}")

        try:
            response = requests.head(self.remote, headers=CONNECTION_CLOSE)
            status = response.status_code
            duration = response.elapsed.total_seconds()
        except Exception as e:
            self.logger.exception(e)
            raise HLSServerUnreachable(f"unable to connect to URL: {self.remote}")

        if status == 200:
            self.logger.info(
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

    def product(
            self,
            product: str,
            tile: str,
            date_UTC: Union[date, str],
            geometry: RasterGeometry = None,
            product_filename: str = None,
            preview_filename: str = None,
            save_data: bool = True,
            save_preview: bool = True,
            return_filename: bool = False) -> Union[Raster, str]:
        target_tile = tile
        target_geometry = self.grid(target_tile)
        tile = tile[:5]

        if geometry is None:
            geometry = self.grid(tile)

        if product_filename is None:
            product_filename = self.product_filename(
                product=product,
                date_UTC=date_UTC,
                tile=tile
            )

        if preview_filename is None:
            preview_filename = product_filename.replace(".tif", ".jpeg")

        if exists(product_filename):
            if return_filename:
                return product_filename
            else:
                self.logger.info(f"loading HLS2 {product}: {cl.file(product_filename)}")
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
            image = sentinel.product(product)
        elif sentinel is None and landsat is not None:
            image = landsat.product(product)
        else:
            image = rt.Raster(np.nanmean(np.dstack([sentinel.NDVI, landsat.NDVI]), axis=2), geometry=sentinel.geometry)

        if self.target_resolution > 30:
            image = image.to_geometry(geometry, resampling="average")
        elif self.target_resolution < 30:
            image = image.to_geometry(geometry, resampling="cubic")

        if (save_data or return_filename) and not exists(product_filename):
            self.logger.info(f"saving HLS2 {product}: {cl.file(product_filename)}")
            image.to_COG(product_filename)

            if save_preview:
                self.logger.info(f"saving HLS2 {product} preview: {cl.file(preview_filename)}")
                image.to_geojpeg(preview_filename)

        image = image.to_geometry(target_geometry)

        if return_filename:
            return product_filename
        else:
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

        if (save_data and return_filename) and not exists(product_filename):
            self.logger.info(f"saving HLS2 albedo: {cl.file(product_filename)}")
            albedo.to_COG(product_filename)

            if save_preview:
                self.logger.info(f"saving HLS2 albedo preview: {cl.file(preview_filename)}")
                albedo.to_geojpeg(preview_filename)

        albedo = albedo.to_geometry(target_geometry)

        if return_filename:
            return product_filename

        return albedo
