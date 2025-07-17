
from typing import List
from os.path import basename, join, abspath, expanduser
from glob import glob
from abc import ABC, abstractmethod
import numpy as np
import rasters as rt
from rasters import Raster
import rasterio
import warnings
from .constants import *
from .exceptions import *
from .HLS_granule_ID import HLSGranuleID

class HLS2Granule(ABC):
    def __init__(self, directory: str, connection=None):
        self.directory = directory
        self.filename = directory  # For compatibility with HLSGranule interface
        self.ID = HLSGranuleID(basename(directory))
        self.connection = connection
        self.band_images = {}

    def __repr__(self) -> str:
        return f"HLS2Granule({self.directory})"

    def _repr_png_(self) -> bytes:
        return self.RGB._repr_png_()

    @property
    def filenames(self) -> List[str]:
        return sorted(glob(join(self.directory, f"*.*")))

    def band_name(self, band):
        if isinstance(band, int):
            band = f"B{band:02d}"
        return band

    def band_filename(self, band: str) -> str:
        band = self.band_name(band)
        pattern = join(abspath(expanduser(self.directory)), f"*.{band}.tif")
        filenames = sorted(glob(pattern))
        if len(filenames) == 0:
            raise HLSBandNotAcquired(f"no file found for band {band} for granule {self.ID} in directory: {self.directory}")
        return filenames[-1]

    def DN(self, band) -> Raster:
        if band in self.band_images:
            return self.band_images[band]
        # Only support GeoTIFF access in directory
        filename = self.band_filename(band)
        image = Raster.open(filename)
        self.band_images[band] = image
        return image

    @property
    def Fmask(self) -> Raster:
        return self.DN("Fmask")

    @property
    def QA(self) -> Raster:
        # For HLS2, QA is Fmask
        return self.Fmask

    @property
    def geometry(self):
        return self.QA.geometry

    @property
    def cloud(self) -> Raster:
        # For HLS2, cloud mask logic may differ; fallback to HLSGranule logic if needed
        return (self.QA & 15 > 0).color(CLOUD_CMAP)

    @property
    def water(self) -> Raster:
        return ((self.QA >> 5) & 1 == 1).color(WATER_CMAP)

    def band(self, band, apply_scale: bool = True, apply_cloud: bool = True) -> Raster:
        image = self.DN(band)
        if apply_scale:
            image = rt.where(image == -1000, np.nan, image * 0.0001)
            image = rt.where(image < 0, np.nan, image)
            image.nodata = np.nan
        if apply_cloud:
            image = rt.where(self.cloud, np.nan, image)
        return image

    @property
    def QA(self) -> Raster:
        return self.Fmask

    @property
    def red(self) -> Raster:
        return self.band("B04")

    @property
    def green(self) -> Raster:
        return self.band("B03")

    @property
    def blue(self) -> Raster:
        return self.band("B02")

    @property
    def NIR(self) -> Raster:
        return self.band("B08")

    @property
    def SWIR1(self) -> Raster:
        return self.band("B11")

    @property
    def SWIR2(self) -> Raster:
        return self.band("B12")

    @property
    def RGB(self):
        from rasters import MultiRaster
        return MultiRaster.stack([self.red, self.green, self.blue])

    @property
    def true(self):
        return self.RGB

    @property
    def false_urban(self):
        from rasters import MultiRaster
        return MultiRaster.stack([self.SWIR2, self.SWIR1, self.red])

    @property
    def false_vegetation(self):
        from rasters import MultiRaster
        return MultiRaster.stack([self.NIR, self.red, self.green])

    @property
    def false_healthy(self):
        from rasters import MultiRaster
        return MultiRaster.stack([self.NIR, self.SWIR1, self.blue])

    @property
    def false_agriculture(self):
        from rasters import MultiRaster
        return MultiRaster.stack([self.SWIR1, self.NIR, self.blue])

    @property
    def false_water(self):
        from rasters import MultiRaster
        return MultiRaster.stack([self.NIR, self.SWIR1, self.red])

    @property
    def false_geology(self):
        from rasters import MultiRaster
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
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            NDSI = (self.green - self.SWIR1) / (self.green + self.SWIR1)
            NDSI = rt.clip(NDSI, -1, 1)
            NDSI = NDSI.astype(np.float32)
            NDSI = NDSI.color("jet")
        return NDSI

    @property
    def MNDWI(self) -> Raster:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            MNDWI = (self.green - self.SWIR1) / (self.green + self.SWIR1)
            MNDWI = rt.clip(MNDWI, -1, 1)
            MNDWI = MNDWI.astype(np.float32)
            MNDWI = MNDWI.color("jet")
        return MNDWI

    @property
    def NDWI(self) -> Raster:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            NDWI = (self.green - self.NIR) / (self.green + self.NIR)
            NDWI = rt.clip(NDWI, -1, 1)
            NDWI = NDWI.astype(np.float32)
            NDWI = NDWI.color("jet")
        return NDWI

    @property
    def moisture(self) -> Raster:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            moisture = (self.NIR - self.SWIR1) / (self.NIR + self.SWIR1)
            moisture = rt.clip(moisture, -1, 1)
            moisture = moisture.astype(np.float32)
            moisture = moisture.color("jet")
        return moisture

    def product(self, product: str) -> Raster:
        return getattr(self, product)
