import warnings
from abc import abstractmethod
from typing import List, Union

import numpy as np
import rasterio

import rasters as rt
from rasters import Raster, MultiRaster

from .constants import *


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
