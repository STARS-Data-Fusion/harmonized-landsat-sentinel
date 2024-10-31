from abc import abstractmethod
from os.path import basename
from typing import List

import rasterio

import numpy as np
import rasters as rt
from rasters import Raster, MultiRaster

from .HLS_granule_ID import HLSGranuleID
from .HLS_granule import HLSGranule


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
