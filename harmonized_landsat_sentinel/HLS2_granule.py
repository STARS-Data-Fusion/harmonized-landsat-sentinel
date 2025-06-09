from typing import List

from os.path import basename, join, abspath, expanduser

from glob import glob

import numpy as np
import rasters as rt
from rasters import Raster

from .constants import *
from .exceptions import *
from .HLS_granule import HLSGranule
from .HLS_granule_ID import HLSGranuleID

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
        pattern = join(abspath(expanduser(self.directory)), f"*.{band}.tif")
        filenames = sorted(glob(pattern))

        if len(filenames) == 0:
            raise HLSBandNotAcquired(f"no file found for band {band} for granule {self.ID} in directory: {self.directory}")

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
