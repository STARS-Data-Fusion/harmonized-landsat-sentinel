from rasters import Raster, MultiRaster

from .constants import *
from .HLS_granule import HLSGranule

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
