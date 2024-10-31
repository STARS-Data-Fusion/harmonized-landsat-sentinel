import rasters as rt
from rasters import Raster

from .constants import *
from .HLS_granule import HLSGranule

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
