from .HLS_landsat_granule import HLSLandsatGranule
from .HLS2_granule import HLS2Granule

class HLS2LandsatGranule(HLS2Granule, HLSLandsatGranule):
    pass
