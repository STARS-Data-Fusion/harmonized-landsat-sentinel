from typing import Union

from HLS import HLSLandsatGranule
from HLS.HLS1_granule import HLS1Granule


class HLS1LandsatGranule(HLS1Granule, HLSLandsatGranule):
    def band_name(self, band: Union[str, int]) -> str:
        if isinstance(band, int):
            band = f"band{band:02d}"

        return band
