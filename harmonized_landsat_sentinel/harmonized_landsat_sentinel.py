import logging
from os.path import dirname, abspath, join

from .constants import *
from .exceptions import *
from .daterange import *
from .earliest_datetime import *
from .get_CMR_granule_ID import *
from .HLS_CMR_query import *
from .HLS_granule_ID import *
from .HLS2_landsat_granule import *
from .HLS2_sentinel_granule import *
from .HLS2_granule import *
from .HLS2_landsat_granule import *
from .HLS2_sentinel_granule import *
from .HLS2_connection import *
from .latest_datetime import *
from .timer import *
from .generate_HLS_timeseries import *
from .login import *

from .version import version

__version__ = version
__author__ = "Gregory H. Halverson, Evan Davis"

logger = logging.getLogger(__name__)

_hls_connection = None


def get_harmonized_landsat_sentinel() -> HLS2Connection:
    global _hls_connection
    if _hls_connection is None:
        _hls_connection = HLS2Connection()
    return _hls_connection


class _LazyHLSConnection:
    def __getattr__(self, name):
        return getattr(get_harmonized_landsat_sentinel(), name)


harmonized_landsat_sentinel = _LazyHLSConnection()
