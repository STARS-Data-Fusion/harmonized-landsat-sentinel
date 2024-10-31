import logging
from os.path import dirname, abspath, join

from .HLS1_connection import HLS1Connection
from .HLS2_CMR_connection import HLS2CMRConnection

with open(join(abspath(dirname(__file__)), "version.txt")) as f:
    version = f.read()

__version__ = version
__author__ = "Gregory H. Halverson, Evan Davis"

logger = logging.getLogger(__name__)

HLS2Connection = HLS2CMRConnection
