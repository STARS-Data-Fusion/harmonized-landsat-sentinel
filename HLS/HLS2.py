from os.path import join, abspath, dirname
import logging

with open(join(abspath(dirname(__file__)), "version.txt")) as f:
    version = f.read()

__version__ = version
__author__ = "Gregory H. Halverson, Evan Davis"

logger = logging.getLogger(__name__)

_AUTH = None


