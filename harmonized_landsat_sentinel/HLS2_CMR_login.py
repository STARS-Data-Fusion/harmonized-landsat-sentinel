import netrc
import os

import earthaccess

from .exceptions import *

_AUTH = None

def HLS2_CMR_login() -> earthaccess.Auth:
    """
    Login to Earthdata using netrc credentials if available, falling back to environment variables.
    """
    # Only login to earthaccess once
    global _AUTH
    if _AUTH is not None:
        return _AUTH

    try:
        # Attempt to use netrc for credentials
        secrets = netrc.netrc()
        auth = secrets.authenticators("urs.earthdata.nasa.gov")
        if auth:
            _AUTH = earthaccess.login(strategy="netrc")  # Use strategy="netrc"
            return _AUTH

        # Fallback to environment variables if netrc fails
        if "EARTHDATA_USERNAME" in os.environ and "EARTHDATA_PASSWORD" in os.environ:
            _AUTH = earthaccess.login(strategy="environment")
            return _AUTH
        else:
            raise CMRServerUnreachable("Missing netrc credentials or environment variables 'EARTHDATA_USERNAME' and 'EARTHDATA_PASSWORD'")

    except Exception as e:
        raise CMRServerUnreachable(e)
