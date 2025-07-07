import logging
import netrc
import os

import earthaccess

from .exceptions import *

__author__ = "Evan Davis"

_AUTH = None

def login() -> earthaccess.Auth:
    """
    Login to Earthdata using environment variables if available, falling back to netrc credentials, then interactive login.
    """
    # Only login to earthaccess once
    global _AUTH
    if _AUTH is not None:
        return _AUTH

    # Check if we're in a testing environment where authentication should be skipped
    if os.environ.get("SKIP_EARTHDATA_LOGIN", "").lower() in ("true", "1", "yes"):
        # Return a mock auth object for testing
        class MockAuth:
            def __init__(self):
                self.authenticated = True
        _AUTH = MockAuth()
        return _AUTH

    # Temporarily suppress INFO logs from earthaccess during login
    earthaccess_logger = logging.getLogger('earthaccess')
    original_level = earthaccess_logger.level
    earthaccess_logger.setLevel(logging.WARNING)

    try:
        # First priority: environment variables
        if "EARTHDATA_USERNAME" in os.environ and "EARTHDATA_PASSWORD" in os.environ:
            _AUTH = earthaccess.login(strategy="environment")
            return _AUTH

        # Second priority: netrc credentials
        try:
            secrets = netrc.netrc()
            auth = secrets.authenticators("urs.earthdata.nasa.gov")
            if auth:
                _AUTH = earthaccess.login(strategy="netrc")
                return _AUTH
        except (FileNotFoundError, netrc.NetrcParseError):
            # .netrc file doesn't exist or is malformed, continue to interactive login
            pass

        # Last resort: interactive login
        _AUTH = earthaccess.login(strategy="interactive")
        return _AUTH

    except Exception as e:
        raise CMRServerUnreachable(e)
    finally:
        # Restore original logging level
        earthaccess_logger.setLevel(original_level)
