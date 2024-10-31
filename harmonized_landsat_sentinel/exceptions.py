class HLSServerUnreachable(ConnectionError):
    pass


class HLSNotAvailable(Exception):
    pass


class HLSSentinelNotAvailable(Exception):
    pass


class HLSSentinelMissing(Exception):
    pass


class HLSLandsatNotAvailable(Exception):
    pass


class HLSLandsatMissing(Exception):
    pass


class HLSTileNotAvailable(Exception):
    pass


class HLSDownloadFailed(ConnectionError):
    pass


class CMRServerUnreachable(Exception):
    pass


class HLSBandNotAcquired(IOError):
    pass
