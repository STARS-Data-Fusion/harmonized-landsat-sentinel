from typing import Union, List

from datetime import date
from dateutil import parser

import earthaccess

import pandas as pd

from .constants import *
from .earliest_datetime import earliest_datetime
from .exceptions import *
from .get_CMR_granule_ID import get_CMR_granule_ID
from .latest_datetime import latest_datetime


def HLS_CMR_query(
        tile: str,
        start_date: Union[date, str],
        end_date: Union[date, str],
        page_size: int = PAGE_SIZE) -> pd.DataFrame:
    """function to search for HLS at tile in date range"""
    granules: List[earthaccess.search.DataGranule]
    try:
        granules = earthaccess.granule_query() \
            .concept_id([L30_CONCEPT, S30_CONCEPT]) \
            .temporal(earliest_datetime(start_date), latest_datetime(end_date)) \
            .readable_granule_name(f"*.T{tile}.*") \
            .get()
    except Exception as e:
        raise CMRServerUnreachable(e)

    granules = sorted(granules, key=lambda granule: granule["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"])
    data = list(map(
        lambda granule: {
            "ID": get_CMR_granule_ID(granule),
            "sensor": get_CMR_granule_ID(granule).split(".")[1],
            "tile": get_CMR_granule_ID(granule).split(".")[2][1:],
            "date_UTC": parser.parse(granule["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]).date().strftime("%Y-%m-%d"),
            "timestamp_str": granule["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"],
            "granule": granule,
        },
        granules
    ))

    return pd.DataFrame(data, columns=["ID", "sensor", "tile", "date_UTC", "timestamp_str", "granule"])
