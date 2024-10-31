import datetime
import logging
from typing import List, Union

from dateutil import parser

logger = logging.getLogger(__name__)


def get_date(dt: Union[datetime.date, datetime.datetime, str]) -> datetime.date or None:
    if dt is None:
        return None
    elif isinstance(dt, datetime.datetime):
        return dt.date()
    elif isinstance(dt, datetime.date):
        return dt
    elif isinstance(dt, str):
        return parser.parse(dt).date()
    else:
        raise ValueError(f"invalid date type: {type(dt)}")


def date_range(start: Union[datetime.date, str], end: Union[datetime.date, str]) -> List[datetime.date]:
    start = get_date(start)
    end = get_date(end)

    try:
        count = (end - start).days
    except TypeError as e:
        logger.exception(e)
        raise TypeError(f"start type: {type(start)} end type: {type(end)}")

    dates = [start + datetime.timedelta(days=days) for days in range(count + 1)]

    return dates
