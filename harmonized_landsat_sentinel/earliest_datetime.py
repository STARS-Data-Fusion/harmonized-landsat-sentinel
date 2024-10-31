from datetime import date, datetime
from typing import Union
from dateutil import parser

def earliest_datetime(date_in: Union[date, str]) -> datetime:
    if isinstance(date_in, str):
        datetime_in = parser.parse(date_in)
    else:
        datetime_in = date_in

    date_string = datetime_in.strftime("%Y-%m-%d")
    return parser.parse(f"{date_string}T00:00:00Z")
