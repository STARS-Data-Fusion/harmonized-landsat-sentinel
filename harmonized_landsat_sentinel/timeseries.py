from typing import Optional, Union
import logging
from datetime import date, datetime
from dateutil import parser as date_parser

logger = logging.getLogger(__name__)

def timeseries(
    band: Optional[str] = None,
    tile: Optional[str] = None,
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
    download_directory: Optional[str] = None
) -> None:
    """
    Produce a timeseries of HLS data for the specified parameters.

    Args:
        band (Optional[str]): The spectral band to use (e.g., "B04").
        tile (Optional[str]): The HLS tile identifier (e.g., "10SEG").
        start_date (Optional[Union[str, date]]): Start date as YYYY-MM-DD string or date object.
        end_date (Optional[Union[str, date]]): End date as YYYY-MM-DD string or date object.
        download_directory (Optional[str]): Directory to save or read data.

    Returns:
        None
    """
    # Parse start_date and end_date if they are strings
    if isinstance(start_date, str):
        start_date = date_parser.parse(start_date).date()
    if isinstance(end_date, str):
        end_date = date_parser.parse(end_date).date()

    logger.info("Generating HLS timeseries with parameters:")
    logger.info(f"  Band: {band}")
    logger.info(f"  Tile: {tile}")
    logger.info(f"  Start date: {start_date}")
    logger.info(f"  End date: {end_date}")
    
    if download_directory is None:
        from harmonized_landsat_sentinel import harmonized_landsat_sentinel as HLS
    else:
        from harmonized_landsat_sentinel import HLS2EarthAccessConnection
        HLS = HLS2EarthAccessConnection(directory=download_directory)
    
    download_directory = HLS.download_directory
    logger.info(f"  Directory: {download_directory}")

    listing = HLS.listing(
        tile=tile,
        start_UTC=start_date,
        end_UTC=end_date
    )

    logger.info(f"{len(listing)} dates available:")

    dates_available = sorted(HLS.dates_listed(tile=tile))

    for d in dates_available:
        logger.info(f"  * {d}")
    