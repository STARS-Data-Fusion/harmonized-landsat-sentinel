from typing import Optional, Union, List
from os.path import join, expanduser
import logging
from datetime import date, datetime
from dateutil import parser

from rasters import RasterGeometry

BANDS = [
    "red",
    "green",
    "blue",
    "NIR",
    "SWIR1",
    "SWIR2"
]

logger = logging.getLogger(__name__)

def timeseries(
    bands: Optional[Union[List[str], str]] = None,
    tile: Optional[str] = None,
    geometry: Optional[RasterGeometry] = None,
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
    download_directory: Optional[str] = None,
    output_directory: Optional[str] = None) -> None:
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
        start_date = parser.parse(start_date).date()
    if isinstance(end_date, str):
        end_date = parser.parse(end_date).date()

    if bands is None:
        bands = BANDS
    elif isinstance(bands, str):
        bands = [bands]

    logger.info("Generating HLS timeseries with parameters:")
    logger.info(f"  Bands: {', '.join(bands)}")
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
    ).dropna(how="all", subset=["sentinel", "landsat"])

    dates_available = sorted(listing.date_UTC)

    if len(dates_available) == 0:
        raise Exception(f"no dates available for tile {tile} in the date range {start_date} to {end_date}")

    logger.info(f"{len(dates_available)} dates available:")

    for d in dates_available:
        logger.info(f"  * {d}")
    
    for d in dates_available:
        d = parser.parse(d).date()

        for band in bands:
            logger.info(f"extracting band {bands} for date {d}")

            try:
                image = HLS.product(
                    product=band,
                    date_UTC=d,
                    tile=tile
                )

                if geometry is not None:
                    image = image.to_geometry(geometry)

            except Exception as e:
                logger.error(e)
                continue
            
            filename = join(
                output_directory,
                f"HLS_{band}_{tile}_{d.strftime('%Y%m%d')}.tif"
            )

            logger.info(f"writing image to {filename}")
            image.to_geotiff(expanduser(filename))
    