from typing import Optional, Union, List
from os.path import join, expanduser
import logging
from datetime import date, datetime
from dateutil import parser
from sentinel_tiles import sentinel_tiles
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

def generate_HLS_timeseries(
    bands: Optional[Union[List[str], str]] = None,
    tiles: Optional[Union[List[str], str]] = None,
    geometry: Optional[RasterGeometry] = None,
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
    download_directory: Optional[str] = None,
    output_directory: Optional[str] = None) -> None:
    """
    Produce a timeseries of HLS data for the specified parameters.

    Args:
        band (Optional[str]): The spectral band to use (e.g., "B04").
        tiles (Optional[Union[List[str], str]]): The HLS tile identifier(s) (e.g., "10SEG" or ["10SEG", "10TEL"]).
        start_date (Optional[Union[str, date]]): Start date as YYYY-MM-DD string or date object.
        end_date (Optional[Union[str, date]]): End date as YYYY-MM-DD string or date object.
        download_directory (Optional[str]): Directory to save or read data.
        output_directory (Optional[str]): Directory to write output files. Defaults to download_directory.

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

    if tiles is None:
        tiles = []
    elif isinstance(tiles, str):
        tiles = [tiles]

    logger.info("Generating HLS timeseries with parameters:")
    logger.info(f"  Bands: {', '.join(bands)}")
    logger.info(f"  Tiles: {', '.join(tiles)}")
    logger.info(f"  Start date: {start_date}")
    logger.info(f"  End date: {end_date}")
    
    if download_directory is None:
        from harmonized_landsat_sentinel import harmonized_landsat_sentinel as HLS
    # Default output_directory to download_directory if not specified
    if output_directory is None:
        output_directory = download_directory
    
    logger.info(f"  Output directory: {output_directory}")

    for tile in tiles:
        logger.info(f"Processing tile: {tile}")
        
        listing = HLS.listing(
            tile=tile,
            start_UTC=start_date,
            end_UTC=end_date
        ).dropna(how="all", subset=["sentinel", "landsat"])

        dates_available = sorted(listing.date_UTC)

        if len(dates_available) == 0:
            logger.warning(f"no dates available for tile {tile} in the date range {start_date} to {end_date}")
            continue

        logger.info(f"{len(dates_available)} dates available for tile {tile}:")

        for d in dates_available:
            logger.info(f"  * {d}")
        
        for d in dates_available:
            d = parser.parse(d).date()

            for band in bands:
                logger.info(f"extracting band {band} for tile {tile} on date {d}")

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
            filename = join(
                output_directory,
                f"HLS_{band}_{tile}_{d.strftime('%Y%m%d')}.tif"
            )

            logger.info(f"writing image to {filename}")
            image.to_geotiff(expanduser(filename))
    