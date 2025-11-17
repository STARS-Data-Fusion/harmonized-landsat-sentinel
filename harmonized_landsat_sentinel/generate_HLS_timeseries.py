from typing import Optional, Union, List
from os.path import join, expanduser
import logging
from datetime import date, datetime
from dateutil import parser
from sentinel_tiles import sentinel_tiles
from rasters import RasterGeometry
import rasters as rt

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
    start_date_UTC: Optional[Union[str, date]] = None,
    end_date_UTC: Optional[Union[str, date]] = None,
    download_directory: Optional[str] = None,
    output_directory: Optional[str] = None) -> List[str]:
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
        List[str]: List of output filenames that were created.
    """
    # Parse start_date and end_date if they are strings
    if isinstance(start_date_UTC, str):
        start_date_UTC = parser.parse(start_date_UTC).date()
    if isinstance(end_date_UTC, str):
        end_date_UTC = parser.parse(end_date_UTC).date()

    if bands is None:
        bands = BANDS
    elif isinstance(bands, str):
        bands = [bands]

    if tiles is None and geometry is None:
        raise ValueError("Either 'tiles' or 'geometry' must be provided.")
    
    if tiles is None and geometry is not None:
        tiles = sentinel_tiles.tiles(target_geometry=geometry.boundary_latlon.geometry)

    if tiles is None:
        tiles = []
    elif isinstance(tiles, str):
        tiles = [tiles]

    output_filenames = []

    logger.info("Generating HLS timeseries with parameters:")
    logger.info(f"  Bands: {', '.join(bands)}")
    logger.info(f"  Tiles: {', '.join(tiles)}")
    logger.info(f"  Start date: {start_date_UTC}")
    logger.info(f"  End date: {end_date_UTC}")
    
    if download_directory is None:
        from harmonized_landsat_sentinel import harmonized_landsat_sentinel as HLS
    else:
        from harmonized_landsat_sentinel import HLS2Connection
        HLS = HLS2Connection(download_directory=download_directory)
        
    # Default output_directory to download_directory if not specified
    if output_directory is None:
        output_directory = download_directory
    
    logger.info(f"  Output directory: {output_directory}")

    # Collect all available dates across all tiles
    all_dates = set()
    tile_dates = {}
    
    for tile in tiles:
        logger.info(f"Querying tile: {tile}")
        
        listing = HLS.listing(
            tile=tile,
            start_UTC=start_date_UTC,
            end_UTC=end_date_UTC
        ).dropna(how="all", subset=["sentinel", "landsat"])

        dates_available = sorted(listing.date_UTC)

        if len(dates_available) == 0:
            logger.warning(f"no dates available for tile {tile} in the date range {start_date_UTC} to {end_date_UTC}")
            tile_dates[tile] = []
            continue

        logger.info(f"{len(dates_available)} dates available for tile {tile}:")
        
        for d in dates_available:
            logger.info(f"  * {d}")
        
        tile_dates[tile] = dates_available
        all_dates.update(dates_available)
    
    all_dates = sorted(all_dates)
    logger.info(f"Total unique dates across all tiles: {len(all_dates)}")
    
    # Iterate through dates, then bands, then tiles (innermost)
    for d in all_dates:
        d_parsed = parser.parse(d).date()
        
        for band in bands:
            images = []
            
            for tile in tiles:
                # Skip if this tile doesn't have data for this date
                if d not in tile_dates.get(tile, []):
                    continue
                
                logger.info(f"extracting band {band} for tile {tile} on date {d_parsed}")

                try:
                    image = HLS.product(
                        product=band,
                        date_UTC=d_parsed,
                        tile=tile
                    )
                    
                    if geometry is None:
                        filename = join(
                            output_directory,
                            f"HLS_{band}_{tile}_{d_parsed.strftime('%Y%m%d')}.tif"
                        )

                        logger.info(f"writing image to {filename}")
                        image.to_geotiff(expanduser(filename))
                        output_filenames.append(filename)
                    else:
                        images.append(image)

                except Exception as e:
                    logger.error(e)
                    continue
                
            if geometry is None:
                filename = join(
                    output_directory,
                    f"HLS_{band}_{d_parsed.strftime('%Y%m%d')}.tif"
                )
                
                composite = rt.mosaic(images, geometry=geometry)
                
                logger.info(f"writing image to {filename}")
                composite.to_geotiff(expanduser(filename))
                output_filenames.append(filename)
    
    return output_filenames

    