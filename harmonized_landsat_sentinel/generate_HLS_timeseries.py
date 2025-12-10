# Import type hints for function annotations
from typing import Optional, Union, List
# Import path manipulation utilities for joining paths and expanding user home directory
from os.path import join, expanduser
# Import logging module for tracking execution and debugging
import logging
# Import date and datetime classes for handling temporal data
from datetime import date, datetime
# Import parser for flexible date string parsing
from dateutil import parser
# Import sentinel_tiles for mapping geometries to Sentinel-2 tile identifiers
from sentinel_tiles import sentinel_tiles
# Import RasterGeometry class for handling geospatial raster operations
from rasters import RasterGeometry
# Import rasters module with alias for mosaic operations
import rasters as rt

# Define the default list of spectral bands to retrieve from HLS data
# These bands cover visible (RGB), near-infrared, and shortwave infrared wavelengths
BANDS = [
    "red",      # Red band (visible spectrum)
    "green",    # Green band (visible spectrum)
    "blue",     # Blue band (visible spectrum)
    "NIR",      # Near-Infrared band (useful for vegetation analysis)
    "SWIR1",    # Shortwave Infrared 1 (useful for moisture and soil analysis)
    "SWIR2"     # Shortwave Infrared 2 (useful for geology and burn detection)
]

# Create a logger instance for this module to track execution progress
logger = logging.getLogger(__name__)

def generate_HLS_timeseries(
    bands: Optional[Union[List[str], str]] = None,              # Spectral band(s) to retrieve (single string or list)
    tiles: Optional[Union[List[str], str]] = None,              # HLS tile identifier(s) (e.g., "10SEG")
    geometry: Optional[RasterGeometry] = None,                  # Geographic area of interest for automatic tile selection
    start_date_UTC: Optional[Union[str, date]] = None,          # Starting date for timeseries (string or date object)
    end_date_UTC: Optional[Union[str, date]] = None,            # Ending date for timeseries (string or date object)
    download_directory: Optional[str] = None,                   # Directory for caching downloaded HLS data
    output_directory: Optional[str] = None) -> List[str]:       # Directory for saving processed output files
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
    # Check if start_date_UTC is provided as a string (e.g., "2023-01-01")
    if isinstance(start_date_UTC, str):
        # Convert string to date object using dateutil parser for flexible parsing
        start_date_UTC = parser.parse(start_date_UTC).date()
    
    # Check if end_date_UTC is provided as a string
    if isinstance(end_date_UTC, str):
        # Convert string to date object using dateutil parser
        end_date_UTC = parser.parse(end_date_UTC).date()

    # Check if no bands were specified by the user
    if bands is None:
        # Use the default BANDS list (red, green, blue, NIR, SWIR1, SWIR2)
        bands = BANDS
    # Check if a single band was provided as a string
    elif isinstance(bands, str):
        # Convert single band string to a list containing one element
        bands = [bands]

    # Validate that either tiles or geometry was provided (at least one is required)
    if tiles is None and geometry is None:
        # Raise an error if neither parameter was specified
        raise ValueError("Either 'tiles' or 'geometry' must be provided.")
    
    # If tiles weren't specified but geometry was provided
    if tiles is None and geometry is not None:
        # Automatically determine which Sentinel-2 tiles cover the geometry
        # This converts the geometry boundary to lat/lon and finds intersecting tiles
        tiles = sentinel_tiles.tiles(target_geometry=geometry.boundary_latlon.geometry)

    # Handle case where tiles might still be None after geometry processing
    if tiles is None:
        # Initialize as empty list to prevent iteration errors
        tiles = []
    # Check if a single tile was provided as a string
    elif isinstance(tiles, str):
        # Convert single tile string to a list containing one element
        tiles = [tiles]

    # Initialize an empty list to track all output filenames created during processing
    output_filenames = []

    # Log the parameters being used for this HLS timeseries generation
    logger.info("Generating HLS timeseries with parameters:")
    # Log the list of bands being retrieved, joined into a comma-separated string
    logger.info(f"  Bands: {', '.join(bands)}")
    # Log the list of tiles being processed, joined into a comma-separated string
    logger.info(f"  Tiles: {', '.join(tiles)}")
    # Log the start date of the timeseries
    logger.info(f"  Start date: {start_date_UTC}")
    # Log the end date of the timeseries
    logger.info(f"  End date: {end_date_UTC}")
    
    # Check if a custom download directory was specified
    if download_directory is None:
        # Use the default HLS connection with standard download location
        from harmonized_landsat_sentinel import harmonized_landsat_sentinel as HLS
    else:
        # Create a custom HLS connection with the specified download directory
        from harmonized_landsat_sentinel import HLS2Connection
        # Instantiate the connection object with custom download path
        HLS = HLS2Connection(download_directory=download_directory)
        
    # Check if output_directory wasn't explicitly set
    if output_directory is None:
        # Use the same directory as download_directory for output files
        output_directory = download_directory
    
    # Log the directory where output files will be saved
    logger.info(f"  Output directory: {output_directory}")

    # Create a set to collect all unique dates available across all tiles
    # Using a set automatically handles duplicates
    all_dates = set()
    # Create a dictionary to store which dates are available for each tile
    # Format: {tile_id: [list_of_dates]}
    tile_dates = {}
    
    # Iterate through each tile to query available dates
    for tile in tiles:
        # Log which tile is currently being queried
        logger.info(f"Querying tile: {tile}")
        
        # Query the HLS system for available data for this tile within the date range
        listing = HLS.listing(
            tile=tile,                    # The specific tile identifier
            start_UTC=start_date_UTC,     # Start date for the query
            end_UTC=end_date_UTC          # End date for the query
        ).dropna(how="all", subset=["sentinel", "landsat"])  # Remove rows where both sentinel and landsat are null

        # Extract and sort the available dates from the listing
        dates_available = sorted(listing.date_UTC)

        # Check if any dates were found for this tile
        if len(dates_available) == 0:
            # Log a warning if no data is available for this tile in the date range
            logger.warning(f"no dates available for tile {tile} in the date range {start_date_UTC} to {end_date_UTC}")
            # Store empty list for this tile in the dictionary
            tile_dates[tile] = []
            # Skip to the next tile
            continue

        # Log how many dates were found for this tile
        logger.info(f"{len(dates_available)} dates available for tile {tile}:")
        
        # Iterate through each available date and log it
        for d in dates_available:
            logger.info(f"  * {d}")
        
        # Store the list of available dates for this tile in the dictionary
        tile_dates[tile] = dates_available
        # Add all dates from this tile to the set of all dates (automatically handles duplicates)
        all_dates.update(dates_available)
    
    # Convert the set of all dates to a sorted list
    all_dates = sorted(all_dates)
    # Log the total number of unique dates found across all tiles
    logger.info(f"Total unique dates across all tiles: {len(all_dates)}")
    
    # Begin main processing loop: iterate through dates (outermost loop)
    # This ensures all bands/tiles for a date are processed together
    for d in all_dates:
        # Parse the date string into a date object for processing
        d_parsed = parser.parse(d).date()
        
        # Iterate through each band (middle loop)
        for band in bands:
            # Initialize empty list to collect images from multiple tiles for this band/date
            # This is used when creating mosaics from multiple tiles
            images = []
            
            # Iterate through each tile (innermost loop)
            for tile in tiles:
                # Check if this specific tile has data available for the current date
                # Use .get() with default empty list to handle missing tiles safely
                if d not in tile_dates.get(tile, []):
                    # Skip this tile if no data is available for this date
                    continue
                
                # Log that we're about to extract data for this band/tile/date combination
                logger.info(f"extracting band {band} for tile {tile} on date {d_parsed}")

                # Wrap the data extraction in try-except to handle any errors gracefully
                try:
                    # Retrieve the HLS product (image data) for this specific combination
                    image = HLS.product(
                        product=band,           # The spectral band to retrieve
                        date_UTC=d_parsed,      # The date of the imagery
                        tile=tile               # The tile identifier
                    )
                    
                    # Check if geometry was NOT provided (tile-based processing)
                    if geometry is None:
                        # Create output filename for this individual tile
                        # Format: HLS_<band>_<tile>_<YYYYMMDD>.tif
                        filename = join(
                            output_directory,
                            f"HLS_{band}_{tile}_{d_parsed.strftime('%Y%m%d')}.tif"
                        )

                        # Log that we're saving the image to disk
                        logger.info(f"writing image to {filename}")
                        # Export the image to GeoTIFF format, expanding ~ to home directory if present
                        image.to_geotiff(expanduser(filename))
                        # Add the filename to the list of outputs for return value
                        output_filenames.append(filename)
                    else:
                        # If geometry was provided, collect images for mosaic instead of saving individually
                        images.append(image)

                # Catch any exception that occurs during processing
                except Exception as e:
                    # Log the error for debugging purposes
                    logger.error(e)
                    # Continue to next tile rather than stopping execution
                    continue
                
            # After processing all tiles for this band/date combination
            # Check if geometry was provided (indicating we should create a mosaic)
            if geometry is not None:
                # Create output filename for the mosaicked image
                # Format: HLS_<band>_<YYYYMMDD>.tif (note: no tile identifier)
                filename = join(
                    output_directory,
                    f"HLS_{band}_{d_parsed.strftime('%Y%m%d')}.tif"
                )
                
                # Create a mosaic from all collected images, cropped to the specified geometry
                composite = rt.mosaic(images, geometry=geometry)
                
                # Log that we're saving the mosaicked image to disk
                logger.info(f"writing image to {filename}")
                # Export the composite image to GeoTIFF format
                composite.to_geotiff(expanduser(filename))
                # Add the filename to the list of outputs for return value
                output_filenames.append(filename)
    
    # Return the complete list of all output filenames that were created
    return output_filenames

    