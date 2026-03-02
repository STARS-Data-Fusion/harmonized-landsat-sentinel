# Import type hints for function annotations
from typing import Optional, Union, List
# Import path manipulation utilities for joining paths and expanding user home directory
from os.path import join, expanduser, dirname
# Import makedirs for creating directories
from os import makedirs
# Import logging module for tracking execution and debugging
import logging
# Import date and datetime classes for handling temporal data
from datetime import date, datetime
# Import parser for flexible date string parsing
from dateutil import parser
# Import sentinel_tiles for mapping geometries to Sentinel-2 tile identifiers
from sentinel_tiles import sentinel_tiles
# Import RasterGeometry and BBox for handling geospatial operations
from rasters import RasterGeometry, BBox
# Import rasters module with alias for mosaic operations
import rasters as rt
# Import the process_sensor_mosaic function for handling multi-tile sensor mosaics
from .process_sensor_mosaic import process_sensor_mosaic
# Import the process_sensor_band function for handling single-tile sensor bands
from .process_sensor_band import process_sensor_band

# Define the default list of spectral bands to retrieve from HLS data
# Includes bands available on both S30 (Sentinel-2) and L30 (Landsat-8) as well as sensor-specific bands
BANDS = [
    "red",              # Red band (visible spectrum, both sensors)
    "green",            # Green band (visible spectrum, both sensors)
    "blue",             # Blue band (visible spectrum, both sensors)
    "coastal_aerosol",  # Coastal/aerosol band (both sensors)
    "NIR",              # Near-Infrared band (both sensors, useful for vegetation analysis)
    "rededge1",         # Red edge 1 band (S30 only, useful for vegetation stress detection)
    "rededge2",         # Red edge 2 band (S30 only)
    "rededge3",         # Red edge 3 band (S30 only)
    "NIR_broad",        # Broad NIR band (S30 only)
    "SWIR1",            # Shortwave Infrared 1 (both sensors, useful for moisture and soil analysis)
    "SWIR2",            # Shortwave Infrared 2 (both sensors, useful for geology and burn detection)
    "water_vapor",      # Water vapor band (S30 only)
    "cirrus",           # Cirrus band (both sensors)
]

# Create a logger instance for this module to track execution progress
logger = logging.getLogger(__name__)

def generate_HLS_timeseries(
    bands: Optional[Union[List[str], str]] = None,              # Spectral band(s) to retrieve (single string or list)
    tiles: Optional[Union[List[str], str]] = None,              # HLS tile identifier(s) (e.g., "10SEG")
    geometry: Optional[Union[RasterGeometry, BBox]] = None,      # Geographic area of interest for automatic tile selection
    start_date_UTC: Optional[Union[str, date]] = None,          # Starting date for timeseries (string or date object)
    end_date_UTC: Optional[Union[str, date]] = None,            # Ending date for timeseries (string or date object)
    download_directory: Optional[str] = None,                   # Directory for caching downloaded HLS data
    output_directory: Optional[str] = None,                     # Directory for saving processed output files
    source: str = "HLS") -> List[str]:                           # Data source: "HLS" (combined, default), "S30" (Sentinel-2), "L30" (Landsat-8), or "both" (separate streams)
    """
    Produce a timeseries of HLS data for the specified parameters.

    Args:
        bands (Optional[Union[List[str], str]]): Spectral band(s) to retrieve (single string or list).
        tiles (Optional[Union[List[str], str]]): HLS tile identifier(s) (e.g., "10SEG" or ["10SEG", "10TEL"]).
        geometry (Optional[Union[RasterGeometry, BBox]]): Geographic area of interest for automatic tile selection.
            - RasterGeometry: Uses the grid resolution for resampling.
            - BBox: Preserves native resolution by building a grid from image cell size.
        start_date_UTC (Optional[Union[str, date]]): Start date as YYYY-MM-DD string or date object.
        end_date_UTC (Optional[Union[str, date]]): End date as YYYY-MM-DD string or date object.
        download_directory (Optional[str]): Directory to save or read data.
        output_directory (Optional[str]): Directory to write output files. Defaults to download_directory.
        source (str): Data source for timeseries. Options:
            - "HLS" (default): Combined HLS data (averages S30 and L30 when both available)
            - "S30": Sentinel-2 only
            - "L30": Landsat-8 only
            - "both": Separate timeseries for S30 and L30 simultaneously

    Returns:
        List[str]: List of output filenames that were created.
    
    Raises:
        ValueError: If invalid source specified or required parameters missing.
    """
    # Validate source parameter
    valid_sources = {"HLS", "S30", "L30", "both"}
    if source not in valid_sources:
        raise ValueError(f"source must be one of {valid_sources}, got '{source}'")
    
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
        if isinstance(geometry, BBox):
            tiles = sentinel_tiles.tiles(target_geometry=geometry.latlon.polygon.geometry)
        else:
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
    # Log the data source
    logger.info(f"  Source: {source}")
    
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

    # Create a dictionary to store which dates are available for each tile and sensor
    # Format: {tile_id: {"S30": [dates], "L30": [dates]}}
    tile_sensor_dates = {}
    # Create a set to collect all unique dates available across all tiles
    all_dates = set()
    
    # Iterate through each tile to query available dates
    for tile in tiles:
        # Log which tile is currently being queried
        logger.info(f"Querying tile: {tile}")
        
        # Query the HLS system for available data for this tile within the date range
        listing = HLS.listing(
            tile=tile,                    # The specific tile identifier
            start_UTC=start_date_UTC,     # Start date for the query
            end_UTC=end_date_UTC          # End date for the query
        )

        # Extract dates available for each sensor
        s30_dates = sorted(listing[listing.sentinel.notna()].date_UTC.unique())
        l30_dates = sorted(listing[listing.landsat.notna()].date_UTC.unique())

        # Check if any dates were found for this tile
        if len(s30_dates) == 0 and len(l30_dates) == 0:
            # Log a warning if no data is available for this tile in the date range
            logger.warning(f"no dates available for tile {tile} in the date range {start_date_UTC} to {end_date_UTC}")
            # Store empty dictionaries for this tile
            tile_sensor_dates[tile] = {"S30": [], "L30": []}
            # Skip to the next tile
            continue

        # Log availability for each sensor
        if len(s30_dates) > 0:
            logger.info(f"{len(s30_dates)} S30 dates available for tile {tile}")
            for d in s30_dates:
                logger.info(f"  * {d} (S30)")
        if len(l30_dates) > 0:
            logger.info(f"{len(l30_dates)} L30 dates available for tile {tile}")
            for d in l30_dates:
                logger.info(f"  * {d} (L30)")
        
        # Store the lists of available dates for each sensor in this tile
        tile_sensor_dates[tile] = {"S30": s30_dates, "L30": l30_dates}
        # Add all dates from this tile to the set of all dates
        all_dates.update(s30_dates)
        all_dates.update(l30_dates)
    
    # Convert the set of all dates to a sorted list
    all_dates = sorted(all_dates)
    # Log the total number of unique dates found across all tiles
    logger.info(f"Total unique dates across all tiles: {len(all_dates)}")
    
    # Begin main processing loop: iterate through dates (outermost loop)
    for d in all_dates:
        # Parse the date string into a date object for processing
        d_parsed = parser.parse(d).date()
        
        # Iterate through each band (middle loop)
        for band in bands:
            # Define band-specific subdirectory path (directory will be created only when needed)
            if source != "both":
                band_output_dir = join(output_directory, band)
            
            # Handle different source modes
            if source == "HLS":
                # Original behavior: use combined HLS (backward compatible)
                images = []
                
                for tile in tiles:
                    # Check if this tile has data available for the current date
                    available_dates = tile_sensor_dates.get(tile, {}).get("S30", []) + tile_sensor_dates.get(tile, {}).get("L30", [])
                    if d not in available_dates:
                        continue
                    
                    logger.info(f"extracting band {band} for tile {tile} on date {d_parsed}")
                    
                    try:
                        # Use the product() method which handles combining S30 and L30
                        image = HLS.product(
                            product=band,
                            date_UTC=d_parsed,
                            tile=tile
                        )
                        
                        # Check if geometry was NOT provided (tile-based processing)
                        if geometry is None:
                            # Create output filename for this individual tile
                            filename = join(
                                band_output_dir,
                                f"HLS_{band}_{tile}_{d_parsed.strftime('%Y%m%d')}.tif"
                            )

                            # Ensure output directory exists before writing
                            directory = dirname(expanduser(filename))
                            if directory:
                                makedirs(directory, exist_ok=True)
                            
                            logger.info(f"writing image to {filename}")
                            image.to_geotiff(expanduser(filename))
                            output_filenames.append(filename)
                        else:
                            # Collect images for mosaic
                            images.append(image)
                    
                    except Exception as e:
                        logger.error(f"Error processing HLS band {band} for tile {tile} on {d_parsed}: {e}")
                        continue
                
                # Create mosaic if geometry was provided
                if geometry is not None and len(images) > 0:
                    try:
                        filename = join(
                            band_output_dir,
                            f"HLS_{band}_{d_parsed.strftime('%Y%m%d')}.tif"
                        )
                        mosaic_geometry = geometry
                        if isinstance(geometry, BBox):
                            target_bbox = geometry.to_crs(images[0].geometry.crs)
                            mosaic_geometry = rt.RasterGrid.from_bbox(
                                bbox=target_bbox,
                                cell_size=images[0].geometry.cell_size,
                                crs=images[0].geometry.crs
                            )
                        composite = rt.mosaic(images, geometry=mosaic_geometry)
                        
                        # Ensure output directory exists before writing
                        directory = dirname(expanduser(filename))
                        if directory:
                            makedirs(directory, exist_ok=True)
                        
                        logger.info(f"writing image to {filename}")
                        composite.to_geotiff(expanduser(filename))
                        output_filenames.append(filename)
                    except Exception as e:
                        logger.error(f"Error creating HLS mosaic for band {band} on {d_parsed}: {e}")
            
            elif source == "S30":
                # Process only Sentinel-2
                if geometry is None:
                    for tile in tiles:
                        available_dates = tile_sensor_dates.get(tile, {}).get("S30", [])
                        if d not in available_dates:
                            continue
                        filename = process_sensor_band("S30", d, d_parsed, band, tile, HLS, band_output_dir)
                        if filename:
                            output_filenames.append(filename)
                else:
                    filename = process_sensor_mosaic("S30", d, d_parsed, band, tiles, tile_sensor_dates, HLS, geometry, band_output_dir)
                    if filename:
                        output_filenames.append(filename)
            
            elif source == "L30":
                # Process only Landsat-8
                if geometry is None:
                    for tile in tiles:
                        available_dates = tile_sensor_dates.get(tile, {}).get("L30", [])
                        if d not in available_dates:
                            continue
                        filename = process_sensor_band("L30", d, d_parsed, band, tile, HLS, band_output_dir)
                        if filename:
                            output_filenames.append(filename)
                else:
                    filename = process_sensor_mosaic("L30", d, d_parsed, band, tiles, tile_sensor_dates, HLS, geometry, band_output_dir)
                    if filename:
                        output_filenames.append(filename)
            
            elif source == "both":
                # Process S30 and L30 simultaneously, writing to separate subdirectories under band directory
                # Define band-specific subdirectories under sensor directories (will be created only when needed)
                s30_output_dir = join(output_directory, "S30", band)
                l30_output_dir = join(output_directory, "L30", band)
                
                if geometry is None:
                    for tile in tiles:
                        for sensor in ["S30", "L30"]:
                            available_dates = tile_sensor_dates.get(tile, {}).get(sensor, [])
                            if d not in available_dates:
                                continue
                            sensor_output_dir = s30_output_dir if sensor == "S30" else l30_output_dir
                            filename = process_sensor_band(sensor, d, d_parsed, band, tile, HLS, sensor_output_dir)
                            if filename:
                                output_filenames.append(filename)
                else:
                    for sensor in ["S30", "L30"]:
                        sensor_output_dir = s30_output_dir if sensor == "S30" else l30_output_dir
                        filename = process_sensor_mosaic(sensor, d, d_parsed, band, tiles, tile_sensor_dates, HLS, geometry, sensor_output_dir)
                        if filename:
                            output_filenames.append(filename)
    
    # Return the complete list of all output filenames that were created
    return output_filenames

    