# Import type hints for function annotations
from typing import Optional
# Import logging module for tracking execution and debugging
import logging
# Import date class for handling temporal data
from datetime import date
# Import path manipulation utilities
from os.path import join, expanduser, dirname
from os import makedirs

# Create a logger instance for this module
logger = logging.getLogger(__name__)

def process_sensor_band(
    sensor: str,
    d: str,
    d_parsed: date,
    band: str,
    tile: str,
    HLS,
    output_directory: str
) -> Optional[str]:
    """
    Helper to extract and save a single band for a specific sensor.
    
    Args:
        sensor (str): "S30" or "L30"
        d (str): Date as string (YYYY-MM-DD)
        d_parsed (date): Date object
        band (str): Band name
        tile (str): Tile identifier
        HLS: HLS connection object
        output_directory (str): Directory to save output files
    
    Returns:
        Optional[str]: Filename if successful, None otherwise
    """
    logger.info(f"extracting band {band} for {sensor} tile {tile} on date {d_parsed}")
    
    try:
        # Get the appropriate granule (sentinel or landsat)
        if sensor == "S30":
            granule = HLS.sentinel(tile=tile, date_UTC=d_parsed)
        else:  # sensor == "L30"
            granule = HLS.landsat(tile=tile, date_UTC=d_parsed)
        
        # Extract the band from the granule
        try:
            image = granule.product(band)
        except (AttributeError, KeyError) as e:
            # Band is not available on this sensor
            logger.warning(f"Band '{band}' not available for {sensor}: {e}")
            return None
        
        # Create output filename with sensor designation
        filename = join(
            output_directory,
            f"{sensor}_{band}_{tile}_{d_parsed.strftime('%Y%m%d')}.tif"
        )

        # Ensure output directory exists before writing
        directory = dirname(expanduser(filename))
        if directory:
            makedirs(directory, exist_ok=True)
        
        # Log that we're saving the image to disk
        logger.info(f"writing image to {filename}")
        # Export the image to GeoTIFF format
        image.to_geotiff(expanduser(filename))
        
        return filename
        
    except Exception as e:
        logger.error(f"Error processing {sensor} band {band} for tile {tile} on {d_parsed}: {e}")
        return None
