# Import type hints for function annotations
from typing import Optional, List
# Import logging module for tracking execution and debugging
import logging
# Import date class for handling temporal data
from datetime import date
# Import path manipulation utilities
from os.path import join, expanduser, dirname
from os import makedirs
# Import RasterGeometry and BBox for handling geospatial operations
from rasters import RasterGeometry, BBox
# Import rasters module with alias for mosaic operations
import rasters as rt

# Create a logger instance for this module
logger = logging.getLogger(__name__)

def process_sensor_mosaic(
    sensor: str,
    d: str,
    d_parsed: date,
    band: str,
    tiles: List[str],
    tile_sensor_dates: dict,
    HLS,
    geometry: RasterGeometry | BBox,
    output_directory: str
) -> Optional[str]:
    """
    Helper to extract and mosaic a band across tiles for a specific sensor.
    
    Args:
        sensor (str): "S30" or "L30"
        d (str): Date as string (YYYY-MM-DD)
        d_parsed (date): Date object
        band (str): Band name
        tiles (List[str]): List of tile identifiers
        tile_sensor_dates (dict): Dictionary mapping tiles to available dates per sensor
        HLS: HLS connection object
        geometry (RasterGeometry | BBox): Geographic area of interest
        output_directory (str): Directory to save output files
    
    Returns:
        Optional[str]: Filename if successful, None otherwise
    """
    images = []
    
    # Iterate through each tile to collect images
    for tile in tiles:
        # Check if this tile has data available for this sensor on this date
        available_dates = tile_sensor_dates.get(tile, {}).get(sensor, [])
        if d not in available_dates:
            continue
        
        logger.info(f"extracting band {band} for {sensor} tile {tile} on date {d_parsed}")
        
        try:
            # Get the appropriate granule
            if sensor == "S30":
                granule = HLS.sentinel(tile=tile, date_UTC=d_parsed)
            else:  # sensor == "L30"
                granule = HLS.landsat(tile=tile, date_UTC=d_parsed)
            
            # Extract the band
            try:
                image = granule.product(band)
                images.append(image)
            except (AttributeError, KeyError) as e:
                # Band is not available on this sensor, skip this tile for this band
                logger.debug(f"Band '{band}' not available for {sensor} on tile {tile}: {e}")
                continue
            
        except Exception as e:
            logger.error(f"Error processing {sensor} band {band} for tile {tile} on {d_parsed}: {e}")
            continue
    
    # Only create mosaic if we collected images
    if len(images) == 0:
        logger.warning(f"No images collected for {sensor} band {band} on {d_parsed}")
        return None
    
    # Create output filename with sensor designation
    filename = join(
        output_directory,
        f"{sensor}_{band}_{d_parsed.strftime('%Y%m%d')}.tif"
    )
    
    try:
        mosaic_geometry = geometry
        if isinstance(geometry, BBox):
            target_bbox = geometry.to_crs(images[0].geometry.crs)
            mosaic_geometry = rt.RasterGrid.from_bbox(
                bbox=target_bbox,
                cell_size=images[0].geometry.cell_size,
                crs=images[0].geometry.crs
            )

        # Create a mosaic from all collected images, cropped to the specified geometry
        composite = rt.mosaic(images, geometry=mosaic_geometry)
        
        # Ensure output directory exists before writing
        directory = dirname(expanduser(filename))
        if directory:
            makedirs(directory, exist_ok=True)
        
        # Log that we're saving the mosaicked image to disk
        logger.info(f"writing image to {filename}")
        # Export the composite image to GeoTIFF format
        composite.to_geotiff(expanduser(filename))
        
        return filename
    except Exception as e:
        logger.error(f"Error creating mosaic for {sensor} band {band} on {d_parsed}: {e}")
        return None
