from os.path import join
import matplotlib.pyplot as plt
import earthaccess
import logging

import geopandas as gpd
import rasters as rt

from harmonized_landsat_sentinel import harmonized_landsat_sentinel as HLS
from harmonized_landsat_sentinel import generate_HLS_timeseries

# Configure logging to see info messages
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

# Date range
start_date_UTC = "2022-08-01"
end_date_UTC = "2022-08-02"

# Download directory
download_directory = "~/data/HLS_download"

# Output directory
output_directory = "~/data/Kings Canyon HLS"

# Upper Kings area of interest
gdf = gpd.read_file("upper_kings.kml")

gdf.geometry[0]

bbox_UTM = rt.Polygon(gdf.unary_union).UTM.bbox

grid = rt.RasterGrid.from_bbox(bbox_UTM, cell_size=60, crs=bbox_UTM.crs)

# Log into earthaccess using netrc credentials
earthaccess.login(strategy="netrc", persist=True)

filenames = generate_HLS_timeseries(
    start_date_UTC=start_date_UTC,
    end_date_UTC=end_date_UTC,
    geometry=grid,
    download_directory=download_directory,
    output_directory=output_directory,
    source="both"
)

