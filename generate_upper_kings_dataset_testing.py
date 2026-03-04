import matplotlib.pyplot as plt
import earthaccess
import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import geopandas as gpd
import rasters as rt

# Configure logging to see info messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True,
)
logger = logging.getLogger(__name__)
logger.info("Starting Upper Kings HLS dataset test script")

# Date range
start_date_UTC = "2022-08-01"
end_date_UTC = "2022-08-02"

# Download directory
download_directory = "~/data/HLS_download"

# Output directory
output_directory = "~/data/Kings_Canyon_HLS"

# Upper Kings area of interest
logger.info("Loading AOI from arrow_peak.geojson")
gdf = gpd.read_file("arrow_peak.geojson")

gdf.geometry[0]

logger.info("Building UTM bounding box and raster grid")
bbox_UTM = rt.Polygon(gdf.unary_union).UTM.bbox

grid = rt.RasterGrid.from_bbox(bbox_UTM, cell_size=60, crs=bbox_UTM.crs)

# Log into earthaccess using netrc credentials
logger.info("Logging into Earthdata via netrc")
login_timeout_seconds = 180
heartbeat_seconds = 5

with ThreadPoolExecutor(max_workers=1) as executor:
    future = executor.submit(earthaccess.login, strategy="netrc", persist=True)
    start_time = time.monotonic()

    while True:
        try:
            future.result(timeout=heartbeat_seconds)
            break
        except TimeoutError:
            elapsed = int(time.monotonic() - start_time)
            auth_obj = getattr(earthaccess, "__auth__", None)
            is_authenticated = bool(getattr(auth_obj, "authenticated", False))

            if is_authenticated:
                logger.info(
                    f"Earthdata credentials accepted; finalizing session ({elapsed}s elapsed, timeout {login_timeout_seconds}s)"
                )
            else:
                logger.info(
                    f"Earthdata login still in progress ({elapsed}s elapsed, timeout {login_timeout_seconds}s)"
                )
            if elapsed >= login_timeout_seconds:
                raise TimeoutError(
                    f"Earthdata login exceeded {login_timeout_seconds}s. "
                    "Check network/VPN/firewall and try again."
                )
logger.info("Earthdata login complete")

logger.info("Importing generate_HLS_timeseries")
from harmonized_landsat_sentinel import generate_HLS_timeseries

logger.info("Generating HLS timeseries (this may take a while)")
filenames = generate_HLS_timeseries(
    start_date_UTC=start_date_UTC,
    end_date_UTC=end_date_UTC,
    geometry=grid,
    download_directory=download_directory,
    output_directory=output_directory,
    source="both",
    skip_all_nan=True,
)

logger.info(f"Timeseries generation complete, produced {len(filenames)} files")
for filename in filenames:
    print(filename)

