from matplotlib.colors import LinearSegmentedColormap

DEFAULT_REMOTE = "https://hls.gsfc.nasa.gov/data/v1.4/"
DEFAULT_WORKING_DIRECTORY = "."
DEFAULT_DOWNLOAD_DIRECTORY = "HLS_download"
DEFAULT_PRODUCTS_DIRECTORY = "HLS_products"
DEFAULT_TARGET_RESOLUTION = 30
DEFAULT_PRODUCTS = ["NIR", "red"]

CONNECTION_CLOSE = {
    "Connection": "close",
}

NDVI_CMAP = LinearSegmentedColormap.from_list(
    name="NDVI",
    colors=[
        (0, "#0000ff"),
        (0.4, "#000000"),
        (0.5, "#745d1a"),
        (0.6, "#e1dea2"),
        (0.8, "#45ff01"),
        (1, "#325e32")
    ]
)

ALBEDO_CMAP = LinearSegmentedColormap.from_list(name="albedo", colors=["black", "white"])
RED_CMAP = LinearSegmentedColormap.from_list(name="red", colors=["black", "red"])
BLUE_CMAP = LinearSegmentedColormap.from_list(name="blue", colors=["black", "blue"])
GREEN_CMAP = LinearSegmentedColormap.from_list(name="green", colors=["black", "green"])
WATER_CMAP = LinearSegmentedColormap.from_list(name="water", colors=["black", "#0eeded"])
CLOUD_CMAP = LinearSegmentedColormap.from_list(name="water", colors=["black", "white"])
CMR_STAC_URL = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"
WORKING_DIRECTORY = "."
DOWNLOAD_DIRECTORY = "HLS2_download"
PRODUCTS_DIRECTORY = "HLS2_products"
TARGET_RESOLUTION = 30
COLLECTIONS = ["HLSS30.v2.0", "HLSL30.v2.0"]
DEFAULT_RETRIES = 3
DEFAULT_WAIT_SECONDS = 20
DEFAULT_DOWNLOAD_RETRIES = 3
DEFAULT_DOWNLOAD_WAIT_SECONDS = 60
L30_CONCEPT = "C2021957657-LPCLOUD"
S30_CONCEPT = "C2021957295-LPCLOUD"
PAGE_SIZE = 2000
CMR_SEARCH_URL = "https://cmr.earthdata.nasa.gov/search"
CMR_GRANULES_JSON_URL = f"{CMR_SEARCH_URL}/granules.json"
DEFAULT_HLS1_REMOTE = "https://hls.gsfc.nasa.gov/data/v1.4/"
DEFAULT_HLS1_DOWNLOAD_DIRECTORY = "HLS1_download"
DEFAULT_HLS1_PRODUCTS_DIRECTORY = "HLS1_products"
