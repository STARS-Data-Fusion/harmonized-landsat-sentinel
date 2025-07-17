
from typing import List
from typing import TYPE_CHECKING
from os.path import basename, join, abspath, expanduser
from glob import glob
from abc import ABC, abstractmethod
import numpy as np
import rasters as rt
from rasters import Raster, MultiRaster
import warnings
from .constants import *
from .exceptions import *
from .HLS_granule_ID import HLSGranuleID
"""
HLS2Granule: Abstract base class for handling HLS (Harmonized Landsat and Sentinel-2) granule data.

This class provides a unified interface for accessing, processing, and visualizing HLS2 granule bands and derived products.
It supports reading band files, applying scaling, masking clouds, and computing common remote sensing indices.

Subclasses must implement the `albedo` property.
"""

class HLS2Granule(ABC):
    """
    Abstract base class for HLS2 granule data access and processing.

    Attributes:
        directory (str): Path to the granule directory containing band files.
        ID (HLSGranuleID): Parsed granule ID object.
        connection: Optional connection object for remote access.
        band_images (dict): Cache of loaded band images.
    """
    def __init__(self, directory: str, connection=None):
        """
        Initialize the HLS2Granule object.

        Args:
            directory (str): Path to the granule directory.
            connection: Optional connection object for remote data access.
        """
        self.directory = directory
        self.ID = HLSGranuleID(basename(directory))
        self.connection = connection
        self.band_images = {}  # Cache for loaded band images

    def __repr__(self) -> str:
        """String representation of the granule."""
        return f"HLS2Granule({self.directory})"

    def _repr_png_(self) -> bytes:
        """PNG representation for Jupyter display (shows RGB composite)."""
        return self.RGB._repr_png_()

    @property
    def filenames(self) -> List[str]:
        """
        List all files in the granule directory.
        Returns:
            List[str]: Sorted list of all file paths in the directory.
        """
        return sorted(glob(join(self.directory, f"*.*")))

    def band_name(self, band: str | int) -> str:
        """
        Convert band identifier to standard band name string.
        Args:
            band (str|int): Band name or number.
        Returns:
            str: Standardized band name (e.g., 'B04').
        """
        if isinstance(band, int):
            band = f"B{band:02d}"
        return band

    def band_filename(self, band: str | int) -> str:
        """
        Get the filename for a given band.
        Args:
            band (str|int): Band name or number.
        Returns:
            str: Path to the band file.
        Raises:
            HLSBandNotAcquired: If the band file is not found.
        """
        band = self.band_name(band)
        pattern = join(abspath(expanduser(self.directory)), f"*.{band}.tif")
        filenames = sorted(glob(pattern))
        if len(filenames) == 0:
            raise HLSBandNotAcquired(f"no file found for band {band} for granule {self.ID} in directory: {self.directory}")
        return filenames[-1]

    def DN(self, band: str | int) -> Raster:
        """
        Load the digital number (DN) raster for a given band.
        Uses caching to avoid reloading the same band.
        Args:
            band (str|int): Band name or number.
        Returns:
            Raster: Raster object for the band.
        """
        if band in self.band_images:
            return self.band_images[band]
        # Only support GeoTIFF access in directory
        filename = self.band_filename(band)
        image = Raster.open(filename)
        self.band_images[band] = image
        return image

    @property
    def Fmask(self) -> Raster:
        """
        Return the Fmask raster (cloud/shadow mask).
        Returns:
            Raster: Fmask raster.
        """
        return self.DN("Fmask")

    @property
    def QA(self) -> Raster:
        """
        Return the QA raster. For HLS2, this is the same as Fmask.
        Returns:
            Raster: QA raster.
        """
        return self.Fmask

    @property
    def geometry(self) -> object:
        """
        Return the geometry of the granule (from QA raster).
        Returns:
            Geometry object.
        """
        return self.QA.geometry

    @property
    def cloud(self) -> Raster:
        """
        Compute the cloud mask raster.
        Returns:
            Raster: Cloud mask raster (colored).
        """
        # For HLS2, cloud mask logic may differ; fallback to HLSGranule logic if needed
        return (self.QA & 15 > 0).color(CLOUD_CMAP)

    @property
    def water(self) -> Raster:
        """
        Compute the water mask raster.
        Returns:
            Raster: Water mask raster (colored).
        """
        return ((self.QA >> 5) & 1 == 1).color(WATER_CMAP)

    def band(self, band: str | int, apply_scale: bool = True, apply_cloud: bool = True) -> Raster:
        """
        Load and process a band raster, applying scaling and cloud masking.
        Args:
            band (str|int): Band name or number.
            apply_scale (bool): Whether to apply scale factor and nodata masking.
            apply_cloud (bool): Whether to mask clouds as NaN.
        Returns:
            Raster: Processed raster.
        """
        image = self.DN(band)
        if apply_scale:
            # Convert fill values to NaN and apply scale factor
            image = rt.where(image == -1000, np.nan, image * 0.0001)
            image = rt.where(image < 0, np.nan, image)
            image.nodata = np.nan
        if apply_cloud:
            # Mask out cloud pixels
            image = rt.where(self.cloud, np.nan, image)
        return image

    @property
    def red(self) -> Raster:
        """Return the red band as a processed raster."""
        return self.band("B04")

    @property
    def green(self) -> Raster:
        """Return the green band as a processed raster."""
        return self.band("B03")

    @property
    def blue(self) -> Raster:
        """Return the blue band as a processed raster."""
        return self.band("B02")

    @property
    def NIR(self) -> Raster:
        """Return the near-infrared (NIR) band as a processed raster."""
        return self.band("B08")

    @property
    def SWIR1(self) -> Raster:
        """Return the shortwave infrared 1 (SWIR1) band as a processed raster."""
        return self.band("B11")

    @property
    def SWIR2(self) -> Raster:
        """Return the shortwave infrared 2 (SWIR2) band as a processed raster."""
        return self.band("B12")

    @property
    def RGB(self) -> MultiRaster:
        """
        Return an RGB composite as a MultiRaster (red, green, blue).
        Returns:
            MultiRaster: RGB composite.
        """
        return MultiRaster.stack([self.red, self.green, self.blue])

    @property
    def true(self) -> MultiRaster:
        """Alias for RGB composite."""
        return self.RGB

    @property
    def false_urban(self) -> MultiRaster:
        """
        Return a false color composite for urban analysis (SWIR2, SWIR1, red).
        Returns:
            MultiRaster: False color composite.
        """
        return MultiRaster.stack([self.SWIR2, self.SWIR1, self.red])

    @property
    def false_vegetation(self) -> MultiRaster:
        """
        Return a false color composite for vegetation (NIR, red, green).
        Returns:
            MultiRaster: False color composite.
        """
        return MultiRaster.stack([self.NIR, self.red, self.green])

    @property
    def false_healthy(self) -> MultiRaster:
        """
        Return a false color composite for healthy vegetation (NIR, SWIR1, blue).
        Returns:
            MultiRaster: False color composite.
        """
        return MultiRaster.stack([self.NIR, self.SWIR1, self.blue])

    @property
    def false_agriculture(self) -> MultiRaster:
        """
        Return a false color composite for agriculture (SWIR1, NIR, blue).
        Returns:
            MultiRaster: False color composite.
        """
        return MultiRaster.stack([self.SWIR1, self.NIR, self.blue])

    @property
    def false_water(self) -> MultiRaster:
        """
        Return a false color composite for water (NIR, SWIR1, red).
        Returns:
            MultiRaster: False color composite.
        """
        return MultiRaster.stack([self.NIR, self.SWIR1, self.red])

    @property
    def false_geology(self) -> MultiRaster:
        """
        Return a false color composite for geology (SWIR2, SWIR1, blue).
        Returns:
            MultiRaster: False color composite.
        """
        return MultiRaster.stack([self.SWIR2, self.SWIR1, self.blue])

    @property
    def NDVI(self) -> Raster:
        """
        Compute the Normalized Difference Vegetation Index (NDVI).
        Returns:
            Raster: NDVI raster (with NDVI colormap).
        """
        image = (self.NIR - self.red) / (self.NIR + self.red)
        image.cmap = NDVI_CMAP
        return image

    @property
    @abstractmethod
    def albedo(self) -> Raster:
        """
        Abstract property for surface albedo raster.
        Subclasses must implement this property.
        Returns:
            Raster: Albedo raster.
        """
        pass

    @property
    def NDSI(self) -> Raster:
        """
        Compute the Normalized Difference Snow Index (NDSI).
        Returns:
            Raster: NDSI raster (colored with 'jet' colormap).
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            NDSI = (self.green - self.SWIR1) / (self.green + self.SWIR1)
            NDSI = rt.clip(NDSI, -1, 1)
            NDSI = NDSI.astype(np.float32)
            NDSI = NDSI.color("jet")
        return NDSI

    @property
    def MNDWI(self) -> Raster:
        """
        Compute the Modified Normalized Difference Water Index (MNDWI).
        Returns:
            Raster: MNDWI raster (colored with 'jet' colormap).
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            MNDWI = (self.green - self.SWIR1) / (self.green + self.SWIR1)
            MNDWI = rt.clip(MNDWI, -1, 1)
            MNDWI = MNDWI.astype(np.float32)
            MNDWI = MNDWI.color("jet")
        return MNDWI

    @property
    def NDWI(self) -> Raster:
        """
        Compute the Normalized Difference Water Index (NDWI).
        Returns:
            Raster: NDWI raster (colored with 'jet' colormap).
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            NDWI = (self.green - self.NIR) / (self.green + self.NIR)
            NDWI = rt.clip(NDWI, -1, 1)
            NDWI = NDWI.astype(np.float32)
            NDWI = NDWI.color("jet")
        return NDWI

    @property
    def moisture(self) -> Raster:
        """
        Compute a moisture index ((NIR - SWIR1) / (NIR + SWIR1)).
        Returns:
            Raster: Moisture index raster (colored with 'jet' colormap).
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            moisture = (self.NIR - self.SWIR1) / (self.NIR + self.SWIR1)
            moisture = rt.clip(moisture, -1, 1)
            moisture = moisture.astype(np.float32)
            moisture = moisture.color("jet")
        return moisture

    def product(self, product: str) -> Raster:
        """
        Generic accessor for any product or band by name.
        Args:
            product (str): Name of the property or method to retrieve.
        Returns:
            Raster: The requested raster product.
        """
        return getattr(self, product)
