import pytest

# List of dependencies
dependencies = [
    "bs4",
    "colored_logging",
    "earthaccess",
    "matplotlib",
    "numpy",
    "pandas",
    "pytest",
    "dateutil",
    "rasterio",
    "rasters",
    "requests",
    "sentinel_tiles",
    "shapely"
]

# Generate individual test functions for each dependency
@pytest.mark.parametrize("dependency", dependencies)
def test_dependency_import(dependency):
    __import__(dependency)
