import sys
import argparse
from typing import Optional, List

def parse_arguments(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command-line arguments for the Harmonized Landsat Sentinel (HLS) utility.

    Args:
        argv (Optional[List[str]]): List of command-line arguments. Defaults to sys.argv.

    Returns:
        argparse.Namespace: Parsed arguments as a namespace object.
    """
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description="Harmonized Landsat Sentinel (HLS) search and download utility"
    )
    
    # Show version and exit
    parser.add_argument(
        "--version", action="store_true", help="Show the version and exit"
    )
    
    # Specify band to use
    parser.add_argument(
        "-b", "--band", type=str, help="Band to use", default=None
    )
    
    # Specify tile to use
    parser.add_argument(
        "-t", "--tile", type=str, help="Tile to use", default=None
    )
    
    # Specify start date (YYYY-MM-DD)
    parser.add_argument(
        "--start", "-s", type=str, help="Start date (YYYY-MM-DD)", default=None
    )
    
    # Specify end date (YYYY-MM-DD)
    parser.add_argument(
        "--end", "-e", type=str, help="End date (YYYY-MM-DD)", default=None
    )
    
    # Specify directory to use
    parser.add_argument(
        "-d", "--directory", type=str, help="Directory to use", default=None
    )

    return parser.parse_args(argv)