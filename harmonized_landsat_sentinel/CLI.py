import sys
import argparse
from typing import Optional, List
from harmonized_landsat_sentinel import __version__, timeseries

from .parse_arguments import parse_arguments

def print_version_and_exit() -> None:
    print(f"HLS CLI {__version__}")
    sys.exit(0)

def main(argv: Optional[List[str]] = None) -> None:
    """
    Entry point for the HLS command-line interface.
    Accepts an optional argv list for testing or custom invocation.
    """
    if argv is None:
        argv = sys.argv[1:]

    args = parse_arguments(argv)

    if args.version:
        print_version_and_exit()

    # Call the timeseries function with parsed arguments
    timeseries(
        bands=args.band,
        tile=args.tile,
        start_date=args.start,
        end_date=args.end,
        download_directory=args.directory
    )

if __name__ == "__main__":
    main()
