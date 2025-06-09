import sys
import argparse
from harmonized_landsat_sentinel import __version__

def print_version_and_exit():
    print(f"HLS CLI {__version__}")
    sys.exit(0)

def main():
    """
    Entry point for the HLS command-line interface.
    """
    parser = argparse.ArgumentParser(
        description="Harmonized Landsat Sentinel (HLS) search and download utility"
    )
    parser.add_argument(
        "--version", action="store_true", help="Show the version and exit"
    )
    parser.add_argument(
        "-b", "--band", type=str, help="Band to use", default=None
    )
    parser.add_argument(
        "-t", "--tile", type=str, help="Tile to use", default=None
    )
    parser.add_argument(
        "--start", type=str, help="Start date (YYYY-MM-DD)", default=None
    )
    parser.add_argument(
        "--end", type=str, help="End date (YYYY-MM-DD)", default=None
    )
    args = parser.parse_args()

    if args.version:
        print_version_and_exit()

    print("Harmonized Landsat Sentinel (HLS) CLI")
    # print("Arguments:", sys.argv[1:])
    if args.band:
        print(f"Band: {args.band}")
    if args.tile:
        print(f"Tile: {args.tile}")
    if args.start:
        print(f"Start date: {args.start}")
    if args.end:
        print(f"End date: {args.end}")

if __name__ == "__main__":
    main()