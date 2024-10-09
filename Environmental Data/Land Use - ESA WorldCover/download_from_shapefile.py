import argparse
import sys
from pathlib import Path
import geopandas as gpd
import requests
from tqdm.auto import tqdm

def download_tiles(output_folder, year, version, s3_url_prefix, tiles, overwrite, dryrun):
    for tile in tqdm(tiles.ll_tile):
        url = f"{s3_url_prefix}/{version}/{year}/map/ESA_WorldCover_10m_{year}_{version}_{tile}_Map.tif"
        out_fn = output_folder / f"ESA_WorldCover_10m_{year}_{version}_{tile}_Map.tif"

        if out_fn.is_file() and not overwrite:
            print(f"{out_fn} already exists.")
            continue

        if not dryrun:
            r = requests.get(url, allow_redirects=True)
            with open(out_fn, 'wb') as f:
                f.write(r.content)
        else:
            print(f"Downloading {url} to {out_fn}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ESA WorldCover download helper with shapefile support")
    parser.add_argument('-o', '--output', default='.', help="Output folder path, defaults to current folder.")
    parser.add_argument('-s', '--shapefile', required=True, help="Path to the input shapefile")
    parser.add_argument('-y', '--year', default=2021, type=int, choices=[2020, 2021], help="Map year, defaults to 2021")
    parser.add_argument('--overwrite', action='store_true', help="Overwrite existing files")
    parser.add_argument('--dry', action='store_true', help="Perform a dry run")
    args = parser.parse_args()

    output_folder = Path(args.output)
    year = args.year
    dryrun = args.dry
    overwrite = args.overwrite

    # Determine the version based on the year
    version = {2020: 'v100', 2021: 'v200'}[year]
    s3_url_prefix = "https://esa-worldcover.s3.eu-central-1.amazonaws.com"

    # Load the input shapefile
    input_shape = gpd.read_file(args.shapefile)

    # Load the WorldCover grid
    url = f'{s3_url_prefix}/v100/2020/esa_worldcover_2020_grid.geojson'
    grid = gpd.read_file(url)

    # Ensure both geometries are in the same CRS
    if grid.crs != input_shape.crs:
        input_shape = input_shape.to_crs(grid.crs)

    # Get grid tiles intersecting the input shapefile
    tiles = grid[grid.intersects(input_shape.geometry.unary_union)]

    if tiles.shape[0] == 0:
        print("No tiles intersect the input shapefile.")
        sys.exit()

    # Download the tiles
    download_tiles(output_folder, year, version, s3_url_prefix, tiles, overwrite, dryrun)
