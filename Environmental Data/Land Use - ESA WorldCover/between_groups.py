import rasterio
import geopandas as gpd
import numpy as np
import pandas as pd
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
from shapely.geometry import LineString, mapping
from multiprocessing import Pool
from tqdm import tqdm


# Function to reproject the raster to a projected CRS
def reproject_raster(input_raster, output_raster, target_crs):
    with rasterio.open(input_raster) as src:
        transform, width, height = calculate_default_transform(
            src.crs, target_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': target_crs,
            'transform': transform,
            'width': width,
            'height': height
        })

        with rasterio.open(output_raster, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=target_crs,
                    resampling=Resampling.nearest
                )
    print(f"Reprojected raster saved to {output_raster}")


# Function to process a single pair of shapefile elements
def process_pair(args):
    src_path, raster_meta, row1, row2, i, j = args

    # Create a straight line between the centroids of the two geometries
    line = LineString([row1['geometry'].centroid, row2['geometry'].centroid])

    # Create a 1 km buffer around the line
    buffered_line = line.buffer(1000)  # Buffer in meters

    # Simplify the buffered geometry to reduce complexity
    buffered_line = buffered_line.simplify(10)  # Simplify with a tolerance of 10 meters

    # Debugging: Print the simplified buffered line geometry
    print(f"Buffered line geometry for group_1={i}, group_2={j}: {buffered_line}")

    # Mask the raster with the buffered line
    geom = [mapping(buffered_line)]
    try:
        with rasterio.open(src_path) as src:
            # Ensure the geometry is in the same CRS as the raster
            if raster_meta['crs'] != src.crs:
                raise ValueError("CRS mismatch between raster and geometry")

            # Mask the raster with the buffered geometry
            out_image, out_transform = mask(src, geom, crop=True)
            out_image = out_image[0]  # Extract the first band

            # Debugging: Check the shape of the masked raster
            print(f"Masked raster shape for group_1={i}, group_2={j}: {out_image.shape}")

            # If the masked raster is empty, skip this pair
            if out_image.size == 0:
                print(f"No intersection for group_1={i}, group_2={j}")
                return None

    except ValueError as e:
        # Skip if the geometry does not intersect the raster
        print(f"Error processing pair group_1={i}, group_2={j}: {e}")
        return None

    # Flatten the masked raster and remove no-data values
    out_image = out_image.flatten()
    out_image = out_image[out_image != raster_meta['nodata']]

    # Count the occurrences of each land use type
    unique, counts = np.unique(out_image, return_counts=True)
    landuse_counts = dict(zip(unique, counts))

    # Debugging: Print the pair and land use counts
    print(f"Processed pair: group_1={i}, group_2={j}, counts={landuse_counts}")

    # Return the results for this pair
    return {
        "group_1": i,
        "group_2": j,
        **landuse_counts
    }


# Function to calculate pairwise land use counts
def calculate_pairwise_landuse(src_path, shapefile_path, output_csv, num_cores=12):
    # Load the raster metadata
    with rasterio.open(src_path) as src:
        raster_meta = src.meta
        raster_crs = src.crs

        # Check if the raster CRS is projected
        if not raster_crs.is_projected:
            print(f"Reprojecting raster from {raster_crs} to a projected CRS (e.g., UTM)...")
            output_raster = src_path.replace(".tif", "_reprojected.tif")
            target_crs = "EPSG:32750"  # Replace with the appropriate UTM zone
            reproject_raster(src_path, output_raster, target_crs)
            src_path = output_raster  # Use the reprojected raster
            with rasterio.open(src_path) as reprojected_src:
                raster_meta = reprojected_src.meta
                raster_crs = reprojected_src.crs

    # Load the shapefile
    shapefile = gpd.read_file(shapefile_path)

    # Check if the shapefile CRS matches the raster CRS
    if shapefile.crs != raster_crs:
        print(f"Reprojecting shapefile from {shapefile.crs} to {raster_crs}")
        shapefile = shapefile.to_crs(raster_crs)

    # Prepare arguments for multiprocessing
    tasks = []
    for i, row1 in shapefile.iterrows():
        for j, row2 in shapefile.iterrows():
            if i >= j:  # Avoid duplicate pairs and self-pairs
                continue
            tasks.append((src_path, raster_meta, row1, row2, i, j))

    print(f"Processing {len(tasks)} pairs using {num_cores} cores...")

    # Use multiprocessing to process pairs in parallel with a progress bar
    with Pool(processes=num_cores) as pool:
        results = list(tqdm(pool.imap(process_pair, tasks), total=len(tasks)))

    # Filter out None results (skipped pairs)
    results = [res for res in results if res is not None]

    # Convert results to a DataFrame
    df = pd.DataFrame(results)

    # Save to CSV
    df.to_csv(output_csv, index=False)

    print(f"Pairwise land use counts saved to {output_csv}")


# Main function to handle command-line arguments
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Calculate pairwise land use counts between shapefile elements.")
    parser.add_argument("raster", type=str, help="Path to the GeoTIFF file.")
    parser.add_argument("shapefile", type=str, help="Path to the shapefile.")
    parser.add_argument("output", type=str, help="Path to the output CSV file.")
    parser.add_argument("--cores", type=int, default=12, help="Number of CPU cores to use (default: 12).")

    args = parser.parse_args()

    # Call the function with command-line arguments
    calculate_pairwise_landuse(args.raster, args.shapefile, args.output, num_cores=args.cores)
