import os
import sys
import geopandas as gpd
import rasterio
from rasterio.mask import mask
import numpy as np
import csv
from pyproj import CRS

__version__ = "1.0.1"

def calculate_stats(shape, raster):
    try:
        masked, _ = mask(raster, [shape], crop=True, all_touched=True)
        masked_data = masked[0]
        masked_data = masked_data[~np.isnan(masked_data)]  # Remove NaN values
        masked_data = masked_data[masked_data != raster.nodata]
        
        if masked_data.size == 0:
            return None, None, None
        
        avg = np.mean(masked_data)
        min_val = np.min(masked_data)
        max_val = np.max(masked_data)
        
        return avg, min_val, max_val
    except Exception as e:
        print(f"Error in calculate_stats: {e}")
        return None, None, None

def process_geotiff(shapefile_path, geotiff_path, output_csv):
    print(f"Processing: {os.path.basename(geotiff_path)}")

    # Read the Shapefile
    gdf = gpd.read_file(shapefile_path)
    
    # Open the GeoTIFF
    with rasterio.open(geotiff_path) as src:
        # Get CRS of both shapefile and raster
        shp_crs = CRS(gdf.crs)
        raster_crs = CRS(src.crs)

        # Check if CRS are the same
        if shp_crs != raster_crs:
            print(f"Warning: CRS mismatch. Shapefile CRS: {shp_crs.to_string()}, Raster CRS: {raster_crs.to_string()}")
            print("Reprojecting shapefile to match raster CRS...")
            gdf = gdf.to_crs(raster_crs)
        
        # Calculate statistics for each shape
        results = []
        for idx, row in gdf.iterrows():
            shape = row.geometry
            avg, min_val, max_val = calculate_stats(shape, src)
            results.append({
                'id': idx,
                'average': avg,
                'minimum': min_val,
                'maximum': max_val
            })
    
    # Write results to CSV
    with open(output_csv, 'w', newline='') as csvfile:
        fieldnames = ['id', 'average', 'minimum', 'maximum']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)

def main(shapefile_path, geotiff_folder, output_folder):
    print(f"shp_soil.py version {__version__}")

    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Process each GeoTIFF in the folder
    for filename in os.listdir(geotiff_folder):
        if filename.lower().endswith(('.tif', '.tiff')):
            geotiff_path = os.path.join(geotiff_folder, filename)
            output_csv = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}_stats.csv")
            process_geotiff(shapefile_path, geotiff_path, output_csv)
            
    print(f"Atribute Units:")
    print(f"Available Phosphorus (mg/kg)")
    print(f"Available Water Capacity (%)")
    print(f"Clay (%)")
    print(f"Coarse Fragments (Proportion)")
    print(f"Sand (%)")
    print(f"Silt (%)")
    print(f"Total Nitrogen (%)")
    print(f"Total Phosphorus (%)")
    print(f"As per: https://esoil.io/TERNLandscapes/Public/Pages/SLGA/ProductDetails-SoilAttributes.html")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <shapefile_path> <geotiff_folder> <output_folder>")
        sys.exit(1)
    
    shapefile_path = sys.argv[1]
    geotiff_folder = sys.argv[2]
    output_folder = sys.argv[3]
    
    main(shapefile_path, geotiff_folder, output_folder)