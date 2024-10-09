__version__ = "2.0.0"

import os
import sys
import argparse
import rasterio
import geopandas as gpd
import pandas as pd
import numpy as np
from rasterio.mask import mask
from shapely.geometry import mapping

def calculate_landuse_percentages(tif_dir, shapefile_path, output_csv, buffer_distance=None):
    # Read the shapefile
    gdf = gpd.read_file(shapefile_path)
    
    # Apply buffer if specified
    if buffer_distance is not None:
        gdf['geometry'] = gdf.geometry.buffer(buffer_distance)
    
    results = []
    all_landuse_types = set()
    
    # Iterate through all GeoTIFF files in the directory
    for tif_file in os.listdir(tif_dir):
        if tif_file.lower().endswith('.tif'):
            tif_path = os.path.join(tif_dir, tif_file)
            print(f"Processing {tif_file}")
            
            with rasterio.open(tif_path) as src:
                # Ensure the shapefile is in the same CRS as the GeoTIFF
                gdf_reprojected = gdf.to_crs(src.crs)
                
                for idx, feature in gdf_reprojected.iterrows():
                    geom = mapping(feature.geometry)
                    
                    try:
                        # Mask the raster to the polygon
                        out_image, out_transform = mask(src, [geom], crop=True)
                        
                        # Get unique land use types and their counts
                        unique, counts = np.unique(out_image, return_counts=True)
                        
                        # Remove no data values
                        valid_data = unique != src.nodata
                        unique = unique[valid_data]
                        counts = counts[valid_data]
                        
                        # Calculate total area
                        total_pixels = np.sum(counts)
                        
                        # Calculate percentages
                        percentages = (counts / total_pixels) * 100
                        
                        # Create a dictionary of land use types and their percentages
                        landuse_percentages = {str(int(u)): p for u, p in zip(unique, percentages)}
                        
                        # Update the set of all land use types
                        all_landuse_types.update(landuse_percentages.keys())
                        
                        # Add to results
                        result = {
                            'id': idx,
                            **landuse_percentages
                        }
                        results.append(result)
                        
                    except ValueError as e:
                        print(f"Warning: Shapefile element {idx} does not overlap with {tif_file}")
    
    # Convert results to a DataFrame
    df_results = pd.DataFrame(results)
    
    # Fill NaN values with 0
    for landuse_type in all_landuse_types:
        if landuse_type not in df_results.columns:
            df_results[landuse_type] = 0
        else:
            df_results[landuse_type] = df_results[landuse_type].fillna(0)
    
    # Sort the DataFrame by 'id' column
    df_results = df_results.sort_values('id')
    
    # Reorder columns to have 'id' first, followed by sorted land use types
    columns = ['id'] + sorted(list(all_landuse_types), key=int)
    df_results = df_results[columns]
    
    # Save results to CSV
    df_results.to_csv(output_csv, index=False)
    print(f"Results saved to {output_csv}")

def main():
    print(f"percent.py version {__version__}")
    parser = argparse.ArgumentParser(description="Calculate land use percentages within shapefile polygons for multiple GeoTIFFs")
    parser.add_argument("tif_dir", help="Directory containing land use GeoTIFF files")
    parser.add_argument("shapefile_path", help="Path to the shapefile")
    parser.add_argument("output_csv", help="Path for the output CSV file")
    parser.add_argument("--buffer", type=float, help="Buffer distance to apply to shapefile polygons (optional)")
    
    args = parser.parse_args()
    
    calculate_landuse_percentages(args.tif_dir, args.shapefile_path, args.output_csv, args.buffer)

if __name__ == "__main__":
    main()