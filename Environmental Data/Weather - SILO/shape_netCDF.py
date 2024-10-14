__version__ = "1.0.0"

import xarray as xr
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import box
import argparse
import rioxarray
from rioxarray.exceptions import NoDataInBounds

# Set up command-line argument parsing
parser = argparse.ArgumentParser(description='Process NetCDF data using shapefile boundaries')
parser.add_argument('nc_file', help='Path to the NetCDF file')
parser.add_argument('shp_file', help='Path to the shapefile')
parser.add_argument('output_file', help='Path for the output CSV file')
args = parser.parse_args()

# Load NetCDF file
try:
    ds = xr.open_dataset(args.nc_file, engine='netcdf4')
    # Set the CRS to EPSG:4326 (WGS84)
    ds = ds.rio.write_crs("EPSG:4326")
except Exception as e:
    print(f"Error opening NetCDF file: {e}")
    print("Trying with 'scipy' engine...")
    try:
        ds = xr.open_dataset(args.nc_file, engine='scipy')
        # Set the CRS to EPSG:4326 (WGS84)
        ds = ds.rio.write_crs("EPSG:4326")
    except Exception as e:
        print(f"Error opening NetCDF file with 'scipy' engine: {e}")
        exit(1)

# Load shapefile
try:
    gdf = gpd.read_file(args.shp_file)
except Exception as e:
    print(f"Error opening shapefile: {e}")
    exit(1)

# Print CRS information
print("NetCDF CRS:")
print(ds.rio.crs)
print("\nShapefile CRS:")
print(gdf.crs)

# Reproject shapefile to match NetCDF if necessary
if gdf.crs != ds.rio.crs:
    print("Reprojecting shapefile to match NetCDF CRS (EPSG:4326)")
    gdf = gdf.to_crs("EPSG:4326")

print("CRS now match:", gdf.crs == ds.rio.crs)

# Print extents
print("\nNetCDF extent:")
print(f"Longitude: {ds.lon.min().item()} to {ds.lon.max().item()}")
print(f"Latitude: {ds.lat.min().item()} to {ds.lat.max().item()}")

print("\nShapefile extent:")
total_bounds = gdf.total_bounds
print(f"Longitude: {total_bounds[0]} to {total_bounds[2]}")
print(f"Latitude: {total_bounds[1]} to {total_bounds[3]}")

# Identify main data variable (assuming it's the one with most dimensions)
data_var = max(ds.data_vars, key=lambda v: len(ds[v].dims))

# Initialize results dictionary
results = {col: [] for col in gdf.columns if col != 'geometry'}
for month in range(1, 13):
    results[f'avg_month_{month:02d}'] = []

# Iterate through each shapefile element
for idx, row in gdf.iterrows():
    # Add a small buffer to the geometry
    geometry = row.geometry.buffer(0.01)
    
    try:
        # Check if the geometry is valid
        if not geometry.is_valid:
            print(f"Invalid geometry for index {idx}. Attempting to fix.")
            geometry = geometry.buffer(0)
        
        # Clip the NetCDF data to the current shapefile geometry
        clipped = ds[data_var].rio.clip([geometry], gdf.crs)
        
        # Calculate average value for each month
        monthly_avgs = clipped.groupby('time.month').mean()
        
        for month in range(1, 13):
            if month in monthly_avgs.month:
                avg_value = monthly_avgs.sel(month=month).values
                # Check if avg_value is a scalar or an array
                if avg_value.size == 1:
                    avg_value = avg_value.item()
                else:
                    # If it's an array, take the mean
                    avg_value = np.nanmean(avg_value)  # Use nanmean to ignore NaN values
            else:
                avg_value = np.nan
            results[f'avg_month_{month:02d}'].append(avg_value)
        
    except NoDataInBounds:
        print(f"No data found for geometry {idx}. Skipping.")
        for month in range(1, 13):
            results[f'avg_month_{month:02d}'].append(np.nan)
    except Exception as e:
        print(f"Error processing geometry {idx}: {str(e)}")
        for month in range(1, 13):
            results[f'avg_month_{month:02d}'].append(np.nan)
    
    # Store other results
    for col in gdf.columns:
        if col != 'geometry':
            results[col].append(row[col])

# Create DataFrame from results
df = pd.DataFrame(results)

# Save to CSV
df.to_csv(args.output_file, index=False)

print(f"Results saved to {args.output_file}")
print(f"shape_netCDF.py version {__version__}")