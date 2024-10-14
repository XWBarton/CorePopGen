__version__ = "1.0.0"

import geopandas as gpd
import pandas as pd
import folium
import os
import argparse

# Function to visualize shapefile with land use data
def visualize_shapefile_with_landuse(shapefile_path, csv_path):
    # Load the shapefile
    gdf = gpd.read_file(shapefile_path)
    print("Shapefile columns:", gdf.columns.tolist())  # Debugging line

    # Load the CSV file
    landuse_data = pd.read_csv(csv_path)
    print("CSV columns:", landuse_data.columns.tolist())  # Debugging line

    # Clean column names by stripping whitespace
    gdf.columns = gdf.columns.str.strip()
    landuse_data.columns = landuse_data.columns.str.strip()

    # Convert 'group' column in shapefile and 'id' column in CSV to string for merging
    gdf['group'] = gdf['group'].astype(str)
    landuse_data['id'] = landuse_data['id'].astype(str)

    # Merge the GeoDataFrame with the land use data on 'group' and 'id'
    gdf = gdf.merge(landuse_data, left_on='group', right_on='id', how='left')

    # Create a base map centered on a specific location
    m = folium.Map(location=[-31.9505, 115.8605], zoom_start=10)  # Centered on Perth, WA

    # Add OpenStreetMap layer
    folium.TileLayer('OpenStreetMap').add_to(m)

    # Function to color the polygons based on land use type
    def style_function(feature):
        return {
            'fillColor': '#ffffff',
            'color': '#000000',
            'weight': 1,
            'fillOpacity': 0.6
        }

    # Add GeoJSON layer to the map with tooltips for land use percentages
    folium.GeoJson(
        gdf,
        style_function=style_function,
        tooltip=folium.GeoJsonTooltip(
            fields=['group', 'trees', 'shrub', 'grass', 'crop', 'built', 'bare', 'water', 'wetland'],
            aliases=['Group', 'Trees (%)', 'Shrub (%)', 'Grass (%)', 'Crop (%)', 'Built (%)', 'Bare (%)', 'Permanant Water (%)', 'Wetland (%)'],
            localize=True
        )
    ).add_to(m)

    # Determine the output HTML file path
    output_html_path = os.path.join(os.path.dirname(csv_path), 'landuse_map.html')

    # Save the map to an HTML file
    m.save(output_html_path)
    print(f"Map has been saved to '{output_html_path}'.")

# Main function to handle command-line arguments
print(f"landuse_map.py version {__version__}")
def main():
    parser = argparse.ArgumentParser(description='Visualize shapefile and land use data on a Leaflet map.')
    parser.add_argument('shapefile', type=str, help='Path to the shapefile (.shp)')
    parser.add_argument('csv', type=str, help='Path to the CSV file with land use data')

    args = parser.parse_args()

    visualize_shapefile_with_landuse(args.shapefile, args.csv)

if __name__ == '__main__':
    main()