import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import unary_union
import folium

def main(input_csv, radius_km):
    # Load the CSV file
    df = pd.read_csv(input_csv)

    # Convert the DataFrame to a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326")

    # Project to UTM (automatically chooses the appropriate UTM zone)
    gdf = gdf.to_crs(gdf.estimate_utm_crs())

    # Convert radius from kilometers to meters
    radius_m = radius_km * 1000

    # Group by the 'group' column
    grouped = gdf.groupby('group')

    # Create a list to store the merged geometries
    merged_geometries = []

    # Iterate over each group
    for name, group in grouped:
        # Buffer the points by the specified radius
        buffered_points = group.geometry.buffer(radius_m)

        # Merge the buffered geometries
        merged_geometry = unary_union(buffered_points)

        # Create a convex hull to fill any gaps
        filled_geometry = merged_geometry.convex_hull

        merged_geometries.append({'group': name, 'geometry': filled_geometry})

    # Create a new GeoDataFrame with the merged geometries
    merged_gdf = gpd.GeoDataFrame(merged_geometries, crs=gdf.crs)

    # Save the merged geometries to a shapefile
    merged_gdf.to_file("output_shapes.shp")

    # Reproject back to WGS 84 for folium
    merged_gdf = merged_gdf.to_crs("EPSG:4326")

    # Create a folium map centered on the average location
    center_lat = df['lat'].mean()
    center_lon = df['lon'].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=10, tiles=None)  # Set tiles to None

    # Add the Esri National Geographic World Map tile layer
    folium.TileLayer('Esri.NatGeoWorldMap').add_to(m)

    # Add the merged geometries to the map
    folium.GeoJson(
        merged_gdf.__geo_interface__,
        name="Merged Geometries",
        style_function=lambda feature: {
            'fillColor': 'red',
            'color': 'red',
            'weight': 2,
            'fillOpacity': 0.5,
        }
    ).add_to(m)

    # Add individual points as CircleMarkers to the map
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=3,  # Radius of the circle
            color='red',
            fill=True,
            fill_color='red',
            fill_opacity=0.7,
            popup=f"ID: {row['id']}, Group: {row['group']}"
        ).add_to(m)

    # Add layer control to toggle between layers
    folium.LayerControl().add_to(m)

    # Save the map to an HTML file
    m.save('output_map.html')

    print("Map has been saved as 'output_map.html'.")
    print("Shapefile has been saved as 'output_shapes.shp'.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_csv> <radius_km>")
        sys.exit(1)

    input_csv = sys.argv[1]
    radius_km = float(sys.argv[2])

    main(input_csv, radius_km)