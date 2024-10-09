import geopandas as gpd
import pandas as pd
import folium
import os
import argparse
import io
import matplotlib.pyplot as plt
import pyproj
import branca

# Mapping of numeric codes to land use types
CODE_MAP = {
    '10': 'Trees',
    '20': 'Shrub Land',
    '30': 'Grassland',
    '40': 'Cropland',
    '50': 'Built',
    '60': 'Bare / Sparse Vegetation',
    '70': 'Snow and Ice',
    '80': 'Permanent Water Bodies',
    '90': 'Herbaceous Wetland',
    '95': 'Mangroves',
    '100': 'Moss and Lichen'
}

# Color mapping for each land use type
COLOR_MAP = {
    'Trees': '#006400',
    'Shrub Land': '#FFBB22',
    'Grassland': '#FFFF4C',
    'Cropland': '#F096FF',
    'Built': '#FA0000',
    'Bare / Sparse Vegetation': '#B4B4B4',
    'Snow and Ice': '#F0F0F0',
    'Permanent Water Bodies': '#0064C8',
    'Herbaceous Wetland': '#0096A0',
    'Mangroves': '#00CF75',
    'Moss and Lichen': '#FAE6A0'
}

def pie_charts(csv_path):
    landuse = pd.read_csv(csv_path)
    landuse = landuse.fillna(0)
    
    # Rename columns using CODE_MAP
    landuse.rename(columns=CODE_MAP, inplace=True)
    
    columns = [col for col in landuse.columns if col in COLOR_MAP]
    colors = [COLOR_MAP[col] for col in columns]
    
    pie_charts_data = landuse[columns].values

    fig = plt.figure(figsize=(0.5, 0.5))
    fig.patch.set_alpha(0)
    ax = fig.add_subplot(111)
    plots = []
    for sizes in pie_charts_data:
        ax.pie(sizes, colors=colors)
        buff = io.StringIO()
        plt.savefig(buff, format="SVG")
        buff.seek(0)
        svg = buff.read()
        svg = svg.replace("\n", "")
        plots.append(svg)
        plt.cla()
    plt.clf()
    plt.close()
    
    return plots

def visualiseShapes(shapefiles, csv, plots):
    # Load the shapefile
    shp = gpd.read_file(shapefiles)
    print("Shapefile columns:", shp.columns.tolist())
    print("Original CRS:", shp.crs)
    
    # Reproject to WGS84 (EPSG:4326)
    shp = shp.to_crs(epsg=4326)
    print("Reprojected CRS:", shp.crs)

    # Load the CSV file and rename columns
    landuse = pd.read_csv(csv)
    landuse.rename(columns=CODE_MAP, inplace=True)
    print("CSV columns:", landuse.columns.tolist())

    # Clean column names by stripping whitespace
    shp.columns = shp.columns.str.strip()
    landuse.columns = landuse.columns.str.strip()

    # Convert 'group' column in shapefile and 'id' column in CSV to string for merging
    shp['group'] = shp['group'].astype(str)
    landuse['id'] = landuse['id'].astype(str)

    # Merge the GeoDataFrame with the land use data on 'group' and 'id'
    landshp = shp.merge(landuse, left_on='group', right_on='id', how='left')
    
    print("Merged Shapefiles and Landuse:", landshp.columns.tolist())

    # Create a base map centered on Perth
    m = folium.Map(location=[-31.9505, 115.8605], zoom_start=8, tiles='http://services.arcgisonline.com/arcgis/rest/services/NatGeo_World_Map/MapServer/tile/{z}/{y}/{x}',
           attr="Sources: National Geographic, Esri, Garmin, HERE, UNEP-WCMC, USGS, NASA, ESA, METI, NRCAN, GEBCO, NOAA, INCREMENT P")
    
    # Function to color the polygons 
    def styleShapes(element):
        return {
            'fillColor': '#ffffff',
            'color': '#000000',
            'weight': 1,
            'fillOpacity': 0.6
        }
        
    # Add shapefile elements to map    
    folium.GeoJson(shp, style_function=styleShapes).add_to(m) 

    # Iterate through GeoDataFrame and add pie charts as DivIcons at each location
    for i, (index, row) in enumerate(landshp.iterrows()):
        centroid = row['geometry'].centroid
        coord = [centroid.y, centroid.x]

        marker = folium.Marker(location=coord)
        icon = folium.DivIcon(html=plots[i])
        marker.add_child(icon)

        popup_content = f"Group: {row['group']}<br>"
        for col in COLOR_MAP:
            if col in row:
                color = COLOR_MAP[col]
                code = [k for k, v in CODE_MAP.items() if v == col][0]
                value = row[col]
                popup_content += f"<span style='color:{color};'>■</span> {col}: {value}% (Code: {code})<br>"

        popup = folium.Popup(popup_content)
        marker.add_child(popup)
        m.add_child(marker)
    
    # Determine the output HTML file path
    output_html_path = os.path.join(os.path.dirname(csv), 'landuse_pies.html')

    # Save the map to an HTML file
    m.save(output_html_path)
    print(f"Map has been saved to '{output_html_path}'.")

# Main function to handle command-line arguments
def main():
    parser = argparse.ArgumentParser(description='Visualize shapefile and land use data on a Leaflet map.')
    parser.add_argument('shapefile', type=str, help='Path to the shapefile (.shp)')
    parser.add_argument('csv', type=str, help='Path to the CSV file with land use data')

    args = parser.parse_args()
    
    plots = pie_charts(args.csv)
    
    visualiseShapes(args.shapefile, args.csv, plots)

if __name__ == '__main__':
    main()