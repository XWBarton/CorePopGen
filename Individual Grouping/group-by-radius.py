__version__ = "1.0.0"

import pandas as pd
from geopy.distance import geodesic
import argparse
import folium
import random
import os

class UnionFind:
    def __init__(self, size):
        self.parent = list(range(size))
        self.rank = [1] * size

    def find(self, p):
        if self.parent[p] != p:
            self.parent[p] = self.find(self.parent[p])
        return self.parent[p]

    def union(self, p, q):
        rootP = self.find(p)
        rootQ = self.find(q)
        if rootP != rootQ:
            if self.rank[rootP] > self.rank[rootQ]:
                self.parent[rootQ] = rootP
            elif self.rank[rootP] < self.rank[rootQ]:
                self.parent[rootP] = rootQ
            else:
                self.parent[rootQ] = rootP
                self.rank[rootP] += 1

def load_data(file_path):
    """Load data from a CSV file."""
    return pd.read_csv(file_path)

def group_individuals(data, radius_km):
    """Assign group numbers to individuals using a union-find approach."""
    n = len(data)
    uf = UnionFind(n)

    for i in range(n):
        for j in range(i + 1, n):
            distance = geodesic((data.iloc[i]['lat'], data.iloc[i]['lon']),
                                (data.iloc[j]['lat'], data.iloc[j]['lon'])).km
            if distance <= radius_km:
                uf.union(i, j)

    # Assign group numbers based on connected components
    data['group'] = [uf.find(i) for i in range(n)]
    # Normalize group numbers to be consecutive integers
    data['group'] = pd.Categorical(data['group']).codes

    return data

def plot_groups_on_map(data, output_html):
    """Plot the groups on a map and save to an HTML file."""
    avg_lat = data['lat'].mean()
    avg_lon = data['lon'].mean()
    map_ = folium.Map(location=[avg_lat, avg_lon], zoom_start=13)
    
    colors = ['#'+''.join([random.choice('0123456789ABCDEF') for _ in range(6)]) for _ in range(data['group'].max() + 1)]
    
    for _, row in data.iterrows():
        folium.CircleMarker(
            location=(row['lat'], row['lon']),
            radius=5,
            color=colors[row['group']],
            fill=True,
            fill_opacity=0.7,
            popup=f"ID: {row['id']}, Group: {row['group']}"
        ).add_to(map_)
    
    map_.save(output_html)
    print(f"Map saved to {output_html}")

def main():
    print(f"group-by-radius.py version {__version__}")
    parser = argparse.ArgumentParser(description="Group individuals by geographic proximity and plot them on a map.")
    parser.add_argument('file_path', type=str, help='Path to the CSV file containing id, lat, and lon columns.')
    parser.add_argument('radius_km', type=float, help='Radius in kilometers for grouping individuals.')
    parser.add_argument('output_file', type=str, help='Path to the output CSV file.')
    
    args = parser.parse_args()
    
    data = load_data(args.file_path)
    grouped_data = group_individuals(data, args.radius_km)
    
    grouped_data.to_csv(args.output_file, index=False)
    print(f"Grouped data saved to {args.output_file}")
    
    output_dir = os.path.dirname(args.output_file)
    output_html = os.path.join(output_dir, "map.html")
    
    plot_groups_on_map(grouped_data, output_html)

if __name__ == "__main__":
    main()