import sys
import os
import cyvcf2
import numpy as np
import networkx as nx
from tqdm import tqdm
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

def process_vcf(vcf_file):
    vcf = cyvcf2.VCF(vcf_file)
    sample_names = vcf.samples
    variants = []
    
    for variant in tqdm(vcf, desc="Processing VCF"):
        variants.append(variant.genotypes)
    return sample_names, np.array(variants)

def allelic_differences(sample1, sample2):
    differences = 0
    for allele1, allele2 in zip(sample1, sample2):
        if allele1[0] != allele2[0]:
            differences += 1
        if allele1[1] != allele2[1]:
            differences += 1
    return differences

def calculate_distances(variants):
    print("Calculating distances...")
    n_samples = variants.shape[1]
    dist_matrix = np.zeros((n_samples, n_samples), dtype=int)
    
    for i in tqdm(range(n_samples), desc="Calculating distance matrix"):
        for j in range(i + 1, n_samples):
            dist_matrix[i, j] = allelic_differences(variants[:, i], variants[:, j])
            dist_matrix[j, i] = dist_matrix[i, j]
    
    return dist_matrix

def create_graph(distances, sample_names, pop_map):
    print("Creating graph...")
    G = nx.Graph()
    for i, name in enumerate(sample_names):
        if name in pop_map.index:
            G.add_node(name, site=pop_map.loc[name, 'site'])
        else:
            print(f"Warning: Sample {name} not found in population map. Setting site to 'Unknown'.")
            G.add_node(name, site='Unknown')
    for i in tqdm(range(len(sample_names)), desc="Adding edges"):
        for j in range(i + 1, len(sample_names)):
            G.add_edge(sample_names[i], sample_names[j], weight=distances[i, j])
    return G

def get_minimum_spanning_tree(G):
    print("Calculating minimum spanning tree...")
    return nx.minimum_spanning_tree(G)

def visualize_mst_interactive(mst, pop_map, output_file):
    print("Visualizing minimum spanning network...")
    pos = nx.spring_layout(mst, k=0.5, iterations=50)
    
    sites = list(set(nx.get_node_attributes(mst, 'site').values()))
    color_map = {site: f'rgb({np.random.randint(0,256)},{np.random.randint(0,256)},{np.random.randint(0,256)})' for site in sites}
    
    edge_trace = go.Scatter(
        x=[], y=[], line=dict(width=0.5, color='#888'), hoverinfo='none', mode='lines')
    
    for edge in mst.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_trace['x'] += (x0, x1, None)
        edge_trace['y'] += (y0, y1, None)
    
    node_trace = go.Scatter(
        x=[], y=[], text=[], mode='markers', hoverinfo='text',
        marker=dict(showscale=False, colorscale='YlGnBu', reversescale=True, color=[], size=10,
                    line_width=2))
    
    for node in mst.nodes():
        x, y = pos[node]
        node_trace['x'] += (x,)
        node_trace['y'] += (y,)
        node_trace['text'] += (f"Sample: {node}<br>Site: {mst.nodes[node]['site']}",)
        node_trace['marker']['color'] += (color_map[mst.nodes[node]['site']],)
    
    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='Minimum Spanning Network',
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20,l=5,r=5,t=40),
                        annotations=[dict(
                            text="",
                            showarrow=False,
                            xref="paper", yref="paper",
                            x=0.005, y=-0.002)],
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )
    
    # Add a legend
    for site, color in color_map.items():
        fig.add_trace(go.Scatter(
            x=[None], y=[None], mode='markers',
            marker=dict(size=10, color=color),
            showlegend=True, name=site
        ))
    
    pio.write_html(fig, file=output_file, auto_open=False)
    print(f"Interactive plot saved as {output_file}")

def load_population_map(csv_file):
    print("Loading population map...")
    pop_map = pd.read_csv(csv_file)
    if 'id' not in pop_map.columns or 'site' not in pop_map.columns:
        raise ValueError("The CSV file must contain 'id' and 'site' columns.")
    pop_map.set_index('id', inplace=True)
    return pop_map

def main(vcf_file, pop_map_file):
    sample_names, variants = process_vcf(vcf_file)
    pop_map = load_population_map(pop_map_file)
    
    # Check for mismatches between VCF samples and population map
    vcf_samples = set(sample_names)
    pop_map_samples = set(pop_map.index)
    missing_samples = vcf_samples - pop_map_samples
    extra_samples = pop_map_samples - vcf_samples
    
    if missing_samples:
        print(f"Warning: The following samples are in the VCF but not in the population map: {missing_samples}")
    if extra_samples:
        print(f"Warning: The following samples are in the population map but not in the VCF: {extra_samples}")
    
    distances = calculate_distances(variants)
    G = create_graph(distances, sample_names, pop_map)
    mst = get_minimum_spanning_tree(G)
    
    # Generate output file name in the same directory as the VCF file
    vcf_dir = os.path.dirname(vcf_file)
    vcf_name = os.path.splitext(os.path.basename(vcf_file))[0]
    output_file = os.path.join(vcf_dir, f"{vcf_name}_MSN.html")
    
    visualize_mst_interactive(mst, pop_map, output_file)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python MSN.py <path_to_vcf_file> <path_to_population_map_csv>")
        sys.exit(1)
    
    vcf_file = sys.argv[1]
    pop_map_file = sys.argv[2]
    main(vcf_file, pop_map_file)