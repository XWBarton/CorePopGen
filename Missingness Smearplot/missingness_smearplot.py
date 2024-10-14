__version__ = "1.0.0"

import pysam
import numpy as np
import plotly.graph_objects as go
from tqdm import tqdm
import sys

def create_missingness_heatmap(vcf_file, output_file):
    
    # Open the VCF file
    vcf_reader = pysam.VariantFile(vcf_file)
    
    # Get the number of samples and create a list of sample names
    samples = list(vcf_reader.header.samples)
    num_samples = len(samples)
    
    # Initialize an empty list to store the genotype data
    genotype_data = []
    
    # Iterate through the VCF records with a progress bar
    for record in tqdm(vcf_reader, desc="Processing VCF"):
        # Create a list to store genotypes for this locus
        locus_genotypes = []
        for sample in samples:
            genotype = record.samples[sample]['GT']
            if genotype is None or None in genotype:
                locus_genotypes.append(3)  # NA or missing
            elif genotype == (0, 0):
                locus_genotypes.append(0)  # Homozygote reference
            elif genotype == (1, 1):
                locus_genotypes.append(2)  # Homozygote alternate
            else:
                locus_genotypes.append(1)  # Heterozygote
        genotype_data.append(locus_genotypes)
    
    # Convert the list to a numpy array and transpose it
    genotype_array = np.array(genotype_data).T
    
    # Define the custom colormap
    custom_colors = ['#27aeef', '#E6C050', '#D95F4B', '#f1f1f1']  
    color_scale = [[0, custom_colors[0]], [0.25, custom_colors[0]],
                   [0.25, custom_colors[1]], [0.5, custom_colors[1]],
                   [0.5, custom_colors[2]], [0.75, custom_colors[2]],
                   [0.75, custom_colors[3]], [1.0, custom_colors[3]]]
    
    # Calculate the step value for x-axis tick marks
    num_loci = genotype_array.shape[1]
    step_value = max(1, round(num_loci / 10, -len(str(num_loci // 10)) + 1)) # Round to nearest significant figure
    
    # Create the interactive heatmap
    fig = go.Figure(data=go.Heatmap(
        z=genotype_array,
        colorscale=color_scale,
        colorbar=dict(
            tickvals=[0, 1, 2, 3],
            ticktext=['Homozygote Reference', 'Heterozygote', 'Homozygote Alternate', 'Missing'],
            title='Genotype'
        )
    ))
    
    # Update layout with labels and title
    fig.update_layout(
        xaxis_title='Loci',
        yaxis_title='Individuals',
        yaxis=dict(tickmode='array', tickvals=np.arange(num_samples), ticktext=samples, tickfont=dict(size=8)),
        xaxis=dict(tickmode='array', tickvals=np.arange(0, num_loci, step=step_value), ticktext=np.arange(0, num_loci, step=step_value))
    )
    
    # Save the plot as an HTML file
    fig.write_html(output_file)
    print(f"Interactive heatmap saved as {output_file}")

if __name__ == "__main__":
    print(f"missingness_smearplot.py version {__version__}")
    if len(sys.argv) != 2:
        print("Usage: missingness_smearplot.py <path_to_indexed_vcf.gz_file>")
        sys.exit(1)
    
    vcf_file = sys.argv[1]
    output_file = 'missingness_smearplot.html'
    create_missingness_heatmap(vcf_file, output_file)
    print("If VCF file is large, html plot may take a couple of minutes to load. There's alotta SNPs!")