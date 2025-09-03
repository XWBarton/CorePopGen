import pysam
import numpy as np
import plotly.graph_objects as go
from tqdm import tqdm
import sys
import math

def create_missingness_heatmap(vcf_file, output_file, max_loci=1000):
    # Open the VCF file
    vcf_reader = pysam.VariantFile(vcf_file)
    
    # Obtain sample names from the header
    samples = list(vcf_reader.header.samples)
    num_samples = len(samples)
    
    # List to store genotype data
    genotype_data = []
    
    # Iterate through VCF records with a progress bar
    for record in tqdm(vcf_reader, desc="Processing VCF"):
        locus_genotypes = []
        for sample in samples:
            genotype = record.samples[sample]['GT']
            if genotype is None or None in genotype:
                locus_genotypes.append(3)  # Missing data
            elif genotype == (0, 0):
                locus_genotypes.append(0)  # Homozygote reference
            elif genotype == (1, 1):
                locus_genotypes.append(2)  # Homozygote alternate
            else:
                locus_genotypes.append(1)  # Heterozygote
        genotype_data.append(locus_genotypes)
    
    # Convert list to numpy array and transpose so rows are samples
    genotype_array = np.array(genotype_data).T
    num_loci = genotype_array.shape[1]
    
    # Downsample if the number of loci exceeds the threshold
    if num_loci > max_loci:
        step = int(math.ceil(num_loci / max_loci))
        print(f"Downsampling data with a step size of: {step}")
        genotype_array = genotype_array[:, ::step]
        num_loci = genotype_array.shape[1]
    
    # Define a custom colour map
    custom_colours = ['#27aeef', '#E6C050', '#D95F4B', '#f1f1f1']
    colour_scale = [
        [0, custom_colours[0]], [0.25, custom_colours[0]],
        [0.25, custom_colours[1]], [0.5, custom_colours[1]],
        [0.5, custom_colours[2]], [0.75, custom_colours[2]],
        [0.75, custom_colours[3]], [1.0, custom_colours[3]]
    ]
    
    # Set tick intervals for the x-axis to reduce label clutter
    tick_step = max(1, round(num_loci / 10, -len(str(num_loci // 10)) + 1))
    
    # Create the interactive heatmap using Plotly's WebGL-based Heatmap for improved rendering
    fig = go.Figure(data=go.Heatmapgl(
        z=genotype_array,
        colorscale=colour_scale,
        colorbar=dict(
            tickvals=[0, 1, 2, 3],
            ticktext=['Homozygote Reference', 'Heterozygote', 'Homozygote Alternate', 'Missing'],
            title='Genotype'
        )
    ))
    
    # Update layout with labels and tick settings
    fig.update_layout(
        xaxis_title='Loci',
        yaxis_title='Individuals',
        yaxis=dict(
            tickmode='array',
            tickvals=np.arange(num_samples),
            ticktext=samples,
            tickfont=dict(size=8)
        ),
        xaxis=dict(
            tickmode='array',
            tickvals=np.arange(0, num_loci, tick_step),
            ticktext=np.arange(0, num_loci, tick_step)
        )
    )
    
    # Save the plot as an HTML file using Plotly's CDN to load Plotly.js (reducing the overall HTML file size)
    fig.write_html(output_file, full_html=True, include_plotlyjs='cdn')
    print(f"Interactive heatmap saved as {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: missingness_smearplot.py <path_to_indexed_vcf.gz_file>")
        sys.exit(1)
    
    vcf_file = sys.argv[1]
    output_file = 'missingness_smearplot-ql.html'
    create_missingness_heatmap(vcf_file, output_file)
    print("Should load in a jiffy, even with large VCF files.")