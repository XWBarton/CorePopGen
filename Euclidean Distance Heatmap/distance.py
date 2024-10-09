import pysam
import numpy as np
from scipy.spatial.distance import pdist, squareform
import argparse
import os
from tqdm import tqdm

#Script that reads in VCF file and outputs a csv distance matrix of individuals

def parse_vcf(vcf_file):
    # Check if the VCF file is indexed
    if not os.path.exists(vcf_file + ".tbi"):
        raise FileNotFoundError(f"Index file for {vcf_file} not found. Please index the VCF file using tabix.")
    
    vcf = pysam.VariantFile(vcf_file)
    samples = vcf.header.samples
    genotypes = {sample: [] for sample in samples}

    total_records = sum(1 for _ in vcf.fetch())  # Get total number of records for the progress bar
    vcf.reset()  # Reset the iterator to the beginning of the file

    for record in tqdm(vcf.fetch(), total=total_records, desc="Parsing VCF"):
        for sample in samples:
            genotype = record.samples[sample]['GT']
            # Convert genotype to 0, 1, 2 format (homozygous ref, heterozygous, homozygous alt)
            if genotype == (0, 0):
                genotypes[sample].append(0)
            elif genotype == (0, 1) or genotype == (1, 0):
                genotypes[sample].append(1)
            elif genotype == (1, 1):
                genotypes[sample].append(2)
            else:
                genotypes[sample].append(np.nan)  # Handle missing data

    return genotypes, samples

def calculate_genetic_distance(genotypes, samples):
    # Convert genotypes to numpy array
    genotype_matrix = np.array([genotypes[sample] for sample in samples])
    
    # Handle missing data by imputing the mean genotype value for each SNP
    nan_mask = np.isnan(genotype_matrix)
    mean_genotype = np.nanmean(genotype_matrix, axis=0)
    genotype_matrix[nan_mask] = np.take(mean_genotype, np.where(nan_mask)[1])
    
    # Calculate pairwise genetic distance (Euclidean distance)
    distance_matrix = squareform(pdist(genotype_matrix, metric='euclidean'))
    
    return distance_matrix

def save_matrix_to_file(matrix, samples, output_file):
    # Save the distance matrix to a CSV file
    np.savetxt(output_file, matrix, delimiter=',', fmt='%.6f', header=','.join(samples), comments='')

def main(vcf_file, output_file):
    genotypes, samples = parse_vcf(vcf_file)
    distance_matrix = calculate_genetic_distance(genotypes, samples)
    
    print("Genetic Distance Matrix:")
    print(distance_matrix)
    print("Samples:")
    print(samples)
    
    save_matrix_to_file(distance_matrix, samples, output_file)
    print(f"Genetic distance matrix saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate genetic distance between individuals from a VCF file.")
    parser.add_argument("vcf_file", type=str, help="Path to the VCF file.")
    parser.add_argument("output_file", type=str, help="Path to the output CSV file.")
    args = parser.parse_args()
    
    main(args.vcf_file, args.output_file)