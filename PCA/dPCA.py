import pandas as pd
import vcfpy
from sklearn.decomposition import PCA
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from joblib import Parallel, delayed
import numpy as np

def load_population_file(population_file):
    """Load the population file and return a dictionary mapping IDs to groups."""
    pop_df = pd.read_csv(population_file, sep='\t')
    return dict(zip(pop_df['id'], pop_df['groups']))

def load_vcf(vcf_file, id_to_group):
    """Load the VCF file and return a genotype matrix and corresponding labels."""
    reader = vcfpy.Reader.from_path(vcf_file)
    samples = reader.header.samples.names
    genotypes = []
    labels = []

    for record in reader:
        row = []
        for call in record.calls:
            gt = call.data.get('GT', './.')
            if gt == './.':
                row.append(np.nan)
            else:
                alleles = gt.split('/')
                row.append(sum(map(int, alleles)))
        genotypes.append(row)

    genotype_matrix = np.array(genotypes).T  # Transpose to have samples as rows
    labels = [id_to_group.get(sample, 'Unknown') for sample in samples]
    
    return genotype_matrix, labels

def perform_dpca(genotype_matrix, labels):
    """Perform dPCA using PCA followed by LDA."""
    # Fill missing values with the mean of each column
    genotype_matrix = np.nan_to_num(genotype_matrix, nan=np.nanmean(genotype_matrix, axis=0))
    
    # Perform PCA
    pca = PCA(n_components=10)
    pca_result = pca.fit_transform(genotype_matrix)

    # Perform LDA
    lda = LDA(n_components=2)
    lda_result = lda.fit_transform(pca_result, labels)
    
    return lda_result

def main(vcf_file, population_file):
    id_to_group = load_population_file(population_file)
    genotype_matrix, labels = load_vcf(vcf_file, id_to_group)

    # Perform dPCA using multithreading
    result = Parallel(n_jobs=-1)(delayed(perform_dpca)(genotype_matrix, labels))

    print("dPCA Result:")
    print(result)

if __name__ == "__main__":
    vcf_file = 'path/to/your/input.vcf'
    population_file = 'path/to/your/population.tsv'
    main(vcf_file, population_file)