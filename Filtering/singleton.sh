#!/bin/zsh

# Define the input VCF file
input_vcf="filtered-III.vcf.gz"

# Get the number of samples
sample_count=$(bcftools query -l $input_vcf | wc -l)

# Calculate the MAF threshold
maf_threshold=$(echo "scale=5; 1/(2*$sample_count)" | bc)

# Run PLINK with the calculated MAF threshold
plink2 --vcf $input_vcf --maf $maf_threshold --export vcf bgz --out filtered-IV

# Print the MAF threshold for verification
echo "MAF threshold used: $maf_threshold"

