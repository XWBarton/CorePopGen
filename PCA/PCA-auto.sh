#!/bin/zsh

#workflow
#https://www.notion.so/xavierbarton/PCA-Plot-b24742efe3214d4381ab13685a809572?pvs=4

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# Check if the user provided a VCF file as an argument
if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <vcf_file> (not gzipped)"
  exit 1
fi

# Assign the first argument to a variable
vcf_file=$1

# Check if the file exists
if [[ ! -f $vcf_file ]]; then
  echo "File not found: $vcf_file"
  exit 1
fi

#change contig names for plink2
sed '/^[[:digit:]]/s/^/contig/' "$vcf_file" > contigs.vcf

#make plink2 binary
plink2 --vcf contigs.vcf --out snps --double-id --allow-extra-chr

#LD prune
plink2 --pfile snps --indep-pairwise 50 5 0.4 --allow-extra-chr --out snps

#extract pruned SNPs
plink2 --pfile snps --extract snps.prune.in --make-pgen --out pruned_snps --allow-extra-chr

#generate PCA
plink2 --pfile pruned_snps --pca --out pca_results

#clean up
#comment out if you want to keep intermittent files
rm contigs.vcf pca_results.log	 pruned_snps.log pruned_snps.pgen pruned_snps.psam pruned_snps.pvar snps.log snps.pgen snps.prune.in snps.prune.out snps.psam snps.pvar

#run PCA through R
python plink2PCA.py