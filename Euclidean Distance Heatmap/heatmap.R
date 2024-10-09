library(ggplot2)
library(reshape2)
library(pheatmap)

# Read the matrix from a text file
distance <- read.csv("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/distance/output.csv", header = TRUE)

rownames(distance) <- colnames(distance)

pheatmap(distance)
