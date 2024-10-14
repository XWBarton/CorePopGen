library(ggplot2)
library(reshape2)
library(pheatmap)

# Read the matrix from a text file
distance <- read.csv("path/to/csv/of/euc-distance.csv", header = TRUE)

rownames(distance) <- colnames(distance)

pheatmap(distance)
