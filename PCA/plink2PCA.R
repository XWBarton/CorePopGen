# Set CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org/"))

# Install required packages if not already installed
packages <- c("ggplot2", "plotly", "tidyverse", "htmlwidgets")

new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if (length(new_packages)) install.packages(new_packages)

# Load the packages
library(ggplot2)
library(plotly)
library(tidyverse)
library(htmlwidgets)

#read in command line
args <- commandArgs(trailingOnly = TRUE)

#read in data
pca <- read_table2("pca_results.eigenvec", col_names = TRUE)
eigenval <- scan("pca_results.eigenval")

# remove nuisance column
pca <- pca[,-1]

# set headers
names(pca)[1] <- "ind"
names(pca)[2:ncol(pca)] <- paste0("PC", 1:(ncol(pca)-1))

#recode
#pca$ind <- substr(pca$ind, 5, nchar(pca$ind))

#add sites
samples <- read.csv("~/bioinformatics/sample_info.csv")
names(samples)[1] <- "ind"

#join 
pca_samples <- merge(pca, samples, by = "ind")

#plotting
#convert to variance explained to %
pve <- data.frame(PC = 1:10, pve = eigenval/sum(eigenval)*100)

#barplot that shows how much each varient explains 
a <- ggplot(pve, aes(PC, pve)) + geom_bar(stat = "identity")
a + ylab("Percentage variance explained") + theme_minimal()

#plot
plot <- ggplot(pca_samples, aes(PC2, PC1, col = river)) + geom_point(size = 3)

plot <- ggplot(pca_samples, aes(x = PC1, y = PC2, color = river, text = paste("Location:", site, "<br>Sample:", ind, "<br>Collection Year:", year ))) +
  geom_text(aes(label = code)) +
  labs(x = "x", y = "y") +
  theme_minimal() +
  theme(legend.position = "none")

p_interactive <- ggplotly(plot, tooltip = c("text"))
p_interactive

saveWidget(p_interactive, "PCA.html")
