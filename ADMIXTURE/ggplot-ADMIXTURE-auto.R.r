library(dplyr)
library(tidyr)
library(ggplot2)
library(cowplot)
library(readr)

#how many populations (Q files) are there?
max_k <- 10

# Define the base file path and the range of K values
base_path <- "/Users/xavierbarton/bioinformatics/newData-full-III/subsets/subOne/admixture/subOne."
k_values <- 2:max_k

#Samples from vcf file used for ADMIXTURE analysis
sample_names <- read.csv("/Users/xavierbarton/bioinformatics/newData-full-III/subsets/subOne/admixture/samples.txt", header = FALSE, col.names = "samples")

# Function to read a file given the K value# Function to read a file given the K valueFALSE
read_q_file <- function(k) {
  file_path <- paste0(base_path, k, ".Q")
  read.table(file_path)
}

# Read all files into a list
q_list <- lapply(k_values, read_q_file)

# Assign each element of the list to a uniquely named data frame
for (i in seq_along(q_list)) {
  assign(paste0("q", k_values[i]), q_list[[i]])
}

#to store plots
plot_list <- list()

# Loop through each q data frame
for (i in seq_along(q_list)) {
  q_data <- q_list[[i]]
  
  # Create the data frame
  all_data <- data.frame(sample = sample_names, q_data)
  
  # Order the data by Q values
  ord_data <- all_data[do.call(order, all_data[, -1]), ]
  
  # Set the sample factor levels based on the ordered data
  ord_data$sample <- factor(ord_data$sample, levels = ord_data$sample)
  
  # Convert the data to long format for ggplot2
  long_data <- ord_data %>%
    pivot_longer(cols = starts_with("V"), names_to = "Q", values_to = "value")
  
  # Create the plot
  plot <- ggplot(long_data, aes(x = sample, y = value, fill = Q)) + 
    geom_bar(stat = "identity", position = "stack") +
    xlab(NULL) + ylab(NULL) +
    theme_bw() +
    theme(axis.text.x = element_text(angle = 60, hjust = 1, size = 2), legend.position = "none", 
          axis.text.y = element_blank(), axis.ticks.y = element_blank()) +
    scale_fill_manual(values = rainbow(ncol(q_data)), name = "K") +
    guides(fill = guide_legend(title = "Q")) +
    ggtitle(paste("Plot for K=", k_values[i], sep = ""))
  
  # Store the plot in the list with a unique name
  plot_list[[paste0("q", k_values[i], "_plot")]] <- plot
  
  # Print the plot to view it
  print(plot)
}

#makes a multi-figure plot
combined_plot <- cowplot::plot_grid(plotlist = plot_list, ncol = 1)

# Plot height in inches (4 cm = 1.57 inches)
plot_height_in_inches <- 4 / 2.54

# Number of plots per page
plots_per_page <- floor(11.69 / plot_height_in_inches)  # Calculate based on A4 page height

# Number of pages required
num_pages <- ceiling(max_k / plots_per_page)

# Create and save the multi-page PDF
pdf("~/Downloads/ADMIXTURE-plots.pdf", width = 8.27, height = 11.69)

for (i in 1:num_pages) {
  start_index <- (i - 1) * plots_per_page + 1
  end_index <- min(i * plots_per_page, max_k)
  current_plots <- plot_list[start_index:end_index]
  
  # Create a combined plot for the current page
  combined_plot <- cowplot::plot_grid(plotlist = current_plots, ncol = 1, rel_heights = rep(1, length(current_plots)))
  
  # Print the combined plot to the PDF
  print(combined_plot)
}

dev.off()
