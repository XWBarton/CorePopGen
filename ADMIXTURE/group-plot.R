library(ggplot2)
library(tidyr)
library(readr)
library(dplyr)

colors <- c("#FF0000", "#428EF4", "#FAB409", "#2E9E49", "#9F75FF")

# Read the .Q file
admixture_data <- read.table("/Users/xavierbarton/bioinformatics/newData-full-III/5km/admixture/5km.5.Q", header = FALSE)

#Samples from vcf file used for ADMIXTURE analysis
sample_names <- read_tsv("/Users/xavierbarton/bioinformatics/newData-full-III/5km/admixture/samples.txt", col_names = "id")

admixture_data <- data.frame(sample = sample_names, admixture_data)

# Read the population group file
population_info <- read.csv("/Users/xavierbarton/bioinformatics/population_maps/5km.csv")

#Merge
combined_data <- merge(admixture_data, population_info, by = "id")

#Sites
centroids <- combined_data %>%
  group_by(group) %>%
  summarise(
    mean_lat = mean(lat),
    mean_lon = mean(lon)
  )

# Calculate the distance matrix
dist_matrix <- as.matrix(dist(centroids[, c("mean_lat", "mean_lon")]))

# Perform hierarchical clustering
hc <- hclust(as.dist(dist_matrix))

# Order groups based on clustering
ordered_groups <- centroids$group[hc$order]

combined_data$group <- factor(combined_data$group, levels = ordered_groups)

combined_data$assigned_color <- apply(combined_data[, c("V1", "V2", "V3", "V4", "V5")], 1, function(row) {
  max_col <- which.max(row)  # Find the index of the column with the highest value
  colors[max_col]            # Assign the corresponding color
})

# Reshape the data
df_long <- pivot_longer(
  combined_data,
  cols = V1:V5,
  names_to = "Population",
  values_to = "Proportion"
)

df_long$id_numeric <- as.numeric(sub("_.*", "", df_long$id))

p <- ggplot(df_long, aes(x = factor(id_numeric), y = Proportion, fill = Population)) +
  geom_bar(stat = "identity", width = 0.8) +
  facet_grid(~ group, scales = "free_x", space = "free_x") +
  labs(x = "Individual", y = "Ancestry Proportion") +
  theme_minimal() +
  theme(axis.text.x = element_blank(),
        strip.text.x = element_text(size = 8, face = "bold"),
        panel.background = element_rect(fill = "white"),
        plot.background = element_rect(fill = "white"),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        strip.background = element_blank()) +
  scale_fill_manual(values = colors)

p

ggsave(filename = "~/Downloads/structure_plot.png", plot = p, width = 10, height = 1, dpi = 300)

#Extract Colours

# Filter the dataframe to keep only unique ids
df_unique <- df_long[!duplicated(df_long$id), ]

# Select only the id and assigned_color columns
df_colour <- df_unique[, c("id", "assigned_color")]

# Save the result to a CSV file
write.csv(df_colour, "~/bioinformatics/filtered_ids_colors.csv", row.names = FALSE)
