library(maps)
library(dplyr)
library(readr)
library(leaflet.minicharts)
library(leaflet)
library(sf)
library(htmlwidgets)


# Read the .Q file
admixture_data <- read.table("/Users/xavierbarton/bioinformatics/newData-full-III/5km/admixture/5km.5.Q", header = FALSE)

#Samples from vcf file used for ADMIXTURE analysis
sample_names <- read_tsv("/Users/xavierbarton/bioinformatics/newData-full-III/5km/admixture/samples.txt", col_names = "id")

admixture_data <- data.frame(sample = sample_names, admixture_data)

# Read the population group file
population_info <- read.csv("/Users/xavierbarton/bioinformatics/population_maps/5km.csv")

all <- merge(admixture_data, population_info, by = "id")

shapefile <- st_read('/Users/xavierbarton/bioinformatics/newData-full-III/5km/shapefile/5km shapefile/5km.shp')

# Calculate centroids
centroids <- st_centroid(shapefile)

# Transform CRS to WGS84
centroids_wgs84 <- st_transform(centroids, crs = 4326)

# Extract latitude and longitude
lat_lon <- st_coordinates(centroids_wgs84)

# Convert to a data frame
centroids_df <- data.frame(
  group = shapefile$group,  # Assuming 'id' is a column in your shapefile
  longitude = lat_lon[,1],
  latitude = lat_lon[,2]
)
 
colors <- c("#FF0000", "#428EF4", "#FAB409", "#2E9E49", "#9F75FF")

map <- leaflet() %>%
  addTiles() %>%
  addMinicharts(
    all$lon, all$lat,
    type = "pie",
    chartdata = all[, c("V1", "V2", "V3", "V4", "V5")],
    colorPalette = colors,
    width = 60, height = 60
  )


saveWidget(map, file = "~/Downloads/leaflet_map.html")

################## per group

# Normalize admixture proportions to sum to 1
all <- all %>%
  mutate(total = V1 + V2 + V3 + V4 + V5) %>%
  mutate(across(starts_with("V"), ~ ./total))

# Calculate group-level admixture proportions and average lat/lon
groups <- all %>%
  group_by(group) %>%
  summarise(
    across(starts_with("V"), mean, .names = "mean_{.col}"),
    avg_lat = mean(lat, na.rm = TRUE),
    avg_lon = mean(lon, na.rm = TRUE)
  )

colors <- c("#FF0000", "#428EF4", "#FAB409", "#2E9E49", "#9F75FF")

group_map <- leaflet() %>%
  addProviderTiles(providers$Esri.NatGeoWorldMap) %>%
  addMinicharts(
    groups$avg_lon, groups$avg_lat,
    type = "pie",
    chartdata = groups[, c("mean_V1", "mean_V2", "mean_V3", "mean_V4", "mean_V5")],
    colorPalette = colors,
    width = 60, height = 60,
    showLabels =  TRUE,
    labelText = groups$group,
    labelMinSize = 20,
  )

group_map

saveWidget(map, file = "~/Downloads/group_map.html")
                           