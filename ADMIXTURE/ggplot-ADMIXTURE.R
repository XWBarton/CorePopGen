library(tibble)
library(tidyverse)
library(dplyr)


#blank
all_data <- tibble(sample=character(),
                   k=numeric(),
                   Q=character(),
                   value=numeric())

#samples
samplelist <- read_tsv("~/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/sample_names.txt",
                       col_names = "sample")

#read loop
for (k in 1:5){
  data <- read_delim(paste0("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/cv/full-II-admix.",k,".Q"),
                     col_names = paste0("Q",seq(1:k)),
                     delim=" ")
  data$sample <- samplelist$sample
  data$k <- k
  
  #This step converts from wide to long.
  data %>% gather(Q, value, -sample,-k) -> data
  all_data <- rbind(all_data,data)
}
all_data

all_data %>%
  filter(k == 2) %>%
  ggplot(.,aes(x=sample,y=value,fill=factor(Q))) + 
  geom_bar(stat="identity",position="stack") +
  xlab("Sample") + ylab("Ancestry") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_brewer(palette="Set1",name="K",
                    labels=c("1","2"))

all_data %>%
  filter(k != 1) %>%
  ggplot(.,aes(x=sample,y=value,fill=factor(Q))) + 
  geom_bar(stat="identity",position="stack") +
  xlab("Sample") + ylab("Ancestry") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_brewer(palette="Set1",name="K",
                    labels=seq(1:5)) +
  facet_wrap(~k,ncol=1)

############

library(pophelper)

k3 <- readQ(files = "/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/cv/full-II-admix.3.Q", filetype = "auto")
k4 <- readQ(files = "/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/cv/full-II-admix.4.Q", filetype = "auto")


plotQ(k4, exportpath = "~/Downloads", sortind = "all")


##############

# https://nicocriscuolo.shinyapps.io/StructuRly/

#############

all_data <- data.frame(sample = sample_names, q2)

# Order the data by Q values
ord2 <- all_data[order(all_data$V1, all_data$V2), ]

# Set the sample factor levels based on the ordered data
ord2$sample <- factor(ord2$sample, levels = ord2$sample)

# Convert the data to long format for ggplot2
long_data <- ord2 %>%
  pivot_longer(cols = starts_with("V"), names_to = "Q", values_to = "value")

# Plot using ggplot2
ggplot(long_data, aes(x = sample, y = value, fill = Q)) + 
  geom_bar(stat = "identity", position = "stack") +
  xlab("Individual #") + ylab("Ancestry") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_manual(values = rainbow(2), name = "K") +
  guides(fill = guide_legend(title = "Q"))

all_data <- data.frame(sample = sample_names, q3)

# Order the data by Q values
ord3 <- all_data[order(all_data$V1, all_data$V2, all_data$V3), ]

# Set the sample factor levels based on the ordered data
ord3$sample <- factor(ord3$sample, levels = ord3$sample)

# Convert the data to long format for ggplot2
long_data <- ord3 %>%
  pivot_longer(cols = starts_with("V"), names_to = "Q", values_to = "value")

# Plot using ggplot2
ggplot(long_data, aes(x = sample, y = value, fill = Q)) + 
  geom_bar(stat = "identity", position = "stack") +
  xlab("Individual #") + ylab("Ancestry") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_manual(values = rainbow(3), name = "K") +
  guides(fill = guide_legend(title = "Q"))
