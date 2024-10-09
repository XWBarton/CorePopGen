library(ggplot2)
library(dplyr)
library(tidyr)
library(tidyverse)

# K =2
q2 <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/full-II-admix.2.Q")

sample_names <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/sample_names.txt", stringsAsFactors = FALSE)$V1

# Order the data (customize the order as needed)
ord2 <- q2[order(q2$V1, q2$V2), ]

# Plot using barplot
K2 <- barplot(t(as.matrix(ord2)),
              space = c(0.2),
              col = rainbow(2),
              xlab = "Individual #",
              ylab = "Ancestry",
              border = NA)

# K = 3
q3 <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/full-II-admix.3.Q")

sample_names <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/sample_names.txt", stringsAsFactors = FALSE)$V1

# Order the data (customize the order as needed)
ord3 <- q3[order(q3$V1, q3$V2, q3$V3), ]

# Plot using barplot
K3 <- barplot(t(as.matrix(ord3)),
              space = c(0.2),
              col = rainbow(3),
              xlab = "Individual #",
              ylab = "Ancestry",
              border = NA)

# K = 4
q4 <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/full-II-admix.4.Q")

sample_names <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/sample_names.txt", stringsAsFactors = FALSE)$V1

# Order the data (customize the order as needed)
ord4 <- q4[order(q4$V1, q4$V2, q4$V3, q4$V4), ]

# Plot using barplot
K4 <- barplot(t(as.matrix(ord4)),
              space = c(0.2),
              col = rainbow(4),
              xlab = "Individual #",
              ylab = "Ancestry",
              border = NA)

# K = 5
q5 <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/full-II-admix.5.Q")

sample_names <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/sample_names.txt", stringsAsFactors = FALSE)$V1

# Order the data (customize the order as needed)
ord5 <- q5[order(q5$V1, q5$V2, q5$V3, q5$V4, q5$V5), ]

# Plot using barplot
K5 <- barplot(t(as.matrix(ord5)),
              space = c(0.2),
              col = rainbow(5),
              xlab = "Individual #",
              ylab = "Ancestry",
              border = NA)


# K = 6
q6 <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/full-II-admix.6.Q")

sample_names <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/sample_names.txt", stringsAsFactors = FALSE)$V1

# Order the data (customize the order as needed)
ord6 <- q6[order(q6$V1, q6$V2, q6$V3, q6$V4, q6$V5, q6$V6), ]

# Plot using barplot
K6 <- barplot(t(as.matrix(ord6)),
              space = c(0.2),
              col = rainbow(6),
              xlab = "Individual #",
              ylab = "Ancestry",
              border = NA)

# K = 7
q7 <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/full-II-admix.7.Q")

sample_names <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/sample_names.txt", stringsAsFactors = FALSE)$V1

# Order the data (customize the order as needed)
ord7 <- q7[order(q7$V1, q7$V2, q7$V3, q7$V4, q7$V5, q7$V6, q7$V7), ]

# Plot using barplot
K7 <- barplot(t(as.matrix(ord7)),
              space = c(0.2),
              col = rainbow(7),
              xlab = "Individual #",
              ylab = "Ancestry",
              border = NA)

# K = 8
q8 <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/full-II-admix.8.Q")

sample_names <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/sample_names.txt", stringsAsFactors = FALSE)$V1

# Order the data (customize the order as needed)
ord8 <- q8[order(q8$V1, q8$V2, q8$V3, q8$V4, q8$V5, q8$V6, q8$V7, q8$V8), ]

# Plot using barplot
K8 <- barplot(t(as.matrix(ord8)),
              space = c(0.2),
              col = rainbow(8),
              xlab = "Individual #",
              ylab = "Ancestry",
              border = NA)

# K = 9
q9 <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/full-II-admix.9.Q")

sample_names <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/sample_names.txt", stringsAsFactors = FALSE)$V1

# Order the data (customize the order as needed)
ord9 <- q9[order(q9$V1, q9$V2, q9$V3, q9$V4, q9$V5, q9$V6, q9$V7, q9$V8, q9$V9), ]

# Plot using barplot
K9 <- barplot(t(as.matrix(ord9)),
              space = c(0.2),
              col = rainbow(9),
              xlab = "Individual #",
              ylab = "Ancestry",
              border = NA)

# K = 10
q10 <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/full-II-admix.10.Q")

sample_names <- read.table("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/sample_names.txt", stringsAsFactors = FALSE)$V1

# Order the data (customize the order as needed)
ord10 <- q10[order(q10$V1, q10$V2, q10$V3, q10$V4, q10$V5, q10$V6, q10$V7, q10$V8, q10$V9, q10$V10), ]

# Plot using barplot
K10 <- barplot(t(as.matrix(ord10)),
              space = c(0.2),
              col = rainbow(10),
              xlab = "Individual #",
              ylab = "Ancestry",
              border = NA)
#arrange
facet_wrap(K2, K3, K4, K5, K6, K7, K8, K9, K10) 
