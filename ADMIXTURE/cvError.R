library(ggplot2)

cv <- read.csv("/Users/xavierbarton/bioinformatics/newData-full-II/totalMap-notFiltered/admixture/cv/cvs.txt", 
               sep = ":", 
               header =  FALSE,
               col.names = c("K Value", "CV Error"))

cv$K.Value <- as.numeric(gsub("[^0-9]", "", cv$K.Value))

cv <- cv[order(cv$K.Value), ]

ggplot(data = cv, aes(x = K.Value, y = CV.Error)) +
  geom_point() +
  labs(title = "CV Error", x = "K Value", y = "CV Error") +
  theme_minimal()


