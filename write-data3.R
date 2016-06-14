setwd("~/Documents/Dropbox/NEO4J-TEST/CUP-try")
library(gdata)
library(boot)
#library(xlsx)
#LINES = 629697
#LINES = 300
#data1 <- read.delim("author-journal.csv",header=F,stringsAsFactors=F,,sep=",")
#data2 <- read.csv("article_date2.csv",header=T,stringsAsFactors=F)
data <- read.csv("authordata3.csv",header=T,stringsAsFactors=F)
#colnames(data2)[colnames(data2)=="year"]<- "aaa"

one<-data[,1]
#seven <- rep(0,LINES)
#three <-data2[1,]
three<-data[,3]
four<-data[,4]
five<-data[,5]
nine<-data[,9]


#for (x in 1:27372)


write(nine, file="articleidV1.txt",ncolumns =1, append=FALSE)
