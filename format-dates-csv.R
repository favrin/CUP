setwd("~/Documents/Dropbox/NEO4J-TEST/CUP-try/test")
library(gdata)
library(boot)
#LINES = 1048576
LINES = 10
#data1 <- read.delim("author-journal.csv",header=F,stringsAsFactors=F,,sep=",")
#data2 <- read.csv("article_date2.csv",header=T,stringsAsFactors=F)
data2 <- read.csv("date-to-change.csv",header=T,stringsAsFactors=F)
#colnames(data2)[colnames(data2)=="year"]<- "aaa"


one <- data2[,2]
#two <- strsplit(data[,2], " ")
two <- rep(0,LINES)
three <- rep(0,LINES)
four <- rep(0,LINES)
five <- rep(0,LINES)
#six <- "20"
seven <- rep(0,LINES)
#three <-data2[1,]

#for (x in 1:27372)
for (x in 1:LINES)
  {
  
  four[x] <- strsplit(one[x], "-")
  mat2 <- matrix(unlist(four[x]),ncol=3)
  five[x]<-mat2[1]
  #seven[x]=paste(six,five[x], sep="")
  
}
write(five, file="dates.txt",ncolumns =1, append=FALSE)

#fit <- kmeans(three, 4)

#test <- cbind(two,three)
#rownames(my.two) <- my.data[,1]
#one <- letters[seq(1,9)]
#one <- seq(1,9)
#test2 <- cbind(one,three,four,three)
#four <-seq(21,29)

#groups<-rep(seq(1,2),each=2)


#tmp<-matrix(nrow=dim(test2)[1],ncol=dim(test2)[2])
#for(i in 1:dim(test2)[2]){
# tmp[,i]<- test2[,i] 
#}

#means<-matrix(nrow=dim(data)[1],ncol=2)
#for(i in 1:dim(means)[1]){
  #means[i,]<-by(sigs.all[i,],groups,mean)
##  means[i,]<-tapply(tmp[i,],INDEX=groups,FUN=mean)
#}
#plot(means[,1],means[,2])


