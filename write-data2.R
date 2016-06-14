setwd("~/Documents/Dropbox/NEO4J-TEST/CUP-try")
library(gdata)
library(boot)
library(xlsx)
#LINES = 629697
LINES = 300
#data1 <- read.delim("author-journal.csv",header=F,stringsAsFactors=F,,sep=",")
#data2 <- read.csv("article_date2.csv",header=T,stringsAsFactors=F)
data <- read.xlsx("authordata.xlsx",header=T,sheetName=1,colIndex=pippo)
#colnames(data2)[colnames(data2)=="year"]<- "aaa"

#seven <- rep(0,LINES)
#three <-data2[1,]
one1<-rep(0,LINES)
one3<-rep(0,LINES) 
one5<-rep(0,LINES) 
one7<-rep(0,LINES) 
one8<-rep(0,LINES) 
one13<-rep(0,LINES) 
one15<-rep(0,LINES) 
one16<-rep(0,LINES) 
one20<-rep(0,LINES) 

two1<-rep(0,LINES) 
two3<-rep(0,LINES) 
two5<-rep(0,LINES) 
two7<-rep(0,LINES) 
two8<-rep(0,LINES) 
two13<-rep(0,LINES) 
two15<-rep(0,LINES) 
two16<-rep(0,LINES) 
two20<-rep(0,LINES) 

onenum<-rep(0,LINES) 

#for (x in 1:27372)
for (x in 1:LINES)
  {
  one1[x]<-gsub("\\(","",data[x,1]) 
  one3[x]<-gsub("\\(","",data[x,3]) 
  one5[x]<-gsub("\\(","",data[x,5]) 
  one7[x]<-gsub("\\(","",data[x,7])
  one8[x]<-gsub("\\(","",data[x,8]) 
  one13[x]<-gsub("\\(","",data[x,13])
  one15[x]<-gsub("\\(","",data[x,15])
  one16[x]<-gsub("\\(","",data[x,16])
  one20[x]<-gsub("\\(","",data[x,20])
  
  two1[x]<-gsub("\\)","",one1[x]) 
  two3[x]<-gsub("\\)","",one3[x]) 
  two5[x]<-gsub("\\)","",one5[x])
  two7[x]<-gsub("\\)","",one7[x]) 
  two8[x]<-gsub("\\)","",one8[x]) 
  two13[x]<-gsub("\\)","",one13[x]) 
  two15[x]<-gsub("\\)","",one15[x]) 
  two16[x]<-gsub("\\)","",one16[x]) 
  two20[x]<-gsub("\\)","",one20[x]) 
  
  #onenum[x]<- lapply(two1[x],as.numeric)
  two7[x] = paste(two7[x],two8[x],sep=" ")
  
}
three <- cbind(two1,two3,two5,two7,two8,two13,two15,two16,two20)
four <- t(three)
#write(four, file="author_journalV4.txt",ncolumns =9, append=FALSE, sep="|")
write(two1, file="articleidV5.txt",ncolumns =1, append=FALSE)
