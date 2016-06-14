setwd("~/Documents/Dropbox/NEO4J-TEST/CUP-try")
library(gdata)
library(boot)
#library(xlsx)
#LINES = 629697
LINES = 10
subdata <- read.delim("cup003b_subs_data.txt",header=T,stringsAsFactors=F,,sep="|",nrows=LINES)

unique <- matrix(0,LINES,5)
#for (x in 1:27372)
k=1
for (x in 1:LINES)
  {
  for (y in 1:LINES){
    if(unique[y,5]==0){#check this better
     #print("one");
      if (unique[y,1]==subdata[x,1] && unique[y,2]==subdata[x,4] && unique[y,3]==subdata[x,5] && unique[y,4]==subdata[x,8]){
        print("one");
        unique[y,5]=k;
        k = k+1;
    }else{
      print("two");
      unique[y,1]=subdata[x,1];
      unique[y,2]=subdata[x,4];
      unique[y,3]=subdata[x,5];
      unique[y,4]=subdata[x,8];
      unique[y,5]=k;
    }
    }
  }
}

#write(two1, file="articleidV5.txt",ncolumns =1, append=FALSE)
