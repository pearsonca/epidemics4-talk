setwd("~/Dropbox/") #setwd("/Volumes/Data/dropbox/") 
origin <- as.POSIXct(strptime('2004-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))
debug <- F

con <- gzfile("montreal.tgz")
montreal.df<-read.csv(con,header=F,skip=1, sep=",", col.names=c("user.id","loc.id","start.s","end.s"))
close(con)
montreal.df$duration <- montreal.df$end.s - montreal.df$start.s
## given the small number of these,
## and the unclear origin,
## resolve by manual inspection where duration < 0
if (debug) dump<-apply(subset(montreal.df,duration<0),1,function(row) {
  tot<-subset(montreal.df, user.id == row["user.id"] & loc.id == row["loc.id"] & start.s != row["start.s"] & end.s != row["end.s"] )
  if(dim(tot)[1] == 0) {
    print(row)
    print(ifelse(row["duration"] < (60*5),">5 min; resolve with swap.","<5 min; resolve with delete."))
  } else {
    rowBefore <- tail(subset(tot, end.s < row["start.s"]),n=1)["end.s"]
    rowAfter <- head(subset(tot, start.s > row["end.s"]),n=1)["start.s"]
    if (row["end.s"] < rowBefore) {
      print(row)
      print("Merge into row before.")
    } else if (rowAfter < row["start.s"]) {
      print(row)
      print("Merge into row after.")
    } else {
      print(row)
      print(ifelse(row["duration"] < (60*5),">5 min; resolve with swap.","<5 min; resolve with delete."))
    }
  }
})

## for our data, these all indicate "SWAP":
montreal.df[which(montreal.df$duration < 0),c("start.s","end.s")] <- montreal.df[which(montreal.df$duration < 0),c("end.s","start.s")]
montreal.df[which(montreal.df$duration < 0),"duration"] <- -montreal.df[which(montreal.df$duration < 0),"duration"]
## delete alternative: montreal.df <- subset(montreal.df, duration > 0)
## another alternative: insert a mean-duration event centered between the start and end?

hour <- 60*60
window <- 0.5 # half hour
hist.data <- hist(subset(montreal.df,!is.na(end.s))$end.s/hour, breaks=seq(min(montreal.df$end.s,na.rm=T), max(montreal.df$end.s,na.rm=T)+window*hour, by=window*hour)/hour, plot=F )

plot(hist.data$mids, hist.data$counts/max(hist.data$counts),type="h")

loc.creation <- aggregate(montreal.df$start.s,
    by=list(loc.id = montreal.df$loc.id), min, na.rm=T)
loc.creation <- loc.creation[with(loc.creation,order(x,loc.id)),]
loc.creation$acc <- cumsum(rep.int(1,dim(loc.creation)[1]))
lines(loc.creation$x/hour, loc.creation$acc/max(loc.creation$acc), col="green")

loc.destruction <- aggregate(montreal.df$end.s,
    by=list(loc.id = montreal.df$loc.id), max, na.rm=T)
loc.destruction <- loc.destruction[with(loc.destruction,order(x,loc.id)),]
loc.destruction$acc <- cumsum(rep.int(1,dim(loc.destruction)[1]))
lines(loc.destruction$x/hour, loc.destruction$acc/max(loc.destruction$acc), col="red")

## merge creation and destruction, interpolate for missing times

loc.cutoff <- subset(aggregate(rep.int(1,dim(montreal.df)[1]), by=list(loc.id = montreal.df$loc.id, end.s = montreal.df$end.s), sum), x > 1)
loc.cutoff <- loc.cutoff[with(loc.cutoff,order(x,loc.id)),]
#most.cutoff <- unique(subset(loc.cutoff, x > 10, select="loc.id"))
#more.cutoff <- subset(montreal.df, loc.id %in% most.cutoff$loc.id)

assessor <- with(montreal.df,{
  function(row) any(user.id == row["user.id"] & loc.id != row["loc.id"])
})
largeAssess<-apply(
  unique(subset(montreal.df, duration > (60*60*12),select=c("user.id","loc.id"))),
  1,
  assessor)

nas.df <- subset(montreal.df, is.na(end.s) )
not.na <- !is.na(montreal.df$end.s)
nas.df$resolution <- apply(nas.df,1,function(row) {
  any(not.na & 
        montreal.df$user.id == row["user.id"] &
        montreal.df$loc.id == row["loc.id"] &
        montreal.df$start.s <= row["start.s"] &
        montreal.df$end.s > row["start.s"])
})

## alt:
nas.df$resolution <- apply(nas.df,1,function(row) {
  hold <- subset(montreal.df, user.id == row["user.id"] & loc.id == row["loc.id"])
  overlap <- subset(hold, start.s <= row["start.s"] & end.s > row["start.s"])
  ifelse(dim(overlap)[1] != 0, max(overlap$end.s), mean(hold) )
  dim(subset(montreal.df, user.id == row["user.id"] ))
  any(not.na & 
        montreal.df$user.id == row["user.id"] &
        montreal.df$loc.id == row["loc.id"] &
        montreal.df$start.s <= row["start.s"] &
        montreal.df$end.s > row["start.s"])
})
## TODO - missing start.s / end.s?
sorted.df<-montreal.df[with(montreal.df,order(user.id,start.s,end.s,loc.id)),]
sorted.df$duration <- sorted.df$end.s - sorted.df$start.s

hist.data <- hist(log10(subset(sorted.df, !is.na(duration))$duration/60),plot=F)

nas.df <- subset(sorted.df, is.na(end.s) )
appearances<-aggregate(rep.int(1,dim(sorted.df)[1]),by=list(user.id=sorted.df$user.id, loc.id=sorted.df$loc.id),"sum")
names(appearances)[3]<-"count"
sorted.appearances<-appearances[with(appearances,order(user.id, count, loc.id)),]