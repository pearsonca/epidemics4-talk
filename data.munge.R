#setwd("~/Dropbox/epidemics4share/")
origin <- as.POSIXct(strptime('2004-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))

## time zero in the data.  first actual data point is later in 2004
debug <- F; plotting <- F
## flag for the interactive portions of the script

preProcess<-function() {
  require(data.table)
  #con <- gzfile("~/Dropbox/montreal.tgz") # since close irrelevant, don't need to hold on to con?
  montreal.df<-data.table(read.csv(gzfile("~/Dropbox/montreal.tgz"),header=F,skip=1, sep=",", col.names=c("user.id","loc.id","start.s","end.s")))
  # close(con) # apparently unnecessary for gzfile?
  setkey(montreal.df, start.s, end.s, user.id, loc.id)
  montreal.df[is.na(end.s), end.s := start.s]
  montreal.df[end.s < start.s, temp := end.s][ !is.na(temp) , end.s := start.s][ !is.na(temp), start.s := temp ]
  montreal.df$temp <- NULL
  # TODO use data.table for these
  consolidate.df <- aggregate(start.s ~ user.id + loc.id +  end.s, montreal.df, min) ## for identical ends, consolidate starts
  consolidate.df <- aggregate(end.s ~ user.id + loc.id + start.s, consolidate.df, max)
}
if (!exists("consolidate.df")) consolidate.df <- preProcess()

merged.df<-data.table(read.csv("~/Dropbox/epidemics4share/merged.o",header=F,skip=1, sep=" ", col.names=c("user.id","loc.id","start.s","end.s")))

store<-function(ref.rf) {
  write.table(ref.df, file="testdt.o", row.names=F, col.names=F)
}
# at this point, defer to languages more amenable to this sort of this work

if (!exists("pairs.df")) {
  require(data.table)
  pairs.df<-data.table(read.csv(gzfile("~/Dropbox/epidemics4share/paired.o"),header=F,skip=0, sep=" ", col.names=c("user.a","user.b","start.s","end.s")))
  pairs.df$duration <- pairs.df$end.s - pairs.df$start.s
}
# calc connection durations

## addressing duration < 0
## resolution based on output of below logic:
## given the small number of these,
## and the unclear provenance, 
## resolve by manual inspection where duration < 0
## for our data, these all indicate "SWAP":
## delete alternative: montreal.df <- subset(montreal.df, duration > 0)
## another alternative: insert a mean-duration event centered between the start and end?
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

  aggregator <- function(df, tarkey, aggkey, fun) {
    by <- list(); by[[aggkey]] <- df[[aggkey]]
    res <- aggregate(df[[tarkey]], by=by, fun)
    names(res)[2] <- "time"
    res <- res[order(res$time,res[[aggkey]]),]
    res$acc <- cumsum(rep.int(1,dim(res)[1]))
    res
  }
  
  merger<-function(left, right, on) {
    ## assert: left + right have "acc" key.
    ## first left entry has time < first right entry time
    res <- merge(left, right, by=c("time", on), all=T, suffixes=c(".left",".right"))
    joined <- paste("acc",c("left","right"),sep=".")
    res[is.na(res[[joined[1]]]),joined[1]] <- 0; res[is.na(res[[joined[2]]]),joined[2]] <- 0
    left.rle <- rle(res[[joined[1]]]); right.rle <- rle(res[[joined[2]]])
    left.rle$values[which(left.rle$values == 0)] <-
      left.rle$values[which(left.rle$values == 0)-1]
    right.rle$values[which(right.rle$values[-1] == 0)+1] <-
      right.rle$values[which(right.rle$values[-1] == 0)]
    res$acc.left<-inverse.rle(left.rle); res$acc.right<-inverse.rle(right.rle)
    res$acc <- res$acc.left - res$acc.right
    res
  }
  
  createDestroyNet.lines <- function(df, on, lty, xscale, lwd=1, col.list=list("green","red","blue")) {
    create <- aggregator(df, "start.s", on, min)
    destroy <- aggregator(df, "end.s", on, max)
    net <- merger(create, destroy, on)
    mapply(function(data, col) {
      lines(data$time/hour*xscale, data$acc/max(data$acc), col=col, lty=lty, lwd=lwd)
      data ## this makes mapply return list(1 = create, 2 = destroy, 3 = net)
    }, list(create, destroy, net), col.list)
  }
  
dataReview<-function(df) {
  hour <- 60*60
  window <- 0.5 # half hour
  xscale<-1/10000
  
  ylines <- (unclass(c(as.POSIXct("2005-01-01"), as.POSIXct("2006-01-01"), as.POSIXct("2007-01-01"), as.POSIXct("2008-01-01"), as.POSIXct("2009-01-01"), as.POSIXct("2010-01-01")))-unclass(origin))/60/60*xscale
  
  #hist.data <- hist(subset(df,!is.na(end.s))$end.s/hour, breaks=seq(min(df$end.s,na.rm=T), max(df$end.s,na.rm=T)+window*hour, by=window*hour)/hour, plot=F )
  hist.starts <- hist(df$start.s/hour, breaks=seq(min(df$start.s,na.rm=T), max(df$start.s,na.rm=T)+window*hour, by=window*hour)/hour, plot=F )
  
  par(mgp=c(1.5,0.25,0), mar=c(3,3,0,0)+0.1,tcl=0.5)
  plot(hist.starts$mids*xscale, hist.starts$counts/max(hist.starts$counts),
       type="h", bty="n", xlab="", lwd=0.5, ylab="%peak", yaxt="n", xaxt="n", col="grey",
       panel.first=abline(v=ylines, col="lightgrey", lty=3))
  #lines(hist.data$mids*xscale, hist.data$counts/max(hist.starts$counts), type="h", lwd=0.5)
  
  axis(2, at=c(0,0.5,1), col="lightgrey")
  axis(1, at=ylines, lwd=0, labels=c("2005","2006","2007","2008","2009","2010"), col="lightgrey") ## weird - no 1.0, 6.0?
  
  dump.loc<-createDestroyNet.lines(df, "loc.id", lty=1, lwd=1, xscale=xscale, col.list=list("darkgreen","darkred","darkblue"))
  dump.user<-createDestroyNet.lines(df, "user.id", lty=1, lwd=3, xscale=xscale)
  legend(0.5, 1.0, bty="n",
         legend=c("logins","acc. new loc.","acc. new users","acc. lost loc.","acc. lost users","net locations","net users"),
         lty=c(1,1,1,1,1,1,1),
         lwd=c(1,1,3,1,3,1,3),
         col=c("grey","darkgreen","green","darkred","red","darkblue","blue"))
  dump.user
}

if (plotting) {
  png(filename="dataReview.png",width=15, height=15, units="cm", res=300)
  users.list <- dataReview(merged.df)
  userCounts <- data.table(users.list[[3]]$time, users.list[[3]]$acc)
  setkey(userCounts,V1)
  net.users<-sapply(ends, function(e) userCounts[V1 < e, tail(V2,n=1)])
  net.users<-c(1,1,net.users)
  net.users<-net.users[-2025] ## deal with div 0 issues
  dev.off()
}

betweener<-function(start, end) {
  function(time) !is.na(time) & (start <= time) & (time <= end)
  # closed left, closed right:
  # for bin1 ends on t, bin2 starts on t, could lead to an edge appearing in both bins
}

# spanner<-function(start, end) {
#   Vectorize(function(s, e) s <= start & end <= e)
# }

edger<-function(start,end,df) { ## spanner may be irrelevant, if we are removing intervals w/ duration > bin width
  b<-betweener(start, end); # s<-spanner(start, end) # to user spanner: | s(start.s, end.s)
  slice <- subset(df, b(start.s) | b(end.s), select=c("user.a","user.b"))
  if (dim(slice)[1]!=0) {
    unique(slice)
  } else {
    matrix(nrow=0, ncol=2)
  }
}

twid <- 60*60*24 # one day is min break
first.midnight <- trunc(as.POSIXct(min(consolidate.df$start.s,na.rm=T), origin=origin),"days")+0
last.midnight <- trunc(as.POSIXct(max(consolidate.df$end.s,na.rm=T), origin=origin),"days")+twid
breaks<-seq(unclass(first.midnight), unclass(last.midnight), twid) - unclass(origin)

writer<-function(outputfh) {
  con<-file(outputfh,open="wt",)
  function(el) {
    if (dim(el)[1] != 0) write.table(el, file=con, row.names=F, col.names=F, append=T)
    write("BREAK", con, append=T)
  }
}

processer <- function(wr) {
  function(start, end, df) wr(edger(start, end, df))
}

starts <- breaks[-length(breaks)]
ends <- breaks[-1]
getStartEnds<-function(daySpan) {
  startDays <- seq(1, length(starts), by=daySpan)
  endDays <- startDays + daySpan
  endDays <- endDays - 1
  endDays[length(endDays)]<-length(starts)
  list(starts=starts[startDays], ends=ends[endDays], mids=(starts[startDays]+ends[endDays])/2)
}

#head(ends[endDays]) - head(starts[startDays])
#tail(ends[endDays]) - tail(starts[startDays])
processPairs<-function(daySpan) {
  fname <- paste("day",daySpan,"els.o", sep="")
  ref <- getStartEnds(daySpan)
  invisible(mapply(processer(writer(fname)), ref$starts, ref$ends, MoreArgs=list(df=pairs.df)))
}

daySlices <- array(c(1, 7, 30, 90, 180, 365)) # per day, per week, per month, per quarter, per half year, per year

uniqueUCount <- length(unique(merged.df$user.id))
if (plotting) {
  png(filename="maxComp.png",width=15, height=15, units="cm", res=300)
  par(mgp=c(1.5,0.25,0), mar=c(3,3,0,0)+0.1,tcl=0.5)
  xscale=1/10000
  ylines <- (unclass(c(as.POSIXct("2005-01-01"), as.POSIXct("2006-01-01"), as.POSIXct("2007-01-01"), as.POSIXct("2008-01-01"), as.POSIXct("2009-01-01"), as.POSIXct("2010-01-01")))-unclass(origin))/60/60*xscale
  plot(NA, ylim=c(0,550), xlim=c(0.5,5.5), bty="n", xlab="", ylab="peak component size / aggregate time", yaxt="n", xaxt="n", panel.first=abline(v=ylines, col="lightgrey", lty=3))
  cols <- gray.colors(length(daySlices), start=0.7, end=0.1)
  ref<-mapply(function(daySpan, col, lwd){
    fname <- paste("~/git/EpiFire/examples/day",daySpan,"comp.o", sep="")
    results.list <- lapply(strsplit(readLines(fname)," "), as.integer)
    maxes<-sapply(results.list,function(x) max(c(x,0)))
    se<-getStartEnds(daySpan)
    lines(se$mids/60/60*xscale, maxes/daySpan, col=col, lwd=lwd)
    list(maxes=maxes, days=daySpan)
  }, daySlices, cols, c(0.2, rep.int(1,length(daySlices)-1)) )
  axis(2, at=pretty(c(1,550)), col="lightgrey")
  axis(1, at=ylines, lwd=0, labels=c("2005","2006","2007","2008","2009","2010"), col="lightgrey")
  legend(0.5, 550, bty="n",
         legend=c("daily","weekly","monthly","quarterly","biannually","yearly"),
         col= cols, lty=1)
  dev.off()
  
  png(filename="maxEdges.png",width=15, height=15, units="cm", res=300)
  par(mgp=c(1.5,0.25,0), mar=c(3,3,0,0)+0.1,tcl=0.5)
  xscale=1/10000
  peak<-5000
  ylines <- (unclass(c(as.POSIXct("2005-01-01"), as.POSIXct("2006-01-01"), as.POSIXct("2007-01-01"), as.POSIXct("2008-01-01"), as.POSIXct("2009-01-01"), as.POSIXct("2010-01-01")))-unclass(origin))/60/60*xscale
  plot(NA, xlim=c(0.5,5.5), ylim=c(5,peak), 
       bty="n", xlab="", ylab="peak degrees", xaxt="n", 
       panel.first=abline(v=ylines, col="lightgrey", lty=3),
       fg="lightgrey", log="y"
       )
  cols <- gray.colors(length(daySlices), start=0.7, end=0.1)
  ref<-mapply(function(daySpan, col, lwd){
    fname <- paste("~/git/EpiFire/examples/day",daySpan,"edges.o", sep="")
    results.list <- sapply(lapply(strsplit(readLines(fname)," "), as.integer), length)
    se<-getStartEnds(daySpan)
    y <-  results.list #/ifelse(daySpan == 1,1,log(daySpan,1.3))
    lines(se$mids/60/60*xscale, y, col=col, lwd=lwd)
  }, daySlices, cols, c(0.2, rep.int(1,length(daySlices)-1)) )
  axis(1, at=ylines, lwd=0, labels=c("2005","2006","2007","2008","2009","2010"), col="lightgrey")
  legend(0.5, peak, bty="n",
         legend=c("daily","weekly","monthly","quarterly","biannually","yearly"),
         col= cols, lty=1)
  dev.off()
}
 ## weird - no 1.0, 6.0?


obsPerRun <- 2024
probs<-c(0.5,.99)
days<-0:2023
extract<-function(x) quantile(x,probs)
mapply(function(input, output){
  data<-read.csv(input, header=F,sep="",col.names=c("day","S","E","I","R"))
  runCount <- dim(data)[1]/obsPerRun
  ei<-apply(array(data$E+data$I,c(obsPerRun,1,runCount)), 1:2, extract)
  #i<-apply(array(data$I,c(obsPerRun,1,runCount)), 1:2, extract)
  #r<-apply(array(data$R,c(obsPerRun,1,runCount)), 1:2, extract)
  #dis<-e+i
  maxComparts<-max(ei[2,180:(2024-90),]/net.users[180:(2024-90)])
  png(output, width=15, height=15, units="cm", res=300)
  plot(NA,ylim=c(0, maxComparts), xlim=c(0+180,2023-90), xlab="years", ylab="% net users", xaxt="n")
  invisible(mapply(function(pop, col){
    lines(days, pop[1,,]/net.users, col=col)
    lines(days, pop[2,,]/net.users, col=col)
  }, list(ei), c("lightgrey")))
  ref<-array(data$E+data$I,c(obsPerRun,1,runCount))
  invisible(mapply(function(ran, col){ lines(days,ref[,1,sample(1000,1)]/net.users,col=col, lwd=3) }, sample(1000,5,replace=F), rainbow(5)))
  ats<-seq(0,2023,365)
  axis(1,at=ats,labels=seq(0,length(ats)-1))
  dev.off()
}, paste("~/Downloads/everything/",daySlices,".o",sep=""), paste("out",daySlices,".png",sep=""))