origin <- as.POSIXct(strptime('2004-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))
## time zero in the data.  first actual data point is later in 2004
debug <- F; plotting <- F
## flag for the interactive portions of the script

#con <- gzfile("~/Dropbox/montreal.tgz") # since close irrelevant, don't need to hold on to con?
montreal.df<-read.csv(gzfile("~/Dropbox/montreal.tgz"),header=F,skip=1, sep=",", col.names=c("user.id","loc.id","start.s","end.s"))
# close(con) # apparently unnecessary for gzfile?

# calc connection durations
montreal.df$duration <- montreal.df$end.s - montreal.df$start.s

## addressing duration < 0
## resolution based on output of below logic:
## given the small number of these,
## and the unclear provenance, 
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

if (plotting) {
  hour <- 60*60
  window <- 0.5 # half hour
  xscale<-1/10000
  hist.data <- hist(subset(montreal.df,!is.na(end.s))$end.s/hour, breaks=seq(min(montreal.df$end.s,na.rm=T), max(montreal.df$end.s,na.rm=T)+window*hour, by=window*hour)/hour, plot=F )
  hist.starts <- hist(montreal.df$start.s/hour, breaks=seq(min(montreal.df$start.s,na.rm=T), max(montreal.df$start.s,na.rm=T)+window*hour, by=window*hour)/hour, plot=F )
  
  maxMax <- max(montreal.df$end.s,na.rm=T)
  optMax <- function(x) ifelse(any(!is.na(x)),max(x,na.rm=T),maxMax)
  
  minMin <- min(montreal.df$start.s,na.rm=T)
  optMin <- function(x) ifelse(any(!is.na(x)),min(x,na.rm=T),minMin)
  
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
  
  createDestroyNet.lines <- function(df, on, lty, xscale) {
    create <- aggregator(df, "start.s", on, optMin)
    destroy <- aggregator(df, "end.s", on, optMax)
    net <- merger(create, destroy, on)
    mapply(function(data, col) {
      lines(data$time/hour*xscale, data$acc/max(data$acc), col=col, lty=lty)
      data ## this makes mapply return list(1 = create, 2 = destroy, 3 = net)
    }, list(create, destroy, net), list("green","red","blue"))
  }
  
  png("db_overview.png",units=cm,width=10,height=5)
  plot(hist.starts$mids*xscale, hist.starts$counts/max(hist.starts$counts),
       type="h", bty="n", xlab="10k hours", ylab="%peak", yaxt="n", xaxt="n", col="grey")
  lines(hist.data$mids*xscale, hist.data$counts/max(hist.data$counts),
       type="h")
  
  axis(2, at=c(0,0.5,1))
  meh <- pretty(c(min(hist.data$mids),max(hist.data$mids)))*xscale
  meh[1] <- mean(meh[1:2])
  len <- length(meh)
  meh[len] <- mean(meh[(len-1):len])
  axis(1, at=signif(meh,2)) ## weird - no 1.0, 6.0?
  
  dump.loc<-createDestroyNet.lines(montreal.df, "loc.id", lty=1, xscale=xscale)
  dump.user<-createDestroyNet.lines(montreal.df, "user.id", lty=3, xscale=xscale)
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
  hold <- unique(subset(df, b(start.s) | b(end.s), select=c("user.id","loc.id")))
  ## get the relevant user.id <-> loc.id combos
  hold <- subset(hold, table(hold$loc.id)[paste(hold$loc.id)] > 1)
  ## exclude the locations with only one visitor
  
  if(dim(hold)[1] == 0) {
    matrix(nrow=0,ncol=2)
  } else {
    hold <- aggregate(hold$user.id, by=list(loc.id=hold$loc.id), combn, m=2)$x
    if ( !is.null(dim(hold)) ) {
      matrix(hold, ncol=2, byrow=T)
    } else {
      t(unique(Reduce(cbind, hold), MARGIN=2))
    }
    
  }
}

twid <- 60*60*24 # one day is min break
first.midnight <- trunc(as.POSIXct(min(montreal.df$start.s,na.rm=T), origin=origin),"days")+0
last.midnight <- trunc(as.POSIXct(max(montreal.df$end.s,na.rm=T), origin=origin),"days")+twid
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
invisible(mapply(processer(writer("test.o")), starts, ends, MoreArgs=list(df=montreal.df)))

# loc.cutoff <- subset(aggregate(rep.int(1,dim(montreal.df)[1]), by=list(loc.id = montreal.df$loc.id, end.s = montreal.df$end.s), sum), x > 1)
# loc.cutoff <- loc.cutoff[with(loc.cutoff,order(x,loc.id)),]
# #most.cutoff <- unique(subset(loc.cutoff, x > 10, select="loc.id"))
# #more.cutoff <- subset(montreal.df, loc.id %in% most.cutoff$loc.id)
# 
# assessor <- with(montreal.df,{
#   function(row) any(user.id == row["user.id"] & loc.id != row["loc.id"])
# })
# largeAssess<-apply(
#   unique(subset(montreal.df, duration > (60*60*12),select=c("user.id","loc.id"))),
#   1,
#   assessor)
# 
# nas.df <- subset(montreal.df, is.na(end.s) )
# not.na <- !is.na(montreal.df$end.s)
# nas.df$resolution <- apply(nas.df,1,function(row) {
#   any(not.na & 
#         montreal.df$user.id == row["user.id"] &
#         montreal.df$loc.id == row["loc.id"] &
#         montreal.df$start.s <= row["start.s"] &
#         montreal.df$end.s > row["start.s"])
# })
# 
# ## alt:
# nas.df$resolution <- apply(nas.df,1,function(row) {
#   hold <- subset(montreal.df, user.id == row["user.id"] & loc.id == row["loc.id"])
#   overlap <- subset(hold, start.s <= row["start.s"] & end.s > row["start.s"])
#   ifelse(dim(overlap)[1] != 0, max(overlap$end.s), mean(hold) )
#   dim(subset(montreal.df, user.id == row["user.id"] ))
#   any(not.na & 
#         montreal.df$user.id == row["user.id"] &
#         montreal.df$loc.id == row["loc.id"] &
#         montreal.df$start.s <= row["start.s"] &
#         montreal.df$end.s > row["start.s"])
# })
# ## TODO - missing start.s / end.s?
# sorted.df<-montreal.df[with(montreal.df,order(user.id,start.s,end.s,loc.id)),]
# sorted.df$duration <- sorted.df$end.s - sorted.df$start.s
# 
# hist.data <- hist(log10(subset(sorted.df, !is.na(duration))$duration/60),plot=F)
# 
# nas.df <- subset(sorted.df, is.na(end.s) )
# appearances<-aggregate(rep.int(1,dim(sorted.df)[1]),by=list(user.id=sorted.df$user.id, loc.id=sorted.df$loc.id),"sum")
# names(appearances)[3]<-"count"
# sorted.appearances<-appearances[with(appearances,order(user.id, count, loc.id)),]