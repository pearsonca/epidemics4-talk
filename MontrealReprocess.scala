import scala.annotation.tailrec
import scala.io.Source
import scala.util.parsing.combinator.RegexParsers
import scala.collection.immutable.Stream.Empty

import shared._

object MontrealReprocess {


  
  def parseFile(path:String) : Iterator[Observation] = for (
    	line <- Source.fromFile(path).getLines
    ) yield ObservationParser(line)
  
  def main(args:Array[String]) : Unit = {
    val fname = "/Users/carlpearson/Dropbox/epidemics4share/merged.o" // args(0)
    // TODO receive a margin-of-error separate for overlaps
    val oname = "/Users/carlpearson/Dropbox/epidemics4share/paired.o" // args(2)
    val fw = new java.io.PrintWriter(oname)
    implicit val recorder = (obs:List[PairObs]) => {
      obs foreach {
        p => fw.println(p.userA+" "+p.userB+" "+p.start+" "+p.end)
      }
      fw.flush
    }
    parse(parseFile(fname).toStream)
  }
  
  final def extract(head:Observation, tail:Stream[Observation]) : List[PairObs] = {
    tail.takeWhile(head.overlapping). // assert: no self.user + self.loc in here; if there were, they should have been consumed in preprocess
    	filter({ _.loc == head.loc }).map { intersect(_, head) }.toList
  }
  
  @tailrec
  final def parse(stream:Stream[Observation])(implicit recorder: List[PairObs]=>Unit )
  	: Unit = stream match {
	    case x #:: Empty => Unit
	    case x #:: rest => {
	      recorder(extract(x, rest))
	      parse(rest)
	    }
  }
  
}