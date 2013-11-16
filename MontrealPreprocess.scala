import scala.annotation.tailrec
import scala.io.Source
import scala.util.parsing.combinator.RegexParsers
import scala.collection.immutable.Stream.Empty

import shared._

object MontrealPreprocess {

  def main(args:Array[String]) : Unit = {
    val fname = "/Users/carlpearson/Dropbox/epidemics4share/testdt.o" // args(0)
    // TODO receive a margin-of-error separate for overlaps
    val start = System.currentTimeMillis()
    val oname = "/Users/carlpearson/Dropbox/epidemics4share/merged.o" // args(2)
    val fw = new java.io.PrintWriter(oname)
    implicit val recorder = (obs:Observation) => {
      fw.println(obs.user+" "+obs.loc+" "+obs.start+" "+obs.end)
      obs
    }
    parse(parseFile(fname).toStream, recorder)
    fw.flush
  }
  
  @tailrec
  final def extract(head:Observation, tail:Stream[Observation]) : Observation = {
    val (left, right) = tail.span(head.overlapping)
    left.filter(head.inGroup).filter({ case Observation(_, _, _, end) => head.end < end }) match {
      case Empty => head
      case xs => extract(head.upEnd(xs.map({ _.end }).max), right)
    }
  }
  
  @tailrec
  final def parse(remaining:Stream[Observation], recorder: Observation=>Observation) : Unit
  	= remaining match {
	    case Empty => Nil
	    case x #:: Empty => recorder(x)
	    case x #:: rest => {
	      parse(purge(recorder(extract(x, rest)), rest), recorder)
	    }
  }
  
  final def purge(head:Observation, downStream:Stream[Observation]) : Stream[Observation] = {
    val (purge, rest) = downStream.span({ case Observation(_,_,start,_) => start < head.end })
    val (_, keep) = purge.partition(head.contained)
    keep ++ rest
  }
  
}