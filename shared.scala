import scala.util.parsing.combinator.RegexParsers
import scala.io.Source

object shared {  
  type Time = Long
  type UserID = Int
  type LocID = Int
  case class Observation(user:UserID, loc:LocID, start:Time, end:Time) {
    def contained: Observation => Boolean = (other:Observation) => inGroup(other) && (other match {
      case Observation(_,_,oStart,oEnd) => start <= oStart & oEnd <= end
    })
    def inGroup: Observation => Boolean = {
      case Observation(this.user,this.loc,_,_) => true
      case _ => false
    }
    // assert: only applied to list of elements where start <= other.start
    def overlapping: Observation => Boolean = {
      case Observation(_, _, start, _) => start <= end
    }
    def upEnd(newEnd:Time) = Observation(user, loc, start, newEnd)
  }
  
  object ObservationParser extends RegexParsers {
    def uid: Parser[UserID] = """\d+""".r ^^ { _.toInt }
    def lid: Parser[LocID] = """\d+""".r ^^ { _.toInt }
    def time: Parser[Time] = """\d+""".r ^^ { _.toLong }
    def obs: Parser[Observation] = 
      (uid~lid~time~time) ^^ { case u~l~s~e => Observation(u,l,s,e) }
    def apply(input:String) : Observation = 
      parseAll(obs, input) match {
	    case Success(obs,_) => obs
	    case _ => Observation(-1,-1,-1,-1)
	  }  
  }
  
  def parseFile(path:String) : Iterator[Observation] = 
    Source.fromFile(path).getLines.map( ObservationParser(_) )
  
  case class PairObs(userA:UserID, userB:UserID, start:Time, end: Time)
 
  def intersect(one:Observation, two:Observation) : PairObs = 
    PairObs(one.user, two.user, Math.max(one.start, two.start), Math.min(one.end, two.end))
  
}