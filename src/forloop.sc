val name = "venu"
val names = Array("venu","venkat","nani","satya","kumar","seenu")
// collections: Array, List, Tuple
//Array : collection of same datatype elements . It's mutable(change values)
//List: collection of same datatype elements, its immutable (not possible to change)
//Tuple: Collection of different datatypes. its immutable
//Seq: Lit+tuple
//for loop ,: to process collections

val nums = Array(1,4,3,5,8,7,9,17,19,13,18,-44,-99,76)

val res = for(x<-nums) print(x+",")
val res = for(x<-nums if(x>0)) yield x match {
  case x if(x%2==0)=> x*x
  case _ => x*x*x
}
val n1 = 1 to 20 toArray
  val n2  = 1 to 10 toArray

val res = for(x<-n1;y<-n2) println(s"$x*$y=${x*y}")
