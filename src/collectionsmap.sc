val nums = Array(41,13,21,9,3,4,5,7,1,9,55,33,22,11,9)
//methods
//map is a method ,, apply a logic/functionality on top of each and every element ... use map
//input and output number of elements must same
val ana = (x:Int) =>x*x
nums.map(ana)

//apply a function on top of each and every element based on bool vaues
val bool = (x:Int) =>x<10
bool(9)
bool(19)
nums.filter(bool)
 nums.filter(x=>x<10)

//array mutable change alue, array starts from 0
//collection of same datatype elements
//if u processing multi datatype elements use tuple
val det = ("venu",66,'m')
det._1
//tuple start from 1 its immutable

val arr = 1 to 20 toArray

arr(0)
arr(3)