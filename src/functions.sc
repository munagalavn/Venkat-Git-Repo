def sumtwo(y:Int, x:Int) = x+y
sumtwo(3,4)
sumtwo(9,2)

def sqr(x:Int) = x*x
sqr(5)
def cube(x:Int) = x*x*x
cube(8)
// recursive functions
//a function call within same function
// a function call itself returning datatype must
def fact(x:Int):Int= x match {
  case x if(x>=1)=> x* fact(x-1)
  case _ => 1
}
fact(5)
1*2*3*4*5
// 5* fact(4)
//5*4*fact(3)
//5*4*3*fact(2)
//5*4*3*2*fact(1)
val num = 5*4*3*2*1*1
num.equals(120)
//nested functions:
//  a function call in another function called (not same function)
//no need return datatype, qny function last line returning value

def process(x:Int) = x match {
  case x if(x%2==0) => sqr(x)
  case x if(x%2!=0) => cube(x)
  case _ => 1
}
process(4)
process(5)

def biggest(x:Int, y:Int, z:Int) = {
  def big(x:Int, y:Int) = {if(x>y) x else y}
  big(x,big(y,z))
}
biggest(50,14,-90)
//big(50,14)

///anonymous function: a function without defin efunction
//its almost like lambda in python
val tmp = (x:Int)=> x*x
tmp(4)
tmp(5)
val nums = Array(4,5,9,1,20,41,11)
nums.map(tmp)
//higher order functions
//a function call as a parameter in another function


def test(x:Int, func: Int => Int) = {
  func(x)
}
test(5,sqr)
test(6,cube)
test(3,fact)
test(4,tmp)




