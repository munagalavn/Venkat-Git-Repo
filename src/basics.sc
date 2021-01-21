val x = 9
x*x
val name = "Venkat"
val age  = -88
//$ means refer something
//s means is give hint to scala comp as its string interpolation
// ${age+5} means age do something processing
val si = s"hi $name after 5 years your age ${age+5} now age $age "
val msg = s"""hi i am $name from blr
             looking for spark opportunity my age $age"""
//expressions
//if else
val ageoff = if(age>18) s"Hi $name your age $age you are major"
else s" your age $age so you are minor"

// anything within { braces called } block. its fundamental element

// its collection of multiple lines
val nifel = {
  if (age > 0 && age < 18) "you are minor"
  else if(age>=18 && age<=59) "ur major"
  else if (age>=60) "you are old aged"
  else "pls enter positive values only"
}
def ageoffer(age:Int) = {
  if (age > 0 && age < 18) "you are minor"
  else if(age>=18 && age<=59) "ur major"
  else if (age>=60) "you are old aged"
  else "pls enter positive values only"
}
ageoffer(88)
ageoffer(11)
ageoffer(-22)
//hide logic, & reusable & no side effects
