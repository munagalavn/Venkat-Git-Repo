import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveIntervalDayTimeObjectInspector

val name = "vani"
val day = "sun"
//if(name.length>20)
val ifel = {
  if(day=="sun" || day=="sunday" || day=="sat" || day=="saturday")  "20% off"
  else "no offer"
}
//match
val mat = name match {
  case "venu"=> s"Hi $name pls complete testing task today"
  case "venkat"=> s"Hi $name pls complete reporting tasks"
  case "harsha"=> s"hi $name pls take care of admin activities"
  case _ => "you are not member in this project"
}
def emp(name:String) = name match {
  case "venu"=> s"Hi $name pls complete testing task today"
  case "venkat"=> s"Hi $name pls complete reporting tasks"
  case "harsha"=> s"hi $name pls take care of admin activities"
  case _ => "you are not member in this project"
}
emp("harsha")
emp("venu")
// => means do something action
val days = day match {
  case "sun" | "sunday" =>1
  case "mon" | "monday"=>2
  case "tue"| "tuesday"=>3
  case "wed" | "wednesday"=>4
  case "thu" | "thursday"=>5
  case "fri" | "friday"=> 5
  case "sat" | "saturday"=>6
  case _ => 0
}

//usecese 3 safeguard
//x is safe guard ... means
//based on buz req ur combine match & for loop & i else

val age = 33
val ageoffers = age match {
  case x if(x>=1 && x<=13 => "j&j products 50% off"
  case x if(x>13 && x<25) => "remi mobile 20% off"
  case a if(a>=25 && a<60) => "free credit card"
  case _=> "no offers"
}

