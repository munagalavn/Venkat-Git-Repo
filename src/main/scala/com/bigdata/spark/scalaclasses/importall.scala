package com.bigdata.spark.scalaclasses


object importall {

  /*    val offer = (state:String)=> state match {
    case "OH"=>"20% off"
    case "NJ" |"CA"=>"10% off"
    case "NY" | "MI"| "IL"=> "30% off"
    case _ => "no offer"
  }*/
  def offer (state:String)= state match {
    case "OH"=>"20% off"
    case "NJ" |"CA"=>"10% off"
    case "NY" | "MI"| "IL"=> "30% off"
    case _ => "no offer"
  }

}
