package com.dataengine.spark

object App {
  def main(args: Array[String]): Unit = {
    val groupCol = Seq("accessKey", "storeId", "orderType")

    val t1groupbyCol=groupCol.map("t1."+_).mkString(",")
    val t1leftjoinGroupbyCol=groupCol.map(x=>"t1."+x+"=t2."+x).mkString(" and ")
    println(groupCol)
    println(t1groupbyCol)
    println(t1leftjoinGroupbyCol)

  }

}
