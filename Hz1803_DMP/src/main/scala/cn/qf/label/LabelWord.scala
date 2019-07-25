package cn.qf.label

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 关键字标签
  */
object LabelWord extends Labels {
  override def MakeLabel(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[Map[String,String]]]
    //取到切分后的数据
    val kws = row.getAs[String]("keywords").split("\\|")
    kws.filter(word=>word.length>=3 && word.length<8 && !stopword.value.contains(word))
      .foreach(word=>list:+=("K"+word,1))
    list
  }
}
