package cn.qf.label

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 地域标签（省标签格式：ZPxxx->1, 地市标签格式: ZCxxx->1）xxx 为省或市名称
  */
object LabelDY extends Labels {
  override def MakeLabel(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val proname = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(proname)) {
      list:+=("ZP"+proname,1)
    }
    if (StringUtils.isNotBlank(cityname)){
      list:+=("ZC"+cityname,1)
    }
    list
  }
}
