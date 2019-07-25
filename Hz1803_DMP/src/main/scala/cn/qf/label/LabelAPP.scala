package cn.qf.label

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * APP标签
  */
object LabelAPP extends Labels {
  override def MakeLabel(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //将参数转换类型
    val row = args(0).asInstanceOf[Row]
    val appdir = args(1).asInstanceOf[Broadcast[Map[String,String]]]
    //获取appname和appid
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if (StringUtils.isNotBlank(appname)) {
      list:+=("APP"+appname,1)
    }else if(StringUtils.isNotBlank(appid)) {
      list:+=("APP"+appdir.value.getOrElse(appid,appid),1)
    }
    list
  }
}
