package cn.qf.app

import cn.qf.utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object LocationV2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      sys.exit()
    }
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    //读取数据源
    val df = sqlContext.read.parquet(inputPath)
    df.map(row=>{
      //先获取原始请求 有效请求 广告请求
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      //参与竞价数 成功数 展示数 点击数
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val ad = row.getAs[Double]("adpayment")
      //编写业务方法，进行调用 原始请求 有效请求 广告请求
      val reqlist = RptUtils.req(iseffective,processnode)
      //参与竞价数 成功数 展示数 点击数
      val adlist = RptUtils.addap(iseffective,isbilling,isbid,iswin,adorderid,winprice,ad)
      //点击数 展示数
      val adCountlist = RptUtils.Counts(iseffective,processnode)
      //取值地域维度
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),
        reqlist ++ adlist ++ adCountlist)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1._1+" , "+t._1._2+" , "+t._2.mkString(","))
      .saveAsTextFile(outputPath)
    sc.stop()
  }
}