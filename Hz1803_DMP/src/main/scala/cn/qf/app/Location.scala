package cn.qf.app

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

object Location {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    //读取数据源
    val df = sqlContext.read.parquet(inputPath)
    df.registerTempTable("log")
    val result1 = sqlContext.sql(
      """
        |select provincename,cityname,
        |sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) yxrequest,
        |sum(case when requestmode = 1 and processnode =3 then 1 else 0 end) adrequest,
        |sum(case when iseffective = 1 and isbilling =1 and isbid = 1 then 1 else 0 end) cybid,
        |sum(case when iseffective = 1 and isbilling =1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccees,
        |sum(case when requestmode = 2 and iseffective =1 then 1 else 0 end) shows,
        |sum(case when requestmode = 3 and iseffective =1 then 1 else 0 end) clicks,
        |sum(case when iseffective = 1 and isbilling =1 and iswin = 1 then winprice/1000 else 0 end) dspcost,
        |sum(case when iseffective = 1 and isbilling =1 and iswin = 1 then adpayment/1000 else 0 end) dsppay
        |from log group by provincename,cityname
      """.stripMargin)
    val load = ConfigFactory.load()
    val pro = new Properties()
    pro.setProperty("user",load.getString("jdbc.user"))
    pro.setProperty("password",load.getString("jdbc.password"))
    result1.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tbn"),pro)
    sc.stop()
  }
}