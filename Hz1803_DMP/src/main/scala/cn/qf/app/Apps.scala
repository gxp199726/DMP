package cn.qf.app


import cn.qf.utils.RptUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext}

object Apps {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      sys.exit()
    }
    val Array(inputPath, outputPath,dirPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    //处理字典文件
    val dirMap = sc.textFile(dirPath).map(_.split("\t",-1))
      .filter(_.length >= 5).map(arr=>(arr(4),arr(1))).collect().toMap
    //广播出去
    val cast = sc.broadcast(dirMap)
    //读取数据源
    val df = sqlContext.read.parquet(inputPath)
    df.map(row=>{
      //处理APP名称
      var name = row.getAs[String]("appname")
      if (StringUtils.isNotBlank(name)) {
          name = cast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
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
      (name,reqlist ++ adlist ++ adCountlist)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+" , "+t._2.mkString(","))
      //数据存入hdfs
     .saveAsTextFile(outputPath)
    sc.stop()
  }
}