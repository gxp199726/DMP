package cn.qf.label

import cn.qf.utils.LabelUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 上下文标签
  */
object LabelContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      sys.exit()
    }
    val Array(inputPath, outputPath,dirPath,stopWord) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    //读取字典文件
    val dirMap = sc.textFile(dirPath).map(_.split("\t",-1))
      .filter(_.length >= 5).map(arr=>(arr(4),arr(1))).collect().toMap
    //广播出去
    val cast = sc.broadcast(dirMap)
    //停用词库
    val stopwords = sc.textFile(stopWord).map((_,0)).collect().toMap
    //广播出去
    val stopWordCast = sc.broadcast(stopwords)
    //读取数据源
    val df = sqlContext.read.parquet(inputPath)
    //过滤数据
    val df2 = df.filter(LabelUtils.OneUserId)
    df2.map(row=>{
      //获取用户的id
      val userId = LabelUtils.getAnyOneUserId(row)
      //广告位和渠道标签
      val adLabels = LabelAdvertising.MakeLabel(row)
      //APP标签
      val AppLabel = LabelAPP.MakeLabel(row,cast)
      //设备标签
      val EqLabel = LabelEquipment.MakeLabel(row)
      //关键字标签
      val KWLabel = LabelWord.MakeLabel(row,stopWordCast)
      //地域标签
      val DYLabel = LabelDY.MakeLabel(row)
      (userId,adLabels++AppLabel++EqLabel++KWLabel++DYLabel)
    }).reduceByKey((list1,list2)=>
      (list1:::list2)
      .groupBy(_._1).mapValues(_.foldLeft[Int](0)(_+_._2)).toList
    ).map(t=>{
      t._1 + "," + t._2.map(t=>t._1+","+t._2).mkString(",")
    }).saveAsTextFile(outputPath)
  }
}