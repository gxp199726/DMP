package cn.qf.utils

import org.apache.spark.{SparkConf, SparkContext}

object AppRedisUtils {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      sys.exit()
    }
    val Array(inputPath, outputPath, dirPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val dirMap = sc.textFile(dirPath).map(_.split("\t",-1))
      .filter(_.length >= 5).map(arr=>(arr(4),arr(1)))
      .foreachPartition(ite=>{
        val jedis = Jedispool.getConnection()
        ite.foreach(t=>{
          jedis.set(t._1,t._2)
        })
        jedis.close()
      })
    sc.stop()
  }
}