package cn.qf.label

/**
  * 标签的接口
  */
trait Labels {
  def MakeLabel(args:Any*):List[(String,Int)]
}
