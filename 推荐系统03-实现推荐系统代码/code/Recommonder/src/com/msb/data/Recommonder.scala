package com.msb.data

import java.io.PrintWriter

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.mllib.classification.{ LogisticRegressionWithLBFGS, LogisticRegressionModel, LogisticRegressionWithSGD }
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkContext, SparkConf }

import scala.collection.Map

/**
  *
  */
class Recommonder {

}

object Recommonder {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("recom").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //加载数据，用\t分隔开
    val data: RDD[Array[String]] = sc.textFile("result").map(_.split("\t"))

    //得到第一列的值，也就是label
    /**
     * -1	Item.id,hitop_id74:1;Item.screen,screen1:1
     */
    val label: RDD[String] = data.map(_(0))
    //sample这个RDD中保存的是每一条记录的特征名
    val sample: RDD[Array[String]] = data.map(_(1)).map(x => {
      //这条记录的所有的特征名
      val arr: Array[String] = x.split(";").map(_.split(":")(0))
      arr  
    })
    //将所有元素压平，得到的是所有分特征，然后去重，最后索引化，也就是加上下标，最后转成map是为了后面查询用
    /**
      *groupByKey  reduceBykey
      *
      * （1，1）   （1，2）  （2，1）  （2，1）
      *
      * rdd.map((_,1))
      * （(1,1)，1）   （(1,2)，1）  （（2，1），1）   （（2，1），1）
      *
      * groupByKey
      *
      *
      * distinct = map+reduceByKey
      *
      * 特征名索引集合
      * zipWithIndex：给每条记录创建一个索引值
      *
      *
      * sample：保存的时每一条记录的特征名
      * dict  K：特征名  V：索引号[0 , dict.length-1]
      */
    val dict: Map[String, Long] = sample.flatMap(x =>x).distinct().zipWithIndex().collectAsMap()
    //得到稀疏向量
    val sam: RDD[SparseVector] = sample.map(sampleFeatures => {
      //index中保存的是，未来在构建训练集时，下面填1的索引号集合
      val index: Array[Int] = sampleFeatures.map(feature => {
        //get出来的元素程序认定可能为空，做一个类型匹配
        val rs: Long = dict.get(feature).get
        //非零元素下标，转int符合SparseVector的构造函数
        rs.toInt
      })
      //SparseVector创建一个向量
      new SparseVector(dict.size, index, Array.fill(index.length)(1.0))
    })
    //mllib中的逻辑回归只认1.0和0.0，这里进行一个匹配转换
    val la: RDD[LabeledPoint] = label.map(x => {
      x match {
        case "-1" => 0.0
        case "1"  => 1.0
      }
      //标签组合向量得到labelPoint
    }).zip(sam).map(x => new LabeledPoint(x._1, x._2))

//    val splited = la.randomSplit(Array(0.1, 0.9), 10)
//    la.sample(true, 0.002).saveAsTextFile("trainSet")
//    la.sample(true, 0.001).saveAsTextFile("testSet")
//    println("done")

     
    //逻辑回归训练，两个参数，迭代次数和步长，生产常用调整参数
     val lr = new LogisticRegressionWithSGD()
    // 设置W0截距
    lr.setIntercept(true)
//    // 设置正则化
//    lr.optimizer.setUpdater(new SquaredL2Updater)
//    // 看中W模型推广能力的权重
//    lr.optimizer.setRegParam(0.4)
    // 最大迭代次数
    lr.optimizer.setNumIterations(10)
    // 设置梯度下降的步长,学习率
    lr.optimizer.setStepSize(0.1)

    //权重
    val model: LogisticRegressionModel = lr.run(la)

    //模型结果权重
    val weights: Array[Double] = model.weights.toArray
    //将map反转，weights相应下标的权重对应map里面相应下标的特征名
    val map: Map[Long, String] = dict.map(_.swap)
    //    model.save()
    //模型保存
    //    LogisticRegressionModel.load()
    //输出
    val pw = new PrintWriter("./model");
    //遍历
    for(i<- 0 until weights.length){
      //通过map得到每个下标相应的特征名
      val featureName = map.get(i)match {
        case Some(x) => x
        case None => ""
      }
      //特征名对应相应的权重
      val str = featureName+"\t" + weights(i)
      pw.write(str)
      pw.println()
    }
    pw.flush()
    pw.close() 
  }
}