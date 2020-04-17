package org.apache.spark.mllib.classification

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.BLAS.dot
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions

/**
  * Created by Administrator on 2015/12/9.
  */
class AUC {

}

object AUC {
  //用 LinearDataGenerator.scala类生成数据
  def train(trainPath: String, testPath: String, sc: SparkContext): Unit = {
    val trainData: RDD[LabeledPoint] = MLUtils.loadLabeledPoints(sc, trainPath)
    val testData: RDD[LabeledPoint] = MLUtils.loadLabeledPoints(sc, testPath)
    val model: LogisticRegressionModel = LogisticRegressionWithSGD.train(trainData, 30, 0.8, 1.0)
    val features = testData.map(_.features)
    val labels: RDD[Double] = testData.map(_.label)
    val M = labels.sum().toInt
    val N = labels.count() - M
    val predictLabels: RDD[Double] = model.predict(features)
    val result: RDD[(Double, Double)] = labels.zip(predictLabels)
    val acc = result.filter(x => {
      x._1.equals(x._2)
    }).count() / result.count().toDouble
    println("acc is ===================" + acc)
    val resultPre: RDD[(Double, Double)] = features.map(features => {
      val intercept = model.intercept
      val margin = dot(model.weights, features) + intercept
      val score = 1.0 / (1.0 + math.exp(-margin))
      score
    }).zip(labels)
    val orderpre = resultPre.sortBy(_._1, false)
    val totalIndex = orderpre.zipWithIndex().map(data => {
      var index = 0
      if (data._1._2.equals(1.0)) {
        index = data._2.toInt
      }
      index
    }).sum()
    val auc = (totalIndex - (M * (M + 1) / 2)) / (M * N)
    println("auc is ===================" + auc)
  }

  def main(args: Array[String]) {
    val (master, trainPath, testPath) = ("local[4]", "testSet","trainSet")
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("LRWithSGD")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    train(trainPath, testPath, sc)
  }
}
