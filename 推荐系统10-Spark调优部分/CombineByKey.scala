package util

import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object CombineByKey {
  def main(args: Array[String]): Unit = {
    val session = SparkSessionBase.createSparkSession()
    val sc = session.sparkContext

    val rdd1 = sc.parallelize(
      Array(Tuple2("A",1),("B",2),("B",3),("B",4),("B",5),("C",1),Tuple2("A",2)) , 2)
    /**
      * def combineByKey[C](
      * 		createCombiner: Int => C,
							为每一个分区中每一个分组进行初始化，如何初始化的？
								将createCobiner函数作用在这个分组的第一个元素上
      *  	mergeValue: (C, Int) => C,
      *  			按照mergeValue的聚合逻辑对每一个分区中每一个分组进行聚合
      *   	mergeCombiners: (C, C) => C): RDD[(String, C)]
      *   			reduce端的大聚合
      */

    /**
      * map
      * flatMap
      * mapPartition vs map：
      * 		map算子是一条一条的遍历处理
      * 		mapPartition 是一个分区一个分区的遍历，然后处理
      * 				partition的数据加载到内存：利：遍历的效率提高了   弊：对内存占用过大，消耗过多的资源
      * 有一个场景建议(必须)使用mapPartition
      * 		将RDD的数据写入到关系型数据oracle mysql
      * rdd.map(x=>{
      * 		//创建数据库连接  rdd中有多少条元素就会创建多少个数据库连接...
      * 		//拼接SQL语句
      * 		//插入数据
      * })
      *
      * rdd.mapPartition(x=>{
      * 		//创建数据库连接数=partition数
      * 		while(x.hasNext){
      * 			//拼接SQL语句
      * 			x.next()
      * 		}
      * 		//批量插入数据
      * })
      *
      *
      */
    /*  rdd1.mapPartitions(x=>{
        println("===========================")
        while(x.hasNext){
          println(x.next())
        }
        println("===========================")
        x
      }).count()*/
    /**
      * mapPartitionsWithIndex vs mapPartitions
      * 相同点：都是一个partition一个partition的遍历
      * 不同点：mapPartitionsWithIndex 能够拿到每一个分区ID号
      */
    rdd1.mapPartitionsWithIndex((index,iterator)=>{
      println("partitionId:" + index)
      while(iterator.hasNext){
        val t = iterator.next()
        println(t)
      }
      iterator
    }).count()


    rdd1.combineByKey(x=>x+"_", (x:String,y:Int)=>x+"@"+y, (x:String,y:String)=>x+"$"+y).foreach(println)
    rdd1.repa

    /**
      * 如何统计相同的key的value的聚合结果
      * combineByKey模拟reduceByKey的效果
      */
    rdd1.reduceByKey((x:Int,y:Int)=>x+y)
    rdd1.combineByKey(x=>x, (x:Int,y:Int)=>x+y, (x:Int,y:Int)=>x+y).foreach(println)

    /**
      * 使用combineByKey 模拟一个groupByKey
      */
    val combineByKeyRDD =  rdd1.combineByKey(x=>ListBuffer(x), (list:ListBuffer[Int],y:Int)=>list += y, (listx:ListBuffer[Int],listy:ListBuffer[Int])=>listx ++= listy)
    /**
      * 使用combineByKey 模拟一个reduceByKey(_+_)
      */
    combineByKeyRDD.foreach(println)

  }
}
