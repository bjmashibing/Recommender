package com.msb.data

import java.io.{FileOutputStream, PrintWriter}
import java.lang.Math._


class DataGenerator {

}


//比例确定如果有10000条记录，有一千个用户，100个app
//默认应该是十万条记录  一万个用户   一千条记录  相应的作者应该是两百个
object DataGenerator{
  def main(args: Array[String]) {
    // 从业务表里面抽取出来的描述应用的基本特征
    makelist(10000,"d:/soft_common/recommonder") // 应用词表,
    makeappdown(10000,"d:/soft_common/recommonder") // 用户历史下载,主题,是一个应用,游戏中心,是一个应用,应用中心,是一个应用
    makesample(10000,"d:/soft_common/recommonder")  // 模拟正负例样本  = 浏览记录+lable
  }
  /**
    模拟app的商品词表
    hitop_id    STRING,  应用软件ID
    name        STRING,  名称
    author      STRING,   作者
    sversion    STRING,   版本号
    ischarge    SMALLINT,  收费软件
    designer    STRING,    设计者
    font        STRING,   字体
    icon_count  INT,      有几张配图
    stars       DOUBLE,   评价星级
    price       INT,      价格
    file_size   INT,      大小
    comment_num INT,      评论数据
    screen      STRING,   分辨率
    dlnum       INT       下载数量
    */
  def makelist(num:Int,path:String): Unit ={
    val pw = new PrintWriter(new FileOutputStream(path+"/applist.txt",true))
    var str = new StringBuilder()
    for(i<-1 to num){
      str.clear()
      str.append("hitop_id").append((random()*num/100).toInt).append("\t")
      str.append("name").append((random()*num/100).toInt).append("\t")
      str.append("author").append((random()*num/500).toInt).append("\t")
      str.append("sversion").append((10*random()).toInt).append("\t")
      str.append((2*random()).toInt).append("\t")
      str.append("designer").append((random()*num/500).toInt).append("\t")
      str.append("font").append((20*random()).toInt).append("\t")
      str.append((10*random()).toInt).append("\t")
      str.append(f"${10*random()}%1.2f").append("\t")
      str.append((random()*num).toInt).append("\t")
      str.append((random()*num).toInt).append("\t")
      str.append((random()*num/50).toInt).append("\t")
      str.append("screen").append((random()*num/20).toInt).append("\t")
      str.append((random()*num/50).toInt)
      pw.write(str.toString());
      pw.println()
    }
    pw.flush()
    pw.close()
  }

  def makeapplist(num: Int) :String = {
    var str = new StringBuilder()
    for(i<-1 to (10*random()).toInt+1){
      str.append("hitop_id").append((random()*num/100).toInt).append(",")
    }
    str.deleteCharAt(str.length-1)
    return str.toString()
  }

  /**
    *模拟用户下载历史表   这里没有用户这个概念   手机设备ID就是userId
    * device_id           STRING,   手机设备ID
      devid_applist       STRING,     下载过软件列表
      device_name         STRING,     设备名称
      pay_ability         STRING      支付能力
    */
  def makeappdown(num:Int,path:String): Unit ={
    val pw = new PrintWriter(new FileOutputStream(path+"/userdownload.txt",true))
    var str = new StringBuilder()
    for(i<-1 to num){
      str.clear()
      str.append("device_id").append((random()*num/10).toInt).append("\t")
      str.append(makeapplist(num)).append("\t")
      str.append("device_name").append((random()*num/10).toInt).append("\t")
      str.append("pay_ability").append((4*random()).toInt)
      pw.write(str.toString());
      pw.println()
    }
    pw.flush()
    pw.close()
  }
  /**
    *
    * 行为数据中，包含了当前浏览的app的基本特征
    * 正负例样本表 = 浏览记录+标签
    label       STRING,        Y列，-1或1代表正负例    label值实际上是批处理得出来的，用户浏览了并在一段时间内下载为正例
    device_id   STRING,        设备ID
    hitop_id    STRING,        应用ID
    screen      STRING,        手机软件需要的分辨率
    en_name     STRING,        英文名
    ch_name     STRING,        中文名
    author      STRING,        作者
    sversion    STRING,        版本
    mnc         STRING,				 Mobile Network Code，移动网络号码
    event_local_time STRING,   浏览的时间
    interface   STRING,
    designer    STRING,
    is_safe     INT,
    icon_count  INT,
    update_time STRING,
    stars       DOUBLE,
    comment_num INT,
    font        STRING,
    price       INT,
    file_size   INT,
    ischarge    SMALLINT,
    dlnum       INT
    */
  def makesample(num:Int,path:String): Unit ={
    val pw = new PrintWriter(new FileOutputStream(path+"/sample.txt",true))
    var str = new StringBuilder()
    for(i<-1 to num){
      str.clear()
      str.append(2*(2*random()).toInt-1).append("\t")
      str.append("device_id").append((random()*num/10).toInt).append("\t")
      str.append("hitop_id").append((random()*num/100).toInt).append("\t")
      str.append("screen").append((random()*20).toInt).append("\t")
      str.append("en_name").append((random()*num/100).toInt).append("\t")
      str.append("ch_name").append((random()*num/100).toInt).append("\t")
      str.append("author").append((random()*num/500).toInt).append("\t")
      str.append("sversion").append((10*random()).toInt).append("\t")
      str.append("mnc").append((random()*num/10).toInt).append("\t")
      str.append("event_local_time").append((random()*num).toInt).append("\t")
      str.append("interface").append((random()*num).toInt).append("\t")
      str.append("designer").append((random()*num/500).toInt).append("\t")
      str.append((2*random()).toInt).append("\t")
      str.append((10*random()).toInt).append("\t")
      str.append("update_date").append((random()*num).toInt).append("\t")
      str.append(f"${10*random()}%1.2f").append("\t")
      str.append((random()*num/50).toInt).append("\t")
      str.append("font").append((20*random()).toInt).append("\t")
      str.append((random()*num).toInt).append("\t")
      str.append((random()*num).toInt).append("\t")
      str.append((2*random()).toInt).append("\t")
      str.append((random()*num/50).toInt)
      pw.write(str.toString());
      pw.println()
    }
    pw.flush()
    pw.close()
  }

}