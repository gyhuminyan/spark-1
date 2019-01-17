package com.tr.spark

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.tr.spark.utils.HBaseUtil

import scala.util.Try
import scala.util.parsing.json.JSON

/**
  * package: com.cloudera.streaming
  * describe: Kerberos环境中Spark2Streaming应用实时读取Kafka数据，解析后存入HBase
  * 使用spark2-submit的方式提交作业
  * spark2-submit --class com.cloudera.streaming.Kafka2Spark2Hbase \
    --master yarn \
    --deploy-mode client \
    --executor-memory 2g \
    --executor-cores 2 \
    --driver-memory 2g \
    --num-executors 2 \
    --queue default  \
    --principal fayson@FAYSON.COM \
    --keytab /data/disk1/spark2streaming-kafka-hbase/conf/fayson.keytab \
    --files "/data/disk1/spark2streaming-kafka-hbase/conf/jaas.conf#jaas.conf" \
    --driver-java-options "-Djava.security.auth.login.config=/data/disk1/spark2streaming-kafka-hbase/conf/jaas.conf" \
    --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/data/disk1/spark2streaming-kafka-hbase/conf/jaas.conf" \
    spark2-demo-1.0-SNAPSHOT.jar
  * creat_user: Fayson
  * email: htechinfo@163.com
  * creat_date: 2018/6/25
  * creat_time: 下午10:40
  * 公众号：Hadoop实操
  */
object Kafka2Spark2Hbase {

  Logger.getLogger("com").setLevel(Level.ERROR) //设置日志级别

  var confPath: String = System.getProperty("user.dir") + File.separator + "conf"

  def main(args: Array[String]): Unit = {
    //加载配置文件
    val properties = new Properties()
    val file = new File(confPath + File.separator + "0289.properties")
    if(!file.exists()) {
      val in = Kafka2Spark2Hbase.getClass.getClassLoader.getResourceAsStream("0289.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(file))
    }

    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics")
    val principal = properties.getProperty("principal.account")
    val keytabFilePath = properties.getProperty("keytab.filepath")
    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)

    if(StringUtils.isEmpty(brokers)|| StringUtils.isEmpty(topics) || StringUtils.isEmpty(principal) || StringUtils.isEmpty(keytabFilePath)) {
      println("未配置Kafka和Kerberos信息")
      System.exit(0)
    }
    val topicsSet = topics.split(",").toSet

    val spark = SparkSession.builder().appName("Kafka2Spark2HBase-kerberos").config(new SparkConf()).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5)) //设置Spark时间窗口，每5s处理一次
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers
      , "auto.offset.reset" -> "latest"
      , "security.protocol" -> "SASL_PLAINTEXT"
      , "sasl.kerberos.service.name" -> "kafka"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> "testgroup"
    )

    val dStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    dStream.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val connection = HBaseUtil.getHBaseConn(confPath, principal, keytabFilePath) // 获取Hbase连接
        partitionRecords.foreach(line => {
          //将Kafka的每一条消息解析为JSON格式数据
          val jsonObj =  JSON.parseFull(line.value())
          println(line.value())
          val map:Map[String,Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
          val rowkey = map.get("id").get.asInstanceOf[String]
          val name = map.get("name").get.asInstanceOf[String]
          val sex = map.get("sex").get.asInstanceOf[String]
          val city = map.get("city").get.asInstanceOf[String]
          val occupation = map.get("occupation").get.asInstanceOf[String]
          val mobile_phone_num = map.get("mobile_phone_num").get.asInstanceOf[String]
          val fix_phone_num = map.get("fix_phone_num").get.asInstanceOf[String]
          val bank_name = map.get("bank_name").get.asInstanceOf[String]
          val address = map.get("address").get.asInstanceOf[String]
          val marriage = map.get("marriage").get.asInstanceOf[String]
          val child_num = map.get("child_num").get.asInstanceOf[String]

          val tableName = TableName.valueOf("user_info")
          val table = connection.getTable(tableName)
          val put = new Put(Bytes.toBytes(rowkey))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(sex))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("occupation"), Bytes.toBytes(occupation))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mobile_phone_num"), Bytes.toBytes(mobile_phone_num))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fix_phone_num"), Bytes.toBytes(fix_phone_num))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("bank_name"), Bytes.toBytes(bank_name))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes(address))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("marriage"), Bytes.toBytes(marriage))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("child_num"), Bytes.toBytes(child_num))

          Try(table.put(put)).getOrElse(table.close())//将数据写入HBase，若出错关闭table
          table.close()//分区数据写入HBase后关闭连接
        })
        connection.close()

      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}

