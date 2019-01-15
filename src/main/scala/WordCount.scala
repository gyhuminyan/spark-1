import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordCount")
      .getOrCreate()

    import spark.implicits._

    val wordRDD = spark
      .read
      .textFile("data/words.txt")
    val result = wordRDD
      .flatMap(_.split(" "))
      .map((_,1))
      .toDF("word","number")
      .groupBy("word")
      .count()
      .show()
  }
}
