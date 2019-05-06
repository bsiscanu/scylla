import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object Streaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Streaming")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "logs")
      .load()

    val query = stream.select(col("value").cast("string"))
      .filter(col("value").contains("WARN"))
      .filter(col("value").contains("MacAddressUtil"))
      .groupBy(col("value"))
      .count()

    query.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()

  }
}
