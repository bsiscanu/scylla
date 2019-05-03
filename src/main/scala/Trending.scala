import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Trending {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Trending")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.read
      .format("text")
      .load("resources/trending.html")

    data.filter(col("value").contains("id=\"video-title\""))
      .select(
        regexp_extract(col("value"), "aria-label=\"(.*?)\"", 1).alias("titles"),
        regexp_extract(col("value"), "href=\"(.*?)\"", 1).alias("links")
      )
      .where("titles like '%Jardin%'")
      .show()

  }
}
