import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object SparkExample {
  def main(args: Array[String]): Unit = {
    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("Coalesce Example")
      .master("local[*]")
      .getOrCreate()

    // Sample DataFrame
    import spark.implicits._
    val data = Seq(("John", 30), ("Jane", 25), ("Jake", 35), ("Jill", 28))
    val df: DataFrame = data.toDF("Name", "Age")

    // Write DataFrame to disk (without coalesce)
    df.write.mode("overwrite").csv("output-directory")

    spark.stop()
  }
}

