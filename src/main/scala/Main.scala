import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object Main {
  def main(args:Array[String]):Unit = {
    println("Hello World!")

    // Create a SparkSession
    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    spark.close
    println("Done")
  }
}
