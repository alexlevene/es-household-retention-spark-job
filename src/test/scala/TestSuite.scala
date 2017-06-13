import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

class TestSuite extends FunSuite {
  test("Test description") {
    assert(true)
  }

  test("Creates SparkSession and loads CSV") {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    println("SUCCESSFULLY LOADED SPARK IN TEST")

    spark.close
  }
}
