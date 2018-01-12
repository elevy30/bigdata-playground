package poc

import com.datastax.spark.connector._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by eyallevy on 10/01/17.
  */
object CsvWriter {


  def main(args: Array[String]): Unit = {
    println("Hello")

    val (sparkSession: SparkSession) = init
    read(sparkSession)
    sparkSession.stop
  }

  private def init = {
    val scsql = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[1]")
      .config("spark.some.config.option", "some-value")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

    (scsql)
  }

  // Add spark connector specific methods to DataFrame
  def read(sparkSession: SparkSession) = {
    val df = sparkSession
      .read
      .option("header", true)
      .option("inferSchema", "true")
      .csv("sample.csv")

    df.show()

    val renamed = df.withColumnRenamed("id", "username")
    df.createCassandraTable(
      "thetaray",
      "sample",
      partitionKeyColumns = Some(Seq("id")))

    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "sample", "keyspace" -> "thetaray"))
      .mode(SaveMode.Append)
      .save()

    val dfFromcassandra = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "sample", "keyspace" -> "thetaray" ))
      .load()

    dfFromcassandra.show()
  }
}
