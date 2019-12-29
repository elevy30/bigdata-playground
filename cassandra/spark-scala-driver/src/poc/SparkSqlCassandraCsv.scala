package poc

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by eyallevy on 10/01/17.
  */
object SparkSqlCassandraCsv {
 //Elapsed time: 542090856470ns
  //9 min
  val tableName = "masssample"
  val keyspace = "thetaray"
  val csvFile = "QR_500K.csv"

  def main(args: Array[String]): Unit = {
    println("Hello")

    val (sparkSession: SparkSession) = init
    val (df: DataFrame) = readFile(sparkSession)

    time(df)

    readFromCassandra(sparkSession)
    //    createTable()
    sparkSession.stop
  }

  private def init = {
    val scsql = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[4]")
      //.config("spark.some.config.option", "some-value")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      //with this two lines it reduce to Elapsed time: 409261168976ns 6.8 min
      .config("spark.cassandra.output.batch.size.bytes", "16384")
      .config("spark.cassandra.output.concurrent.writes", "10")
      .getOrCreate()

    (scsql)
  }

  private def createTable() = {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")

    CassandraConnector(conf).withSessionDo { session =>
            session.execute("DROP TABLE " + keyspace + "." + tableName + ";");
            session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";");
//            session.execute("CREATE KEYSPACE thetaray WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
//      session.execute("CREATE TABLE thetaray.masssample (sessionId INT PRIMARY KEY)");
    }
  }

  // Add spark connector specific methods to DataFrame
  def readFile(sparkSession: SparkSession) = {
    val df = sparkSession
      .read
      .option("header", true)
      .option("inferSchema", "true")
      .csv("/opt/data/QR_500K.csv")

//    df.show(10)
    val lines = df.count()
    println(lines)
    (df)

  }


  def time(df:DataFrame) = {
    val t0 = System.nanoTime()
    val result = writeToCassandra(df)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
  }

  def writeToCassandra(df: DataFrame) {
    val renamed = df.withColumnRenamed("sessionId", "sessionid")

    renamed.createCassandraTable(
      keyspace,
      tableName,
      partitionKeyColumns = Some(Seq("sessionid")))

    renamed.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> keyspace))
      .mode(SaveMode.Append)
      .save()
  }

  def readFromCassandra(sparkSession: SparkSession) {
    val dfFromcassandra = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> keyspace))
      .load()

    dfFromcassandra.show(10)
  }


}
