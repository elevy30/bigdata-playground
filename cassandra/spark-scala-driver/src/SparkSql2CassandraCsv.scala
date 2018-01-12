//import com.datastax.spark.connector.cql.CassandraConnector
//import com.github.jparkie.spark.cassandra.rdd._
//import com.github.jparkie.spark.cassandra.sql._
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
//
///**
//  * Created by eyallevy on 10/01/17.
//  */
//object SparkSql2CassandraCsv {
//  //Elapsed time: 542090856470ns
//  //9 min
//  val tableName = "masssample1"
//  val keyspace = "thetaray1"
//  val csvFile = "QR_500K.csv"
//
//  def main(args: Array[String]): Unit = {
//    println("Hello")
//
//    val (sqlContext: SQLContext) = initSQLContext
//    val (df: DataFrame) = readFile(sqlContext)
//    createTable()
//    writerTimeMeasure(df)
//
//    val (sparkSession: SparkSession) = initSparkSession
//    readFromCassandra(sparkSession)
//    sparkSession.stop
//  }
//
//  private def initSQLContext = {
//    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
//    val sc = new SparkContext("local[2]", "cassandra", conf)
//    val sqlContext = SQLContext.getOrCreate(sc)
//
//    (sqlContext)
//  }
//
//  private def initSparkSession = {
//    val sparkSession = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .master("local[4]")
//      //.config("spark.some.config.option", "some-value")
//      .config("spark.cassandra.connection.host", "127.0.0.1")
//      //with this two lines it reduce to Elapsed time: 409261168976ns 6.8 min
//      .config("spark.cassandra.output.batch.size.bytes", "16384")
//      .config("spark.cassandra.output.concurrent.writes", "10")
//      .getOrCreate()
//
//    (sparkSession)
//  }
//
//  private def createTable() = {
//    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
//
//    CassandraConnector(conf).withSessionDo { session =>
//      //session.execute("DROP TABLE " + keyspace + "." + tableName + ";");
//      session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";");
//      session.execute("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
//      session.execute("CREATE TABLE " + keyspace + "." + tableName + " (sessionid INT PRIMARY KEY)");
//    }
//  }
//
//  // Add spark connector specific methods to DataFrame
//  def readFile(sqlContext: SQLContext) = {
//    val df = sqlContext
//      .read
//      .option("header", true)
//      .option("inferSchema", "true")
//      .csv(csvFile)
//
//    //df.show(10)
//    val lines = df.count()
//    println(lines)
//    (df)
//
//  }
//
//
//  def writerTimeMeasure(df: DataFrame) = {
//    val t0 = System.nanoTime()
//    val result = writeToCassandra(df)
//    val t1 = System.nanoTime()
//    println("Elapsed time: " + (t1 - t0) + "ns")
//  }
//
//  def writeToCassandra(df: DataFrame) {
//    val renamed = df.withColumnRenamed("sessionId", "sessionid")
//
//    renamed.bulkLoadToCass(
//      keyspaceName = keyspace,
//      tableName = tableName
//    )
//  }
//
//  def readFromCassandra(sparkSession: SparkSession) {
//    val dfFromCassandra = sparkSession
//      .read
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> tableName, "keyspace" -> keyspace))
//      .load()
//
//    dfFromCassandra.show(10)
//  }
//
//
//}
