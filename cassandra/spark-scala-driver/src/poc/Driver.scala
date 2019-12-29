package poc

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by eyallevy on 10/01/17.
  */
object Driver {

  def main(args: Array[String]): Unit = {
    println("Hello")

    val (conf: SparkConf, sc: SparkContext) = init

    createTable(conf)

    insertData(sc)

    readData(sc)

    sc.stop

  }

  private def init = {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")


    val sc = new SparkContext("local[2]", "cassandra", conf)
    (conf, sc)
  }

  private def createTable(conf: SparkConf) = {
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS thetaray");
      session.execute("CREATE KEYSPACE thetaray WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
      session.execute("CREATE TABLE thetaray.products (id INT PRIMARY KEY, name TEXT)");
    }
  }

  private def insertData(sc: SparkContext) = {
    val collection = sc.parallelize(Seq((1, "hello"), (2, "world")))
    collection.saveToCassandra("thetaray", "products", SomeColumns("id", "name"))
  }

  private def readData(sc: SparkContext) = {
    var rdd = sc.cassandraTable("thetaray", "products")

    println(rdd.count)
    println(rdd.first)
  }

}
