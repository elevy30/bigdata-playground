import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by eyallevy on 10/01/17.
  */


  class JoinWithScala {
    // API for scala
    def join(sc: SparkSession, ds1: Dataset, ds2 Dataset) {
      ds1.join(ds2, ds1.col("Id") === ds2.col("Id")).show
    }

  }

