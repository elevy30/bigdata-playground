//package spark.streaming.hdfs
//
///**
//  * Created by elevy on 01/02/17.
//  */
//class StructueStream {
//
//  // business object
//  case class Person(name: String, age: Long, address: String)
//
//  // you could build your schema manually
//  import org.apache.spark.sql.types._
//  val schema = StructType(
//      StructField("name", StringType, false) ::
//      StructField("age", LongType, false) ::
//      StructField("address", StringType, false) :: Nil)
//
//  // ...but is error-prone and time-consuming, isn't it?
//  import org.apache.spark.sql.Encoders
//  val schema = Encoders.product[Person].schema
//
//  val people = spark.readStream
//    .schema(schema)
//    .csv("/user/elevy/*.csv")
//    .as[Person]
//
//    // ...but it is still a Dataset.
//  // (Almost) any Dataset operation is available
//  val population = people.groupBy('address).agg(count('address) as "population")
//
//  // Start the streaming query
//  // print the result out to the console
//  // Only Complete output mode supported for groupBy
//  import org.apache.spark.sql.streaming.OutputMode.Complete
//  val populationStream = population.writeStream
//    .format("console")
//    .outputMode(Complete)
//    .queryName("Population")
//    .start
//}
//
