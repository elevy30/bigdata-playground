//package spark.streaming.hdfs
//
//// usage within spark-shell: HdfsWordCount.main(Array("hdfs://quickstart.cloudera:8020/user/cloudera/sparkStreaming/"))
//
//import java.sql.Time
//import java.time.Duration
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.reflect.ClassTag
//
//
///**
//  * Counts words in new text files created in the given directory
//  * Usage: HdfsWordCount <directory>
//  *   <directory> is the directory that Spark Streaming will use to find and read new text files.
//  *
//  * To run this on your local machine on directory `localdir`, run this example
//  *    $ bin/run-example \
//  *       org.apache.spark.examples.streaming.HdfsWordCount localdir
//  *
//  * Then create a text file in `localdir` and the words in the file will get counted.
//  */
//object RddToStream {
//  def main(args: Array[String]) {
//    if (args.length < 1) {
//      System.err.println("Usage: HdfsWordCount <directory>")
//      System.exit(1)
//    }
//
//    //StreamingExamples.setStreamingLogLevels()
//    val conf = new SparkConf().setAppName("HdfsWordCount")
//    conf.setMaster("local[2]")
//    // Create the context
//    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(conf, Seconds(2))
//
//
//    val lines = sc.textFile("hdfs://localhost:9000/user/elevy/testdata.txt")
//    //val inputStream = ssc.queueStream(lines)
//
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => {System.out.println("HHHHHHH -> " + x);(x, 1)}).reduceByKey(_ + _)
////    // Create the FileInputDStream on the directory and use the
////    // stream to count words in new files created
////    val lines = ssc.textFileStream(args(0))
////    val words = lines.flatMap(_.split(" "))
////    if (words != null) System.out.println("HHHHHHH")
////    val wordCounts = words.map(x => {System.out.println("HHHHHHH -> " + x);(x, 1)}).reduceByKey(_ + _)
////    wordCounts.print()
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//}
//
//class RDDExtension[T: ClassTag](rdd: RDD[T]) {
//  def toStream(streamingContext: StreamingContext, chunkSize: Int, slideDurationMilli: Option[Long] = None): DStream[T] = {
//    new InputDStream[T](streamingContext) {
//
//      private val iterator = rdd.toLocalIterator
//      // WARNING: each partition much fit in RAM of local machine.
//      private val grouped = iterator.grouped(chunkSize)
//
//      override def start(): Unit = {}
//
//      override def stop(): Unit = {}
//
//       def compute(validTime: Time) = {
//        if (grouped.hasNext) {
//          Some(rdd.sparkContext.parallelize(grouped.next()))
//        } else {
//          None
//        }
//      }
//
//       def slideDuration = {
//        slideDurationMilli.map(duration => new Duration(duration)).getOrElse(super.slideDuration)
//      }
//    }
//  }
//}