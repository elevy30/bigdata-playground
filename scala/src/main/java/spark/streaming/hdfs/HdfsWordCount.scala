// usage within spark-shell: HdfsWordCount.main(Array("hdfs://quickstart.cloudera:8020/user/cloudera/sparkStreaming/"))

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Counts words in new text files created in the given directory
  * Usage: HdfsWordCount <directory>
  * <directory> is the directory that Spark Streaming will use to find and read new text files.
  *
  * To run this on your local machine on directory `localdir`, run this example
  * $ bin/run-example \
  *       org.apache.spark.examples.streaming.HdfsWordCount localdir
  *
  * Then create a text file in `localdir` and the words in the file will get counted.
  */
object HdfsWordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    sparkConf.setMaster("local[2]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    if (words != null) System.out.println("HHHHHHH")
    val wordCounts = words.map(x => {
      System.out.println("HHHHHHH -> " + x);
      (x, 1)
    }).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()


    //    val words = ssc.fileStream[LongWritable, Text, TextInputFormat]("/user/elevy").map(_._2.toString).map(u => u.split(" "))
    //    if (words != null) System.out.println("HHHHHHH")
    //    val wordCounts = words.map(x => {System.out.println("HHHHHHH -> " + x);(x, 1)}).reduceByKey(_ + _)
    //    wordCounts.print()
    //    ssc.start()
    //    ssc.awaitTermination()
  }
}