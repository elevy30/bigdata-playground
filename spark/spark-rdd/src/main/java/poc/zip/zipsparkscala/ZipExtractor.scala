package poc.zip.zipsparkscala

import java.nio.charset._

import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.input.PortableDataStream

import scala.util.Try

class ZipExtractor extends Serializable {

  def extractFiles(ps: PortableDataStream, n: Int = 1024) = Try {
    val zipInputStream = new ZipArchiveInputStream(ps.open)
    println("zipInputStream: " + zipInputStream)
    Stream.continually(Option(zipInputStream.getNextZipEntry))
      // Read until next entry is null
      .takeWhile(_.isDefined)
      // flatten
      .flatMap(x => {
          println("flatMap: " + x)
          x
      })
      // Drop directories
      .filter(!_.isDirectory)
      .map(e => {
        println("file name: " + e)
        Stream.continually({
          // Read n bytes
          val buffer = Array.fill[Byte](n)(-1)
          val i = zipInputStream.read(buffer, 0, n)
          (i, buffer.take(i))
        })
          // Take as long as we've read something
        .takeWhile(_._1 > 0)
        .flatMap(x => x._2)
        .toArray

      }).toArray
  }

  def decode(charset: Charset = StandardCharsets.UTF_8)(bytes: Array[Byte]) =
    new String(bytes, StandardCharsets.UTF_8)

  def extractAndDecode(javaRDD: JavaPairRDD[String, PortableDataStream], charset: Charset): JavaRDD[(String, String)] = {
    javaRDD.rdd
      .flatMapValues(x => extractFiles(x).toOption)
      .flatMapValues(_.map(decode()))
      .toJavaRDD()
  }
}