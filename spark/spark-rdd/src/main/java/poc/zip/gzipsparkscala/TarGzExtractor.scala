package poc.zip.gzipsparkscala;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.input.PortableDataStream

import scala.util.Try
import java.nio.charset._

class TarGzExtractor extends Serializable {

  def extractFiles(ps: PortableDataStream, n: Int = 1024) = Try {
    val gzipInputStream = new GzipCompressorInputStream(ps.open)
    val tar = new TarArchiveInputStream(gzipInputStream)
    println(tar)
    Stream.continually(Option(tar.getNextTarEntry))
      // Read until next entry is null
      .takeWhile(_.isDefined)
      // flatten
      .flatMap(x => {
          println("hello")
          x
      })
      // Drop directories
      .filter(!_.isDirectory)
      .map(e => {
        println(e)
        Stream.continually {
          // Read n bytes
          val buffer = Array.fill[Byte](n)(-1)
          val i = tar.read(buffer, 0, n)
          (i, buffer.take(i))
        }
          // Take as long as we've read something
        .takeWhile(_._1 > 0)
        .flatMap(_._2)
        .toArray
      }).toArray
  }

  def decode(charset: Charset = StandardCharsets.UTF_8)(bytes: Array[Byte]) =
    new String(bytes, StandardCharsets.UTF_8)

  def extractAndDecode(javaRDD: JavaPairRDD[String, PortableDataStream], charset: Charset): JavaRDD[(String, String)] = {
    javaRDD.rdd
      .flatMapValues(x => {
        println(x)
        extractFiles(x).toOption
      })
      .flatMapValues(_.map(decode()))
      .toJavaRDD()
  }
}