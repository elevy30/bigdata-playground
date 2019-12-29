package poc.zip.gzipsparkscala;

import org.apache.commons.compress.utils.Charsets;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.SparkSession;
import poc.zip.zipsparkscala.SevenZExtractor;
import scala.Tuple2;


/**
 * Computes an approximation to pi
 * Usage: JavaSparkPi [slices]
 */
public final class ZipReader {

    public static void main(String[] args) throws Exception {
        String tarGZipPath ="file:///opt/Dropbox/dev/git-hub/poc/_resources/data/zip/data.zip";

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");

        SparkSession spark = SparkSession.builder().appName("ZipReader").config(conf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        //extract TAR GZIP file
        JavaPairRDD<String, PortableDataStream> tarGzipRdd = jsc.binaryFiles(tarGZipPath);
        JavaRDD<Tuple2<String, String>> extractedTarGzip = new TarGzExtractor().extractAndDecode(tarGzipRdd, Charsets.UTF_8);
        System.out.println(extractedTarGzip.collect());

        jsc.stop();


        System.out.println();
    }

}

