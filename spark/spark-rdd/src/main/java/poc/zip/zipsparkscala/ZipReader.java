package poc.zip.zipsparkscala;

import org.apache.commons.compress.utils.Charsets;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.util.Try;

import java.util.Arrays;
import java.util.List;


/**
 * Computes an approximation to pi
 * Usage: JavaSparkPi [slices]
 */
public final class ZipReader {

    public static void main(String[] args) throws Exception {
        String path ="/opt/Dropbox/dev/git-hub/poc/_resources/data/zip/data.zip";
        String sevenZPath = "file:///opt/Dropbox/dev/git-hub/poc/_resources/bigdata/ZIP/proxy_fixed.csv.7z";

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");

        SparkSession spark = SparkSession.builder().appName("ZipReader").config(conf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        // SQLContext sqlContext = new SQLContext(jsc);


        ////////////////////////////////////////////////////////////////////////////
        JavaPairRDD<String, PortableDataStream> sevenZipRdd = jsc.binaryFiles(sevenZPath);
        JavaRDD<Tuple2<String, String>> sevenZExtracted = new SevenZExtractor().extractAndDecode(sevenZipRdd, Charsets.UTF_8);
        ////////////////////////////////////////////////////////////////////////////



        ////////////////////////////////////////////////////////////////////////////
        JavaPairRDD<String, PortableDataStream> zipRdd = jsc.binaryFiles(path);
        JavaRDD<Tuple2<String, String>> extracted = new ZipExtractor().extractAndDecode(zipRdd, Charsets.UTF_8);

        JavaRDD<String> content = extracted.map(s -> new String(s._2.getBytes()));
        System.out.println("CONTENT:\n" + content.collect());

        JavaRDD<String> lines = content.flatMap(line -> {
                                            List<String> splitLine = Arrays.asList(line.split("\n"));
                                            return splitLine.iterator();
                                        });
        System.out.println("LINES:\n" + lines.collect());
////////////////////////////////////////////////////////////////////////////



        jsc.stop();


        System.out.println();
    }

}

