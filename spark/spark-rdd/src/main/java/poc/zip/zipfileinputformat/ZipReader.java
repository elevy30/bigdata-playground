package poc.zip.zipfileinputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map;


/**
 * Computes an approximation to pi
 * Usage: JavaSparkPi [slices]
 */
public final class ZipReader {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .config(conf)
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaPairRDD<Text, BytesWritable> rdd1 = jsc.newAPIHadoopFile("file:///opt/Dropbox/dev/git-hub/poc/_resources/data/zip/data.zip", ZipFileInputFormat.class, Text.class, BytesWritable.class, new Job().getConfiguration());

//        Map<Text, BytesWritable> textTextMap = rdd1.collectAsMap();
//        System.out.println();

        JavaPairRDD<String, String[]> rdd2 = rdd1.flatMapToPair((PairFlatMapFunction<Tuple2<Text, BytesWritable>, String, String[]>) (Tuple2<Text, BytesWritable> textTextTuple2) -> {
            List<Tuple2<String,String[]>> newList = new ArrayList<>();

            InputStream is = new ByteArrayInputStream(textTextTuple2._2.getBytes());
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

            String line;
            line  = br.readLine();
            Tuple2<String,String[]> header = new Tuple2<>("header", line.split(","));
            newList.add(header);
            while ((line = br.readLine()) != null) {
                Tuple2<String,String[]> newTuple = new Tuple2<>(line,line.split(","));
                newList.add(newTuple);
            }

            return newList.iterator();
        });


        Map<String, String[]> stringStringMap = rdd2.collectAsMap();
        for (String key: stringStringMap.keySet()){
            System.out.println(key + " -> " + Arrays.asList(stringStringMap.get(key)));
        }

    }
}

