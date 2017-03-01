package poc.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;


public  class CountWords implements Serializable {

    public static void main(String[] args) {
        String logFile = "/opt/sparkwork/mRemote.log";

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        //conf.setMaster("spark://elevy:7077");
        conf.setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter( s -> new Boolean(s.contains("exit"))).count();
        long numBs = logData.filter(s -> new Boolean(s.contains("Start"))).count();

//        long numAs = logData.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) {
//                return s.contains("exit");
//            }
//        }).count();
//
//
//        long numBs = logData.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) { return s.contains("start"); }
//        }).count();

        System.out.println("Lines with Exit: " + numAs + ", lines with Start: " + numBs);

        sc.stop();
    }
}
