package poc;

import poc.driver.SparkDriver;
import org.apache.spark.SparkConf;

/**
 * Created by eyallevy on 08/01/17.
 */
public class Main {

    public static void main(String[] args) {
//        if (args.length != 2) {
//            System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
//            System.exit(1);
//        }

        SparkConf conf = new SparkConf();
        conf.setAppName("Java API demo");
        conf.setMaster("local[2]");
        conf.set("spark.cassandra.connection.host", "127.0.0.1");
        conf.set("spark.cassandra.connection.port", "9042");

        SparkDriver app = new SparkDriver(conf);
        app.run();
    }
}
