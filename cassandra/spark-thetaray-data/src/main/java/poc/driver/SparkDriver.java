package poc.driver;

import com.datastax.spark.connector.cql.CassandraConnector;
import poc.dal.CassandraWriter;
import poc.ddl.Creator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;


/**
 * Created by eyallevy on 08/01/17.
 */
public class SparkDriver implements Serializable {

    private transient SparkConf conf;

    public SparkDriver(SparkConf conf) {
        this.conf = conf;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Java API demo");
        conf.setMaster("local[2]");
        conf.set("spark.cassandra.connection.host", "127.0.0.1");
        conf.set("spark.cassandra.connection.port", "9042");

        SparkDriver app = new SparkDriver(conf);
        app.run();
    }

    public void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);

        createTables(sc);
        writeToCassandra(sc);
//        transform(sc);
//        writeTransToCassandra(sc);
//        sc.stop();
    }

    private void createTables(JavaSparkContext sc) {
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        Creator creator = new Creator();
        creator.createProxyTable(connector);
    }

    private void writeToCassandra(JavaSparkContext sc) {
//        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
//        JavaRDD<String> stringJavaRDD = sc.textFile("aaa.csv");
//        RDD<String> stringRDD = CassandraTableScanJavaRDD.toRDD(stringJavaRDD);



        System.out.println("###########_read data from csv");
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> dataSet = sqlContext.read().option("header", true).csv("cassandra/_input/aaa.csv");

        System.out.println("###########_convert dataframe to javardd");
        JavaRDD proxies = dataSet.toJavaRDD();

//        JavaRDD<Proxy> proxies1 = proxies.map( new Function<Row, Proxy>() {
//                    public Proxy call(Row record) {
//
//                        try {
//                            Integer id = record.getInt(0);
//
//                            return new Proxy(id,
//                                    record.getString(1),
//                                    record.getString(2),
//                                    record.getTimestamp(3),
//                                    record.getInt(4),
//                                    record.getDouble(5),
//                                    record.getDouble(6),
//                                    record.getDouble(7),
//                                    record.getString(8),
//                                    record.getString(9),
//                                    record.getString(10));
//
//                        }catch (Exception e){
//                            System.out.println("################### Fail to parse Long " + record.getString(0));
//                            System.out.println("###################" + record.getString(0) + "########################");
//                        }
//                        return  new Proxy();
//
//                }});


        System.out.println("###########_writing data to cassandra ");
        CassandraWriter writer = new CassandraWriter();
        writer.writeProxyData(proxies);
    }
}
