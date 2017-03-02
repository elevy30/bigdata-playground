package poc.sql.integrity.internal;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;


/**
 * Created by eyallevy on 08/01/17.
 */
public class SparkReadFromDataSource implements Serializable {

    private transient SparkConf conf;

    public SparkReadFromDataSource(SparkConf conf) {
        this.conf = conf;
    }

    public void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        readParquetFile(sc);
    }

    private void readParquetFile(JavaSparkContext sc) {

        System.out.println("###########_read file from parquet");
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> dataSet = sqlContext.read().option("header", true).parquet("/opt/tr/data-source/output/50/staging/integrity");

        System.out.println("###########_convert dataframe to javardd");
        JavaRDD dataRdd = dataSet.toJavaRDD();


        System.out.println("###########_Number of lines ");
        System.out.println(dataSet.count());
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Java API demo");
        conf.setMaster("local[2]");


        SparkReadFromDataSource app = new SparkReadFromDataSource(conf);
        app.run();

    }
}
