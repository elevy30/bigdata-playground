package poc.sql.integrity.internal;

import org.apache.spark.sql.*;

import java.io.Serializable;


/**
 * Created by eyallevy on 08/01/17.
 */
public class CreateNoValidValues implements Serializable {

    //    public static final String CSV_PATH = "file:///Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/data/proxy_fixed.csv";
    private static final String CSV_PATH = "file:///opt/Dropbox/dev/poc/data/proxy_fixed.csv";
    //    public static final String PARQUET_PATH = "file:///Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/data/proxy_fixed";
    private static final String PARQUET_PATH = "file:///opt/Dropbox/dev/poc/data/proxy_fixed";

    public void run() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/01_poc/hadoop_home");
        SparkSession spark = getSparkSession();
        readParquetFile(spark);
    }

    private void readParquetFile(SparkSession sc) {
        System.out.println("###########_read file from parquet");
        SQLContext sqlContext = new SQLContext(sc);

        convertCSVToParquet(sqlContext);

        Dataset<Row> dataSetParquet = readParquet(sqlContext);
        dataSetParquet.show();
    }

    private SparkSession getSparkSession() {
        return SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[1]")
                .getOrCreate();
    }

    private void convertCSVToParquet(SQLContext sqlContext) {
        Dataset<Row> dataSetCSV = sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .csv(CSV_PATH);

        dataSetCSV.write().mode(SaveMode.Overwrite).parquet(PARQUET_PATH);
    }

    private Dataset<Row> readParquet(SQLContext sqlContext) {
        return sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .parquet(PARQUET_PATH);
    }


    public static void main(String[] args) {
        CreateNoValidValues app = new CreateNoValidValues();
        app.run();
    }
}
