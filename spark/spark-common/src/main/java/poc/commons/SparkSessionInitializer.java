package poc.commons;

import org.apache.spark.sql.SparkSession;

/**
 * Created by elevy on 17-Feb-17.
 */
public class SparkSessionInitializer {


    public SparkSession init() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/hadoop_home");
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();

        return sparkSessionInitializer.getSparkSession();
    }

    public SparkSession getSparkSession() {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        String warehouseLocation = System.getProperty("user.dir") + "/_resources/spark-warehouse";
        return SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[8]")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .getOrCreate();
    }
}
