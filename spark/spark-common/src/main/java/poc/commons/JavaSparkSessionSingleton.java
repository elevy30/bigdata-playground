package poc.commons;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Created by elevy on 21/12/16.
 */
public class JavaSparkSessionSingleton {

    /**
     * Lazily instantiated singleton instance of SparkSession
     */
    private static transient SparkSession instance = null;

    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}
