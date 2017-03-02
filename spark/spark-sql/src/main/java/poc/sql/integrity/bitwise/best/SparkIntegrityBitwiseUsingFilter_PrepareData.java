package poc.sql.integrity.bitwise.best;

import org.apache.spark.sql.SparkSession;
import poc.sql.integrity.internal.generator.BitwiseGenerator;
import poc.sql.integrity.internal.helper.SparkSessionInitializer;
import poc.sql.integrity.internal.prop.Prop;
import poc.sql.integrity.internal.prop.Properties_1;

import java.io.Serializable;

/**
 * Created by eyallevy on 08/01/17.
 */
public class SparkIntegrityBitwiseUsingFilter_PrepareData implements Serializable {

    public static void main(String[] args) {
        SparkIntegrityBitwiseUsingFilter_PrepareData app = new SparkIntegrityBitwiseUsingFilter_PrepareData();
        SparkSession sparkSession = app.init();
        app.run(sparkSession);
    }

    private SparkSession init() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/hadoop_home");
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        return sparkSessionInitializer.getSparkSession();
    }

    private void run(SparkSession sc) {
        Prop prop = new Properties_1();
        BitwiseGenerator fileBitwiseGenerator = new BitwiseGenerator(sc, prop);
       fileBitwiseGenerator.generateDummyIntegrityBitwise();
    }
}
