package poc.sql.integrity.internal.bigfilter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import poc.commons.time.StreamTimer;
import poc.commons.SparkSessionInitializer;
import poc.sql.integrity.internal.helper.DatasetHelper;
import poc.sql.integrity.internal.helper.FileHelper;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Random;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonicallyIncreasingId;


/**
 * Created by eyallevy on 08/01/17.
 * <p>
 * Result:
 * =======

 */
//"NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING"
public class BigFilterWithPairRDD implements Serializable {
    private static final String TR_ID = "ID";

    //WINDOW
    //private static final String CSV_PATH = "file:///Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/bigdata/QR_500K.csv";
    //Private static final String PARQUET_PATH = "file:///Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/bigdata/QR_500K";

    //UBUNTU
    //private static final String CSV_PATH = "file:///opt/Dropbox/dev/poc/_resources/bigdata/QR_500K.csv";
    //private static final String PARQUET_PATH = "file:///opt/Dropbox/dev/poc/_resources/bigdata/QR_500Q_ID";
    private static final String CSV_PATH = "file:///opt/Dropbox/dev/poc/_resources/data/proxy_fixed.csv";
    private static final String PARQUET_PATH = "file:///opt/Dropbox/dev/poc/_resources/data/proxy_fixed";



    private void run(SparkSession sc) {
        StreamTimer streamTimerFilter = new StreamTimer();
        DatasetHelper datasetHelper = new DatasetHelper();
        FileHelper fileHelper =  new FileHelper();

        SQLContext sqlContext = new SQLContext(sc);

        System.out.println("Read src data-frame from CSV file");
        Dataset<Row> fullDataset = fileHelper.readCSV(sqlContext, CSV_PATH);

        System.out.println("Add IDs to data-frame");
        Dataset<Row> datasetWithId = fullDataset.withColumn(TR_ID, monotonicallyIncreasingId());

        System.out.println("Filter 20% of the data into new DF and get only the IDs");
        Dataset<Row> idsOnly20PrecentDataset = datasetHelper.filter20Precent(datasetWithId, TR_ID);
        idsOnly20PrecentDataset.sort(col(TR_ID)).show();

        System.out.println("convert Ids to PairRDD");
        streamTimerFilter.start();
        JavaPairRDD<Long, Boolean> idsJavaPairRDD = datasetHelper.convertToPairRDD(idsOnly20PrecentDataset, TR_ID);
        streamTimerFilter.stop();
        System.out.println("ToPairRDD Duration " + streamTimerFilter.getDuration());

//        System.out.println("convert Ids to Map");
//        streamTimerFilter.start();
//        Map<Long, Boolean> idsMapWithJava = convertToMapJava(idsOnly20PrecentDataset);
//        streamTimerFilter.stop();
//        System.out.println("ToMapJava Duration " + streamTimerFilter.getDuration());


        System.out.println("Filter the source DF by the ids ");
        streamTimerFilter.start();
        Dataset<Row> joined = filter(datasetWithId, idsJavaPairRDD);
        streamTimerFilter.stop();
        System.out.println("Filter Duration " + streamTimerFilter.getDuration());

        System.out.println("Paging - take 1000 starting from 1000");
        streamTimerFilter.start();
        Dataset<Row> page = datasetHelper.readPage(joined, 1000, 1000, true, true, TR_ID);
        page.show();
        streamTimerFilter.stop();
        System.out.println("Paging Duration " + streamTimerFilter.getDuration());
    }

    private Dataset<Row> filter(Dataset<Row> fullDataset, JavaPairRDD<Long, Boolean> javaPairRDD) {
        return fullDataset.filter((FilterFunction<Row>) row -> {
            Long longValue = row.getAs(TR_ID);
            List<Boolean> lookup = javaPairRDD.lookup(longValue);
            if (lookup != null && lookup.size() > 0) {
                return true;
            } else {
                return false;
            }
        });
    }


    public static void main(String[] args) {
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        SparkSession sparkSession = sparkSessionInitializer.getSparkSession();

        BigFilterWithPairRDD app = new BigFilterWithPairRDD();
        app.run(sparkSession);
    }

}

