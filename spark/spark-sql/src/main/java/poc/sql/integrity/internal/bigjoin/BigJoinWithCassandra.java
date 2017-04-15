package poc.sql.integrity.internal.bigjoin;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import poc.commons.time.StreamTimer;
import poc.commons.SparkSessionInitializer;
import poc.sql.integrity.internal.helper.DatasetHelper;
import poc.sql.integrity.internal.helper.FileHelper;

import java.io.Serializable;
import java.util.Random;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonicallyIncreasingId;


/**
 * Created by eyallevy on 08/01/17.
 */
public class BigJoinWithCassandra implements Serializable {
    private static final String TR_ID = "ID";

    //    private static final String CSV_PATH = "file:///Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/bigdata/QR_500K.csv";
//    private static final String PARQUET_PATH = "file:///Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/bigdata/QR_500K";
    private static final String CSV_PATH = "file:///opt/Dropbox/dev/poc/_resources/bigdata/QR_500K.csv";

    private void run(SparkSession sc) {
        SQLContext sqlContext = new SQLContext(sc);
        FileHelper fileHelper = new FileHelper();
        DatasetHelper datasetHelper = new DatasetHelper();

        System.out.println("Read src data-frame from CSV file");
        Dataset<Row> fullDataset = fileHelper.readCSV(sqlContext, CSV_PATH);

        System.out.println("Add IDs to data-frame");
        Dataset<Row> datasetWithId = fullDataset.withColumn(TR_ID, monotonicallyIncreasingId());

        //datasetWithId.write().mode(SaveMode.Overwrite).parquet(PARQUET_PATH);
        //Dataset<Row> rowDataset = readParquet(sqlContext);

        System.out.println("Filter 20% of the data into new DF and get only the IDs");
        Dataset<Row> idsOnly20PrecentDataset = filter20Precent(datasetWithId);

        //join 2 dataset
        System.out.println("Join the source DF by the ids Map");
        Dataset<Row> joined = join(datasetWithId, idsOnly20PrecentDataset);

        //Paging
        StreamTimer streamTimerFilter = new StreamTimer();
        streamTimerFilter.start();
        Dataset<Row> page = datasetHelper.readPage(joined, 1000, 1000, true, false, TR_ID);
        page.show();
        streamTimerFilter.stop();
        System.out.println("Filter Duration" + streamTimerFilter.getDuration());
    }

    private Dataset<Row> join(Dataset<Row> fullDataset, Dataset<Row> idsDataset) {
//        Column joinedColumn = fullDataset.col(TR_ID).equalTo(idsDataset.col(TR_ID));
        return fullDataset.intersect(idsDataset);
    }

    private Dataset<Row> filter20Precent(Dataset<Row> rowDataset) {
        Random r = new Random();
        return rowDataset.filter((FilterFunction<Row>) row -> r.nextFloat() <= 0.20F).select(col(TR_ID));
    }

    public static void main(String[] args) {
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        SparkSession sparkSession = sparkSessionInitializer.getSparkSession();

        BigJoinWithCassandra app = new BigJoinWithCassandra();
        app.run(sparkSession);
    }
}

