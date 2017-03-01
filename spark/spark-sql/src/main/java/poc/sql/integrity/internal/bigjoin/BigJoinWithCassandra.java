package poc.sql.integrity.internal.bigjoin;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import poc.commons.time.Stream;
import poc.sql.integrity.internal.helper.SparkSessionInitializer;

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
    private static final String PARQUET_PATH = "file:///opt/Dropbox/dev/poc/_resources/bigdata/QR_500Q_ID";

    public SparkSession init() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/hadoop_home");
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();

        return sparkSessionInitializer.getSparkSession();
    }

    private void run(SparkSession sc) {
        SQLContext sqlContext = new SQLContext(sc);

        System.out.println("Read src data-frame from CSV file");
        Dataset<Row> fullDataset = readCSV(sqlContext);

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
        Stream streamFilter = new Stream();
        streamFilter.start();
        Dataset<Row> page = readPage(joined, 1000, 1000, true, false);
        page.show();
        streamFilter.stop();
        System.out.println("Filter Duration" + streamFilter.getDuration());


    }


    private Dataset<Row> join(Dataset<Row> fullDataset, Dataset<Row> idsDataset) {
        Column joinedColumn = fullDataset.col(TR_ID).equalTo(idsDataset.col(TR_ID));
        Dataset<Row> joined = fullDataset.intersect(idsDataset);
        return joined;
    }

    private Dataset<Row> filter20Precent(Dataset<Row> rowDataset) {
        Random r = new Random();
        return rowDataset.filter((FilterFunction<Row>) row -> {
                    if (r.nextFloat() <= 0.20F) {
                        return true;
                    } else {
                        return false;
                    }
                }
        ).select(col(TR_ID));
    }


    private Dataset<Row> readPage(Dataset<Row> df, int skip, int limit, boolean isFiltered, boolean alreadySorted) {
        Dataset<Row> skipped = df.filter(col(TR_ID).geq(skip));
        if (!isFiltered) {
            skipped = skipped.filter(col(TR_ID).lt(skip + limit));
        }
        Dataset<Row> limited = skipped.limit(limit);
        if (!alreadySorted) {
            limited = limited.sort(TR_ID);
        }
        return limited;
    }


    private Dataset<Row> readCSV(SQLContext sqlContext) {
        return sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .csv(CSV_PATH);
    }

    private Dataset<Row> readParquet(SQLContext sqlContext) {
        return sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .parquet(PARQUET_PATH);
    }


    public static void main(String[] args) {
        BigJoinWithCassandra app = new BigJoinWithCassandra();
        SparkSession sparkSession = app.init();
        app.run(sparkSession);
    }
}

