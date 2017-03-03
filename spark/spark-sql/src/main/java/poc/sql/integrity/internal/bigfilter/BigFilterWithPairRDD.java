package poc.sql.integrity.internal.bigfilter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import poc.commons.time.Stream;
import poc.sql.integrity.internal.helper.SparkSessionInitializer;
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

    public SparkSession init() {
        System.out.println("NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING");
        System.out.println("NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING");
        System.out.println("NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING");
        System.out.println("NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING");
        System.out.println("NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING - NOT WORKING");
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/hadoop_home");
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();

        return sparkSessionInitializer.getSparkSession();
    }

    private void run(SparkSession sc) {
        Stream streamFilter = new Stream();

        SQLContext sqlContext = new SQLContext(sc);

        System.out.println("Read src data-frame from CSV file");
        Dataset<Row> fullDataset = readCSV(sqlContext);

        System.out.println("Add IDs to data-frame");
        Dataset<Row> datasetWithId = fullDataset.withColumn(TR_ID, monotonicallyIncreasingId());

        System.out.println("Filter 20% of the data into new DF and get only the IDs");
        Dataset<Row> idsOnly20PrecentDataset = filter20Precent(datasetWithId);
        idsOnly20PrecentDataset.sort(col(TR_ID)).show();

        System.out.println("convert Ids to PairRDD");
        streamFilter.start();
        JavaPairRDD<Long, Boolean> idsJavaPairRDD = convertToPairRDD(idsOnly20PrecentDataset);
        streamFilter.stop();
        System.out.println("ToPairRDD Duration " + streamFilter.getDuration());

//        System.out.println("convert Ids to Map");
//        streamFilter.start();
//        Map<Long, Boolean> idsMapWithJava = convertToMapJava(idsOnly20PrecentDataset);
//        streamFilter.stop();
//        System.out.println("ToMapJava Duration " + streamFilter.getDuration());


        System.out.println("Filter the source DF by the ids ");
        streamFilter.start();
        Dataset<Row> joined = filter(datasetWithId, idsJavaPairRDD);
        streamFilter.stop();
        System.out.println("Filter Duration " + streamFilter.getDuration());

        System.out.println("Paging - take 1000 starting from 1000");
        streamFilter.start();
        Dataset<Row> page = filterPage(joined, 1000, 1000, true, true);
        page.show();
        streamFilter.stop();
        System.out.println("Paging Duration " + streamFilter.getDuration());
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

    private JavaPairRDD<Long, Boolean> convertToPairRDD(Dataset<Row> rowDataset) {
        JavaPairRDD<Long, Boolean> idsMapJavaPairRDD = rowDataset.select(TR_ID).toJavaRDD().mapToPair(row -> new Tuple2<Long, Boolean>(row.getLong(0), true));
        return idsMapJavaPairRDD;
    }

    private Dataset<Row> filter(Dataset<Row> fullDataset, JavaPairRDD<Long, Boolean> javaPairRDD) {
        Dataset<Row> filtered = fullDataset.filter((FilterFunction<Row>) row -> {
            Long longValue = row.getAs(TR_ID);
            List<Boolean> lookup = javaPairRDD.lookup(longValue);
            if (lookup != null && lookup.size() > 0) {
                return true;
            } else {
                return false;
            }
        });
        return filtered;
    }


    private Dataset<Row> filterPage(Dataset<Row> df, int skip, int limit, boolean isFiltered, boolean alreadySorted) {

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
        BigFilterWithPairRDD app = new BigFilterWithPairRDD();
        SparkSession sparkSession = app.init();
        app.run(sparkSession);
    }
}

