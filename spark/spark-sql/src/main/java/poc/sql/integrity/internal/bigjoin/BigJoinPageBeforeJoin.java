package poc.sql.integrity.internal.bigjoin;

import org.apache.spark.sql.*;
import poc.commons.time.Stream;
import poc.sql.integrity.internal.helper.DatasetHelper;
import poc.sql.integrity.internal.helper.FileHelper;
import poc.sql.integrity.internal.helper.SparkSessionInitializer;
import poc.sql.integrity.internal.prop.Prop;
import poc.sql.integrity.internal.prop.Properties_1;

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonicallyIncreasingId;


/**
 * Created by eyallevy on 08/01/17.
 */
public class BigJoinPageBeforeJoin implements Serializable {

    Prop prop = new Properties_1();
    Stream streamFilter = new Stream();
    DatasetHelper datasetHelper = new DatasetHelper();
    FileHelper fileHelper = new FileHelper();

    public SparkSession init() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/hadoop_home");
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();

        return sparkSessionInitializer.getSparkSession();
    }

    private void run(SparkSession sc) {
        SQLContext sqlContext = new SQLContext(sc);

        System.out.println("####### Read datasource from CSV file");
        Dataset<Row> dataSource = fileHelper.readCSV(sqlContext, prop.getDataSourcePath());
        //System.out.println("#OfRow in the Full Dataset " + fullDataset.count());
        //fullDataset.printSchema();

        System.out.println("####### Filter 20 precent of lines");
        Dataset<Row> idsOnly20PrecentDataset = datasetHelper.filter20Precent(dataSource, prop.getId());
        Dataset<Row> idsSorted = idsOnly20PrecentDataset.sort(col(prop.getId()));
        long numOfIntegrity = idsSorted.count();
        //System.out.println("#OfRow in the Filtered Dataset " + idsOnly20PrecentDataset.count());
        //idsOnly20PrecentDataset.show();

        boolean hasNextPage = false;
        Long startFrom = 0L;
        int pageNumber = 0;
        int pageSize = 100;
        do {
            streamFilter.reset();

            System.out.println("Collect Ids For page " + (pageNumber + 1) + " start form id " + startFrom);
            Dataset<Row> pageIdsDS = datasetHelper.collectIdsForSpecificPage(idsSorted, startFrom, pageSize, prop.getId(), streamFilter);

            Dataset<Row> joined = join(dataSource, pageIdsDS);
            hasNextPage = false;

//            if (pageIdsList.size() > 0) {
//                System.out.println("Get the next start from ID");
//                startFrom = pageIdsList.stream().max(Long::compareTo).orElse(-1L);
//
//                System.out.println("Filter the source DF by the ids Map");
//                Dataset<Row> page = filter(datasetWithId, pageIdsList);
////                statistics(startFrom, pageNumber, pageIdsDS, pageIdsMap, page);
//
//                startFrom++;
//                pageNumber++;
//                hasNextPage = true;
//            } else {
//                System.out.println("Page " + (pageNumber + 1) + " is Empty!!!");
//                hasNextPage = false;
//            }

            System.err.println("\n\nTotal Duration: " + streamFilter.totalDuration + "\n\n");
        }
        while (hasNextPage);
    }


    private Dataset<Row> join(Dataset<Row> fullDataset, Dataset<Row> idsDataset) {
        streamFilter.start();

        Column joinedColumn = fullDataset.col(prop.getId()).equalTo(idsDataset.col(prop.getId()));
        Dataset<Row> joined = idsDataset.join(fullDataset, joinedColumn).drop(idsDataset.col(prop.getId()));
        joined.show();

        streamFilter.stop();
        streamFilter.updateTotal();
        System.err.println("Join Duration: " + streamFilter.getDuration());

        return joined;
    }


    public static void main(String[] args) {
        BigJoinPageBeforeJoin app = new BigJoinPageBeforeJoin();
        SparkSession sparkSession = app.init();
        app.run(sparkSession);
    }
}

