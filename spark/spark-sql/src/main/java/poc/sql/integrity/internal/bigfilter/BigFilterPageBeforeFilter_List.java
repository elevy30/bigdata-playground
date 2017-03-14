package poc.sql.integrity.internal.bigfilter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import poc.commons.time.StreamTimer;
import poc.sql.integrity.internal.helper.DatasetHelper;
import poc.sql.integrity.internal.helper.FileHelper;
import poc.commons.SparkSessionInitializer;
import poc.sql.integrity.internal.prop.Prop;
import poc.sql.integrity.internal.prop.Properties_1;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;


/**
 * Created by eyallevy on 08/01/17.
 * <p>
 * Result:
 * =======
 */
public class BigFilterPageBeforeFilter_List implements Serializable {

    private Prop prop = new Properties_1();
    private StreamTimer streamTimerFilter = new StreamTimer();
    private FileHelper fileHelper = new FileHelper();
    private DatasetHelper datasetHelper = new DatasetHelper();

    private void run(SparkSession sc) {

        SQLContext sqlContext = new SQLContext(sc);

        System.out.println("Read src data-frame from CSV file");
        Dataset<Row> dataSource = fileHelper.readCSV(sqlContext, prop.getDataSourceIdPath());

        System.out.println("Filter 20% of the data into new DF and get only the IDs");
        Dataset<Row> idsOnly20PrecentDataset = datasetHelper.filter20Precent(dataSource, prop.getId());

//        System.out.println("# Of Lines in source file " + fullDataset.count());
//        System.out.println("# Of Lines in ids file " + idsOnly20PrecentDataset.count());

        Dataset<Row> idsSorted = idsOnly20PrecentDataset.sort(col(prop.getId()));
//        idsSorted = idsSorted.cache();


        boolean hasNextPage;
        Long startFrom = 0L;
        int pageNumber = 0;
        int pageSize = 100;
        do {
            streamTimerFilter.reset();

            System.out.println("Collect Ids For page " + (pageNumber + 1) + " start form id " + startFrom);
            Dataset<Row> pageIdsDS = datasetHelper.collectIdsForSpecificPage(idsSorted, startFrom, pageSize, prop.getId(), streamTimerFilter);

            System.out.println("Collect ids as a List");
            List<Long> pageIdsList = collectAsList(pageIdsDS);

            if (pageIdsList.size() > 0) {
                System.out.println("Get the next start from ID");
                startFrom = pageIdsList.stream().max(Long::compareTo).orElse(-1L);

                System.out.println("Filter the source DF by the ids Map");
                Dataset<Row> page = filterByList(dataSource, pageIdsList);
                page.show();
//                statistics(startFrom, pageNumber, pageIdsDS, pageIdsMap, page);

                startFrom++;
                pageNumber++;
                hasNextPage = true;
            } else {
                System.out.println("Page " + (pageNumber + 1) + " is Empty!!!");
                hasNextPage = false;
            }

            System.err.println("\n\nTotal Duration: " + streamTimerFilter.totalDuration +"\n\n");
        }
        while (hasNextPage);
    }

    private List<Long> collectAsList(Dataset<Row> rowDataset) {
        streamTimerFilter.start();

        JavaPairRDD<Long, Boolean> idsMapJavaPairRDD = rowDataset.select(prop.getId())
                .toJavaRDD()
                .mapToPair((PairFunction<Row, Long, Boolean>) row -> new Tuple2<Long, Boolean>(row.getLong(0), true));

        List<Tuple2<Long, Boolean>> collect = idsMapJavaPairRDD.collect();
        List<Long> ids = new ArrayList<>();
        for (Tuple2 tuple: collect) {
            ids.add((Long)(tuple._1()));
        }

        streamTimerFilter.stop();
        streamTimerFilter.updateTotal();
        System.err.println("collectAsList Duration: " + streamTimerFilter.getDuration());

        return ids;
    }

    private Dataset<Row> filterByList(Dataset<Row> fullDataset, List<Long> idsList) {
        streamTimerFilter.start();

        @SuppressWarnings("SuspiciousMethodCalls")
        Dataset<Row> filtered = fullDataset.filter((FilterFunction<Row>) row -> idsList.contains(row.getAs(prop.getId())));
        filtered.show();

        streamTimerFilter.stop();
        streamTimerFilter.updateTotal();
        System.err.println("Filter Duration: " + streamTimerFilter.getDuration());

        return filtered;
    }

    @SuppressWarnings("unused")
    private void statistics(Long startFrom, int pageNumber, Dataset<Row> pageIdsDS, Map<Long, Boolean> pageIdsMap, Dataset<Row> page) {
        System.out.println("----- STATISTICS -----");
        System.out.println("Max id in page " + (pageNumber + 1) + " is " + startFrom);
        //System.out.println("# Of ids in page " + (pageNumber + 1) + ": " + pageIdsDS.count());
        System.out.println("# Of ids in map " + pageIdsMap.size());
        System.out.println("# Of Filtered Lines in page " + (pageNumber + 1) + ": " + page.count());
    }


    public static void main(String[] args) {
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        SparkSession sparkSession = sparkSessionInitializer.init();

        BigFilterPageBeforeFilter_List app = new BigFilterPageBeforeFilter_List();
        app.run(sparkSession);
    }
}

