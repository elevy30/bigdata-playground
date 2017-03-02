package poc.sql.integrity.internal.bigfilter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import poc.commons.time.Stream;
import poc.sql.integrity.internal.generator.FileGenerator;
import poc.sql.integrity.internal.helper.DatasetHelper;
import poc.sql.integrity.internal.helper.FileHelper;
import poc.sql.integrity.internal.helper.SparkSessionInitializer;
import poc.sql.integrity.internal.prop.Prop;
import poc.sql.integrity.internal.prop.Properties_1;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

import static org.apache.spark.sql.functions.col;


/**
 * Created by eyallevy on 08/01/17.
 * <p>
 * Result:
 * =======
 */
public class BigFilterPageBeforeFilter_Map implements Serializable {
    private Prop prop;
    private Stream streamFilter = new Stream();
    private FileHelper fileHelper = new FileHelper();
    private DatasetHelper datasetHelper = new DatasetHelper();

    public BigFilterPageBeforeFilter_Map(Prop prop) {
        this.prop = prop;
    }

    public SparkSession init() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/hadoop_home");
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();

        return sparkSessionInitializer.getSparkSession();
    }

    private void run(SparkSession sc) {
        SQLContext sqlContext = new SQLContext(sc);

        //FileGenerator fileGenerator = new FileGenerator();
        //fileGenerator.generateFilesWithIDS(sqlContext);

        System.out.println("Read src file");
        streamFilter.reset();
        streamFilter.start();
        Dataset<Row> datasetWithId = fileHelper.readCSV(sqlContext, prop.getDataSourcePath());
//        System.out.println("# Of Lines in source file " + datasetWithId.count());
        streamFilter.stop();
        System.err.println("\n\nTime take to read src: " + streamFilter.getDuration() + "\n\n");


        System.out.println("Read ids file");
        streamFilter.reset();
        streamFilter.start();
        Dataset<Row> idsOnly20PrecentDataset = fileHelper.readCSV(sqlContext, prop.getIdsOnlyPath());
//        System.out.println("# Of Lines in ids file " + idsOnly20PrecentDataset.count());
        streamFilter.stop();
        System.err.println("\n\nTime take to read ids: " + streamFilter.getDuration() + "\n\n");

        bigFilter(datasetWithId, idsOnly20PrecentDataset.sort(col(prop.getId())));
    }

    public void bigFilter(Dataset<Row> dataSource, Dataset<Row> idsSorted) {
        boolean hasNextPage = false;
        Long startFrom = 0L;
        int pageNumber = 0;
        int pageSize = 100;
        do {
            streamFilter.reset();

            System.out.println("Collect Ids For page " + (pageNumber + 1) + " start form id " + startFrom);
            Dataset<Row> pageIdsDS = datasetHelper.collectIdsForSpecificPage(idsSorted, startFrom, pageSize, prop.getId(), streamFilter);

            System.out.println("Collect ids as a Map");
            Map<Long, Boolean> pageIdsMap = datasetHelper.collectAsMap(pageIdsDS, prop.getId(), streamFilter);

            if (pageIdsMap.size() > 0) {
                System.out.println("Get the next start from ID - by taking the Max Id");
                startFrom = pageIdsMap.keySet().stream().max(Long::compareTo).orElse(-1L);

                System.out.println("Filter the source DF by the ids Map");
                Dataset<Row> page = datasetHelper.filter(dataSource, pageIdsMap, prop.getId(), streamFilter);
//                statistics(startFrom, pageNumber, pageIdsDS, pageIdsMap, page);

                startFrom++;
                pageNumber++;
                hasNextPage = true;
            } else {
                System.out.println("Page " + (pageNumber + 1) + " is Empty!!!");
                hasNextPage = false;
            }

            System.err.println("\n\nTotal Duration: " + streamFilter.totalDuration + "\n\n");
        }
        while (hasNextPage);

    }

    private void statistics(Long startFrom, int pageNumber, Dataset<Row> pageIdsDS, Map<Long, Boolean> pageIdsMap, Dataset<Row> page) {
        System.out.println("----- STATISTICS -----");
        System.out.println("Max id in page " + (pageNumber + 1) + " is " + startFrom);
        //System.out.println("# Of ids in page " + (pageNumber + 1) + ": " + pageIdsDS.count());
        System.out.println("# Of ids in map " + pageIdsMap.size());
        System.out.println("# Of Filtered Lines in page " + (pageNumber + 1) + ": " + page.count());
    }

    public static void main(String[] args) {
        Prop prop = new Properties_1();
        BigFilterPageBeforeFilter_Map app = new BigFilterPageBeforeFilter_Map(prop);
        SparkSession sparkSession = app.init();
        app.run(sparkSession);
    }
}

