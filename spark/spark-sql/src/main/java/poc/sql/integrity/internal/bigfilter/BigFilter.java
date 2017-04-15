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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;


/**
 * Created by eyallevy on 08/01/17.
 *
 * Result:
 * =======
 * convert Ids to Map With PairRDD/RDD
 * ToMap Duration 35981/39202
 * Filter the source DF by the ids Map
 * Filter Duration 135
 * Paging - take 1000 starting from 1000 (sort before filtering filterPage alreadySorted=true)
 * Paging Duration 43954
 *
 * convert Ids to Map With PairRDD/RDD
 * ToMap Duration 43243/37682
 * Filter the source DF by the ids Map
 * Filter Duration 78
 * Paging - take 1000 starting from 1000 (not sort before filtering filterPage alreadySorted=false)
 * Paging Duration 24335
 *
 * convert Ids to Map With PairRDD/RDD
 * ToMap Duration 36088/38754
 * Filter the source DF by the ids Map
 * Filter Duration 77
 * Paging - take 1000 starting from 1000 (not sort before filtering filterPage alreadySorted=true)
 * Paging Duration 1326
 *
 */
public class BigFilter implements Serializable {
    private Prop prop = new Properties_1();
    private FileHelper fileHelper = new FileHelper();
    private DatasetHelper datasetHelper = new DatasetHelper();

    private void run(SparkSession sc) {
        StreamTimer streamTimerFilter = new StreamTimer();

        SQLContext sqlContext = new SQLContext(sc);

        System.out.println("Read dataSource from CSV file");
        Dataset<Row> dataSource = fileHelper.readCSV(sqlContext, prop.getDataSourceIdPath());

        System.out.println("Filter 20% of the data into new DF and get only the IDs");
        Dataset<Row> idsOnly20PrecentDataset = datasetHelper.filter20Precent(dataSource, prop.getId());
        idsOnly20PrecentDataset.sort(col(prop.getId())).show();

        System.out.println("convert Ids to Map");
        Map<Long, Boolean> idsMap = datasetHelper.collectAsMap(idsOnly20PrecentDataset, prop.getId(), streamTimerFilter);

        System.out.println("convert Ids to Map");
        streamTimerFilter.start();
        Map<Long, Boolean> idsMapWithJava = convertToMap(idsOnly20PrecentDataset);
        streamTimerFilter.stop();
        System.out.println("ToMapJava Duration " + streamTimerFilter.getDuration());

        boolean equals = idsMap.equals(idsMapWithJava);
        System.out.println("The map are " + (equals ? "EQUALS" : "NOT EQUALS"));

        System.out.println("Filter the source DF by the ids Map");
        streamTimerFilter.start();
//        Dataset<Row> joined = filterByMap(datasetWithId, idsMap).sort(TR_ID);
        Dataset<Row> joined = filterByMap(dataSource, idsMap);
        streamTimerFilter.stop();
        System.out.println("Filter Duration " + streamTimerFilter.getDuration());

        System.out.println("Paging - take 1000 starting from 1000");
        streamTimerFilter.start();
        Dataset<Row> page = filterPage(joined, 1000, 1000, true, true);
        page.show();
        streamTimerFilter.stop();
        System.out.println("Paging Duration " + streamTimerFilter.getDuration());
    }

    private Map<Long, Boolean> convertToMap(Dataset<Row> rowDataset) {
        Map<Long, Boolean> idsMap = new HashMap<>();
        List<Long> collect = rowDataset.select(prop.getId()).toJavaRDD().map(row -> row.getLong(0)).collect();
        for (Long longVal:collect) {
            idsMap.put(longVal,true);
        }
        return idsMap;
    }

    private Dataset<Row> filterByMap(Dataset<Row> fullDataset, Map<Long, Boolean> idsMap) {
        return fullDataset.filter((FilterFunction<Row>) row -> {
            Long id = row.getAs(prop.getId());
            Boolean aBoolean = idsMap.get(id);
            return aBoolean != null;
        });
    }

    @SuppressWarnings("SameParameterValue")
    private Dataset<Row> filterPage(Dataset<Row> df, int skip, int limit, boolean isFiltered, boolean alreadySorted) {

        Dataset<Row> skipped = df.filter(col(prop.getId()).geq(skip));
        if (!isFiltered) {
            skipped = skipped.filter(col(prop.getId()).lt(skip + limit));
        }
        Dataset<Row> limited = skipped.limit(limit);
        if (!alreadySorted) {
            limited = limited.sort(prop.getId());
        }
        return limited;
    }


    public static void main(String[] args) {
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        SparkSession sparkSession = sparkSessionInitializer.getSparkSession();

        BigFilter app = new BigFilter();
        app.run(sparkSession);
    }
}

