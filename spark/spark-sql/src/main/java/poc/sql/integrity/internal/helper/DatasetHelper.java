package poc.sql.integrity.internal.helper;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import poc.commons.time.StreamTimer;
import scala.Serializable;
import scala.Tuple2;

import java.util.Map;
import java.util.Random;

import static org.apache.spark.sql.functions.col;

/**
 * Created by eyallevy on 27/02/17.
 */
public class DatasetHelper implements Serializable {

    public Dataset<Row> collectIdsForSpecificPage(Dataset<Row> idsSorted, long startFrom, int pageSize, String columnName, StreamTimer streamTimer) {
        streamTimer.start();

        Dataset<Row> page = idsSorted.filter(col(columnName).geq(startFrom)).limit(pageSize);

        streamTimer.stop();
        streamTimer.updateTotal();
        System.err.println("CollectIdsForSpecificPage Duration: " + streamTimer.getDuration());

        return page;
    }


    public JavaPairRDD<Long, Boolean> convertToPairRDD(Dataset<Row> rowDataset, String columnName) {
        return rowDataset.select(columnName)
                .toJavaRDD()
                .mapToPair((PairFunction<Row, Long, Boolean>) row -> new Tuple2<Long, Boolean>(row.getLong(0), true));
    }

    public Map<Long, Boolean> collectAsMap(Dataset<Row> rowDataset, String columnName, StreamTimer streamTimer) {
        streamTimer.start();

        JavaPairRDD<Long, Boolean> mapJavaPairRDD = convertToPairRDD(rowDataset, columnName);
        Map<Long, Boolean> map = mapJavaPairRDD.collectAsMap();

        streamTimer.stop();
        streamTimer.updateTotal();
        System.err.println("collectAsMap Duration: " + streamTimer.getDuration());

        return map;
    }

    @SuppressWarnings("SameParameterValue")
    public Dataset<Row> readPage(Dataset<Row> df, int skip, int limit, boolean isFiltered, boolean alreadySorted, String columnName) {
        Dataset<Row> skipped = df.filter(col(columnName).geq(skip));
        if (!isFiltered) {
            skipped = skipped.filter(col(columnName).lt(skip + limit));
        }
        Dataset<Row> limited = skipped.limit(limit);
        if (!alreadySorted) {
            limited = limited.sort(columnName);
        }
        return limited;
    }

    public Dataset<Row> filter20Precent(Dataset<Row> rowDataset, String columnName) {
        Random r = new Random();
        return rowDataset.filter((FilterFunction<Row>) row -> r.nextFloat() <= 0.20F).select(col(columnName));
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    public Dataset<Row> filter(Dataset<Row> fullDataset, Map<Long, Boolean> idsMap, String columnName, StreamTimer streamTimer) {
        streamTimer.start();

        Dataset<Row> filtered = fullDataset.filter((FilterFunction<Row>) row -> idsMap.get(row.getAs(columnName)) != null);
        filtered.show();

        streamTimer.stop();
        streamTimer.updateTotal();
        System.err.println("Filter Duration: " + streamTimer.getDuration());

        return filtered;
    }


}
