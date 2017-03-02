package poc.sql.integrity.internal.helper;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import poc.commons.time.Stream;
import scala.Serializable;
import scala.Tuple2;

import java.util.Map;
import java.util.Random;

import static org.apache.spark.sql.functions.col;

/**
 * Created by eyallevy on 27/02/17.
 */
public class DatasetHelper implements Serializable {

    public Dataset<Row> collectIdsForSpecificPage(Dataset<Row> idsSorted, long startFrom, int pageSize, String columnName, Stream stream) {
        stream.start();

        Dataset<Row> page = idsSorted.filter(col(columnName).geq(startFrom)).limit(pageSize);

        stream.stop();
        stream.updateTotal();
        System.err.println("CollectIdsForSpecificPage Duration: " + stream.getDuration());

        return page;
    }

    public Map<Long, Boolean> collectAsMap(Dataset<Row> rowDataset, String columnName, Stream stream) {
        stream.start();

        JavaPairRDD<Long, Boolean> idsMapJavaPairRDD = rowDataset.select(columnName).toJavaRDD().mapToPair(row -> new Tuple2<Long, Boolean>(row.getLong(0), true));
        Map<Long, Boolean> idsMap = idsMapJavaPairRDD.collectAsMap();

        stream.stop();
        stream.updateTotal();
        System.err.println("collectAsMap Duration: " + stream.getDuration());

        return idsMap;
    }

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
        return rowDataset.filter((FilterFunction<Row>) row -> {
                    if (r.nextFloat() <= 0.20F) {
                        return true;
                    } else {
                        return false;
                    }
                }
        ).select(col(columnName));
    }

    public Dataset<Row> filter(Dataset<Row> fullDataset, Map<Long, Boolean> idsMap, String columnName, Stream stream) {
        stream.start();

        Dataset<Row> filtered = fullDataset.filter((FilterFunction<Row>) row -> {
            Boolean aBoolean = idsMap.get(row.getAs(columnName));
            if (aBoolean != null) {
                return true;
            } else {
                return false;
            }
        });
        filtered.show();

        stream.stop();
        stream.updateTotal();
        System.err.println("Filter Duration: " + stream.getDuration());

        return filtered;
    }


}
