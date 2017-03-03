package poc.sql.integrity.internal.helper;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import poc.sql.integrity.bitwise.ColumnLocation;
import poc.sql.integrity.internal.prop.Prop;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by eyallevy on 27/02/17.
 */
public class BitwiseHelper implements Serializable{

    Prop prop;

    public BitwiseHelper(Prop prop) {
        this.prop = prop;
    }

    public Dataset<Row> getAllIntegrityIdsForSpecificColumn(Dataset<Row> integrityBitwiseDS, Map<String, ColumnLocation> columnLocationMap) {
        String bitColumnId = String.valueOf(columnLocationMap.get(prop.getTestedColumn()).bitColumnId);
        int bitLocation = columnLocationMap.get(prop.getTestedColumn()).bitLocation;
        long bitLocationLong = 1L << bitLocation;

        return integrityBitwiseDS.filter((FilterFunction<Row>) record -> {
            long recordLong = record.getAs(bitColumnId);
            return (recordLong & bitLocationLong) != 0;
        }).sort(new Column(prop.getId()));
    }

    public Dataset<Row> joinIdsWithFullData(Dataset<Row> fullData, Dataset<Row> idsDS, Map<String, ColumnLocation> columnLocationMap) {
        String bitColumnId = String.valueOf(columnLocationMap.get(prop.getTestedColumn()).bitColumnId);

        //Dataset<Row> join = csBytesDataSet.join(dataSetParquet, "Id");
        Column joinedColumn = idsDS.col(prop.getId()).equalTo(fullData.col(prop.getId()));

        return idsDS.join(fullData, joinedColumn, "left_outer")
                .drop(idsDS.col(prop.getId()))
                .drop(idsDS.col(bitColumnId));
    }

    public void printOnlyRowWithValue(Dataset<Row> integrityBitwiseDS) {
        JavaRDD<Row> dataSetRDD = integrityBitwiseDS.toJavaRDD();
        JavaRDD<Row> filter = dataSetRDD.filter(record -> (record.getLong(1) != 0L));
        filter.take(20).forEach(System.out::println);
    }








}
