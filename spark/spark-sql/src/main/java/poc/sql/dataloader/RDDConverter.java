package poc.sql.dataloader;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import poc.commons.SparkSessionInitializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by eyallevy on 05/03/17.
 */
public class RDDConverter {

    private static final int FIELD_COUNT = 3000;

    private Dataset<Row> createBigSchema(SparkSession sparkSession , int startColName, int fieldNumber) {
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(sparkSession.sparkContext());

        String[] row = IntStream.range(startColName, fieldNumber).mapToObj(String::valueOf).toArray(String[]::new);
        List<String[]> data = Collections.singletonList(row);
        JavaRDD<Row> rdd = jsc.parallelize(data).map(RowFactory::create);

        //        StructField id = DataTypes.createStructField("ID", DataTypes.LongType, false);

        StructField[] structFields = IntStream.range(startColName, fieldNumber)
                .mapToObj(i -> new StructField(String.valueOf(i), DataTypes.StringType, true, Metadata.empty()))
                .toArray(StructField[]::new);
        StructType schema = DataTypes.createStructType(structFields);

        Dataset<Row> dataSet = sqlContext.createDataFrame(rdd.collect(), schema);
        dataSet.show();
        return dataSet;
    }

//
//    private Dataset<Row> convertDataset(SparkSession sparkSession , int startColName, int fieldNumber) {
//        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
//        SQLContext sqlContext = new SQLContext(sparkSession.sparkContext());
//
//        String[] row = IntStream.range(startColName, fieldNumber).mapToObj(String::valueOf).toArray(String[]::new);
//        List<String> data = Collections.singletonList(Arrays.toString(row));
//        JavaRDD<Row> rdd = jsc.parallelize(data).map(RowFactory::create);
//
//        StructField oneColSchema = DataTypes.createStructField("array", DataTypes.StringType, false);
//
//        Dataset<Row> dataSet = sqlContext.createDataFrame(data, oneColSchema);
//        dataSet.show();
//        return dataSet;
//    }

    private void unionBigSchema(SparkSession sparkSession) {
        Dataset<Row> bigSchema1 = createBigSchema(sparkSession, 0, 901);
        Dataset<Row> bigSchema2 = createBigSchema(sparkSession, 900, 1800);
        Column joinedColumn = bigSchema1.col("900").equalTo(bigSchema2.col("900"));
        Dataset<Row> join = bigSchema1.join(bigSchema2, joinedColumn);
        join.show();

    }


    public static void main(String[] args) {
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        SparkSession sparkSession = sparkSessionInitializer.init();



        RDDConverter rddConverter = new RDDConverter();
        rddConverter.createBigSchema(sparkSession, 0, FIELD_COUNT);
//        rddConverter.unionBigSchema(sparkSession);
    }


}

