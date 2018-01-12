package poc.sql.dataloader;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import poc.commons.SparkSessionInitializer;
import poc.sql.integrity.internal.helper.FileHelper;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by eyallevy on 05/03/17.
 */
public class RDDToDF_CreateDataFrame {

    private static final int FIELD_COUNT = 1900;

    private Dataset<Row> createBigSchema(SparkSession sparkSession, int startColName, int fieldNumber) {
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(sparkSession.sparkContext());

        String[] row = IntStream.range(startColName, fieldNumber).mapToObj(String::valueOf).toArray(String[]::new);
        List<String[]> data = Collections.singletonList(row);
        JavaRDD<Row> rdd = jsc.parallelize(data).map(RowFactory::create);


        //StructField id = DataTypes.createStructField("ID", DataTypes.LongType, false);

        StructField[] structFields = IntStream.range(startColName, fieldNumber)
                .mapToObj(i -> new StructField(String.valueOf(i), DataTypes.StringType, true, Metadata.empty()))
                .toArray(StructField[]::new);
        StructType schema = DataTypes.createStructType(structFields);

        Dataset<Row> dataSet = sqlContext.createDataFrame(rdd, schema);
        dataSet.show();
        return dataSet;
    }

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
//

    public void rddConvertor(SparkSession sparkSession) {
        FileHelper fileHelper = new FileHelper();
        SQLContext sqlContext = new SQLContext(sparkSession);
        Dataset<Row> dataSource = fileHelper.readCSV(sqlContext, System.getProperty("user.dir") + "/_resources/data/dataloader/ds1.csv");

//        CSVParser parser = new CSVParser();
        JavaPairRDD<Row, Long> rowLongJavaPairRDD = dataSource.toJavaRDD().zipWithIndex();
        JavaRDD<String> rddWithIndex = rowLongJavaPairRDD.map(t -> {
            String line = t._1().toString();
            StringBuilder builder = new StringBuilder(line);
//            builder.deleteCharAt(0);
//            builder.deleteCharAt(builder.length() - 1);
            return builder.toString();
//            String[] fields = parser.parseLine(builder.toString());
//            return t._2() + "," + Arrays.toString(fields);
        });


        System.out.println(rddWithIndex.collect());
    }

    public static void main(String[] args) {
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        SparkSession sparkSession = sparkSessionInitializer.getSparkSession("RDDToDF_CreateDataFrame");

        RDDToDF_CreateDataFrame rddToDFCreateDataFrame = new RDDToDF_CreateDataFrame();
        rddToDFCreateDataFrame.createBigSchema(sparkSession, 0, FIELD_COUNT);
//        rddToDFCreateDataFrame.rddConvertor(sparkSession);
    }


}

