package poc.sql.integrity.internal.generator;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import poc.sql.integrity.bitwise.ColumnLocation;
import poc.sql.integrity.internal.helper.FileHelper;
import poc.sql.integrity.internal.prop.Prop;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.convert.Decorators;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.*;

/**
 * Created by eyallevy on 27/02/17.
 */
public class BitwiseGenerator implements Serializable{

    private Prop prop;
    private FileHelper fileHelper;
    private SparkSession sparkSession;

    private int numberOfBitColumn;

    public BitwiseGenerator(SparkSession sparkSession, Prop prop) {
        this.prop = prop;
        this.sparkSession = sparkSession;
        this.fileHelper = new FileHelper();
    }

    public void generateDummyIntegrityBitwise() {
        SQLContext sqlContext = new SQLContext(sparkSession);

        Dataset<Row> dataSource = fileHelper.readCSV(sqlContext, prop.getDataSourceIdPath());

        System.out.println("Generate ColumnLocationMapping");
        Map<String, ColumnLocation> columnLocationMapWrite = buildColumnLocationMapping(dataSource);
        writeColumnLocationToFile(columnLocationMapWrite);

        //validateColumnLocation(columnLocationMapWrite);

        System.out.println("Generate IntegrityBitwiseDS");
        Dataset<Row> integrityDS = createIntegrityDataSet(dataSource);
        Dataset<Row> integrityBitwiseDS = convertIntegrityToBitwiseDataSet(integrityDS, columnLocationMapWrite);

        System.out.println("Show IntegrityBitwiseDS");
        integrityBitwiseDS.show();

        fileHelper.writeCSV(integrityBitwiseDS, prop.getBitwisePath());
    }

    @SuppressWarnings("unused")
    private void validateColumnLocation(Map<String, ColumnLocation> columnLocationMapWrite) {
        Map<String, ColumnLocation> columnLocationMapRead = readColumnLocationFromFile();

        System.out.println(columnLocationMapWrite.equals(columnLocationMapRead) ? "OK OK OK OK OK" : "FAIL FAIL");

        System.out.println(columnLocationMapWrite);
        System.out.println(columnLocationMapRead);
    }

    private void writeColumnLocationToFile(Map<String, ColumnLocation> columnLocationMap) {
        SparkContext sparkContext = sparkSession.sparkContext();
        Decorators.AsScala<scala.collection.mutable.Map<String, ColumnLocation>> mapAsScala = JavaConverters.mapAsScalaMapConverter(columnLocationMap);
        scala.collection.Seq<Tuple2<String, ColumnLocation>> tuple2Seq = mapAsScala.asScala().toSeq();
        Tuple2<String, ColumnLocation> tuple2 = new Tuple2<>(null, null);
        ClassTag<Tuple2<String, ColumnLocation>> classTag = ClassTag$.MODULE$.apply(tuple2.getClass());
        RDD<Tuple2<String, ColumnLocation>> tuple2RDDWrite = sparkContext.parallelize(tuple2Seq, 1, classTag);
        tuple2RDDWrite.saveAsObjectFile(prop.getColumnLocationMapPath());
    }

    public Map<String, ColumnLocation> readColumnLocationFromFile() {
        SparkContext sparkContext = sparkSession.sparkContext();
        Map<String, ColumnLocation> columnLocationMapRead = new HashMap<>();
        Tuple2<String, ColumnLocation> tuple2 = new Tuple2<>(null, null);
        // Decorators.AsScala<scala.collection.mutable.Map<String, ColumnLocation>> mapAsScala = JavaConverters.mapAsScalaMapConverter(columnLocationMapRead);
        ClassTag<Tuple2<String, ColumnLocation>> classTag = ClassTag$.MODULE$.apply(tuple2.getClass());
        RDD<Tuple2<String, ColumnLocation>> tuple2RDDRead = sparkContext.objectFile(prop.getColumnLocationMapPath(), 1, classTag);
        @SuppressWarnings("RedundantCast")
        Tuple2<String, ColumnLocation>[] tuple2s = (Tuple2<String, ColumnLocation>[]) tuple2RDDRead.collect();
        for (Tuple2<String, ColumnLocation> tuple21 : tuple2s) {
            columnLocationMapRead.put(tuple21._1(), tuple21._2());
        }
        return columnLocationMapRead;
    }

    public Map<String, ColumnLocation> buildColumnLocationMapping(Dataset<Row> dataSource) {
        Map<String, ColumnLocation> columnLocationMap = new HashMap<>();
        String[] columns = dataSource.columns();
        int columnId = 0;
        int location = 0;
        for (String colName : columns) {
            columnLocationMap.put(colName, new ColumnLocation(columnId, location));
            location++;
            if (location % 64 == 0) {
                location = 0;
                columnId++;
            }
        }
        numberOfBitColumn = columnId + 1;
        return columnLocationMap;

    }

    @SuppressWarnings("unchecked")
    private Dataset<Row> createIntegrityDataSet(Dataset<Row> dataSource) {
        Random r = new Random();

        JavaRDD<Row> dataSetRDD = dataSource.toJavaRDD();
        JavaRDD<Row> integrityRowRDD = dataSetRDD.map((Function<Row, Row>) record -> {
            if (r.nextFloat() <= 0.20F) {
                return RowFactory.create(record.getAs(prop.getId()), prop.getTestedColumn());
            } else {
                return RowFactory.create(record.getAs(prop.getId()), "");
            }
        });

        integrityRowRDD = integrityRowRDD.filter(row -> (row.getString(1).length() != 0));

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        StructField field1 = DataTypes.createStructField(prop.getId(), DataTypes.LongType, false);
        StructField field2 = DataTypes.createStructField(prop.getInvalidList(), DataTypes.StringType, false);
        fields.add(field1);
        fields.add(field2);
        StructType schema = DataTypes.createStructType(fields);

        return sparkSession.createDataFrame(integrityRowRDD, schema);
    }

    public Dataset<Row> convertIntegrityToBitwiseDataSet(Dataset<Row> dataSet, Map<String, ColumnLocation> columnLocationMap) {
        JavaRDD<Row> dataSetRDD = dataSet.toJavaRDD();

        Long[] collectorArray = new Long[numberOfBitColumn + 1];
        Arrays.fill(collectorArray, 0L);

        JavaRDD<Row> integrityBitwiseRowRDD = dataSetRDD.map((Function<Row, Row>) record -> {
            Arrays.fill(collectorArray, 0L);

            collectorArray[0] = record.getLong(0);

            List<String> mismatchColumns = Arrays.asList(record.getString(1).split("\\s*,\\s*"));
            if (!mismatchColumns.isEmpty()) {
                for (String columnName : mismatchColumns) {
                    if (columnName.length() > 0) {
                        ColumnLocation columnLocation = columnLocationMap.get(columnName);
                        int bitColumnId = columnLocation.bitColumnId;
                        int bitLocation = columnLocation.bitLocation;
                        Long collector = collectorArray[bitColumnId + 1];
                        collector = collector + (1L << bitLocation);
                        collectorArray[bitColumnId + 1] = collector;
                    }
                }
            }
            //TODO Warning - If I casting to Object[] (like the warning asked) - Spark change the value type to BigInt
            //noinspection ConfusingArgumentToVarargsMethod
            return RowFactory.create(collectorArray);
        });

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        StructField idField = DataTypes.createStructField(prop.getId(), DataTypes.LongType, false);
        fields.add(idField);
        for (int i = 0; i < numberOfBitColumn; i++) {
            StructField bitColumnField = DataTypes.createStructField(String.valueOf(i), DataTypes.LongType, false);
            fields.add(bitColumnField);
        }
        StructType schema = DataTypes.createStructType(fields);
        System.out.println("IntegrityToBitwiseDataSet schema\n" + schema);

        return sparkSession.createDataFrame(integrityBitwiseRowRDD, schema);
    }

}