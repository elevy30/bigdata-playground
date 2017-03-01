package poc.sql.integrity.bitwise;

import com.google.common.base.Joiner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import poc.sql.integrity.internal.generator.BitwiseGenerator;
import poc.sql.integrity.internal.helper.BitwiseHelper;
import poc.sql.integrity.internal.helper.FileHelper;
import poc.sql.integrity.internal.helper.SparkSessionInitializer;
import poc.sql.integrity.internal.prop.Prop;
import poc.sql.integrity.internal.prop.Properties_2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by eyallevy on 08/01/17.
 */
public class SparkIntegrityBitwiseUsingJoin implements Serializable {

    private Prop prop = new Properties_2();
    private FileHelper fileHelper = new FileHelper();
    private BitwiseHelper bitwiseHelper = new BitwiseHelper(prop);

    int numberOfBitColumn;

    private SparkSession init() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/hadoop_home");
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        return sparkSessionInitializer.getSparkSession();
    }

    private void run(SparkSession sc) {


        SQLContext sqlContext = new SQLContext(sc);
        BitwiseGenerator bitwiseGenerator = new BitwiseGenerator(sc, prop);

        System.out.println("########### read datasource from parquet");
        Dataset<Row> dataSource = fileHelper.readCSV(sqlContext, prop.getDataSourcePath());
        dataSource.show();

        System.out.println("########### build ColumnLocationMapping");
        Map<String, ColumnLocation> columnLocationMap = buildColumnLocationMapping(dataSource);
        System.out.println(columnLocationMap);

        System.out.println("########### create IntegrityDataSet");
        Dataset<Row> integrityDS = createIntegrityDataSet(sc, dataSource);
        integrityDS.show();

        System.out.println("########### convert IntegrityToBitwiseDataSet");
        Dataset<Row> integrityBitwiseDS = convertIntegrityToBitwiseDataSet(sc, integrityDS, columnLocationMap);
        integrityBitwiseDS.show();
        bitwiseHelper.printOnlyRowWithValue(integrityBitwiseDS);

        Dataset<Row> idsDS = bitwiseHelper.getAllIntegrityIdsForSpecificColumn(integrityBitwiseDS, columnLocationMap);
        idsDS.show();

        Dataset<Row> joined = bitwiseHelper.joinIdsWithFullData(dataSource, idsDS, columnLocationMap);
        joined.show();

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

    private Dataset<Row> createIntegrityDataSet(SparkSession sc, Dataset<Row> dataSet) {
        JavaRDD<Row> dataSetRDD = dataSet.toJavaRDD();
        dataSet.printSchema();
        // Convert records of the RDD (people) to Rows/?/??
        JavaRDD<Row> integrityRowRDD = dataSetRDD.map((Function<Row, Row>) record -> {
            List<String> columnsNames = new ArrayList<>();
            if (record.getString(4).equals("not_valid")) {
                columnsNames.add("sc_status");
            }
            if (record.getString(9).equals("not_valid")) {
                columnsNames.add("s_action");
            }
            String columnsNamesJoin = Joiner.on(",").join(columnsNames);

            Long longVal = new Long (record.getInt(record.fieldIndex(prop.getId())));
            return RowFactory.create(longVal, columnsNamesJoin);
        });

        //List<String> items = Arrays.asList(listString.split("\\s*,\\s*"));

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        StructField field1 = DataTypes.createStructField(prop.getId(), DataTypes.LongType, false);
        StructField field2 = DataTypes.createStructField(prop.getInvalidList(), DataTypes.StringType, false);
        fields.add(field1);
        fields.add(field2);
        StructType schema = DataTypes.createStructType(fields);

        return sc.createDataFrame(integrityRowRDD, schema);
    }

    public Dataset<Row> convertIntegrityToBitwiseDataSet(SparkSession sc, Dataset<Row> dataSet, Map<String, ColumnLocation> columnLocationMap) {
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
//???????????????????/
            return RowFactory.create(collectorArray);
        });

        //List<String> items = Arrays.asList(listString.split("\\s*,\\s*"));

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

        return sc.createDataFrame(integrityBitwiseRowRDD, schema);
    }


    public static void main(String[] args) {
        SparkIntegrityBitwiseUsingJoin app = new SparkIntegrityBitwiseUsingJoin();
        SparkSession sparkSession = app.init();
        app.run(sparkSession);
    }
}
