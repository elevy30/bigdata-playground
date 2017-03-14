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
import poc.commons.SparkSessionInitializer;
import poc.sql.integrity.internal.prop.Prop;
import poc.sql.integrity.internal.prop.Properties_2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by eyallevy on 08/01/17.
 */
public class SparkIntegrityBitwiseUsingJoin implements Serializable {

    private SparkSession sparkSession;
    private SQLContext sqlContext;

    private Prop prop = new Properties_2();
    private FileHelper fileHelper;
    private BitwiseHelper bitwiseHelper;
    private BitwiseGenerator bitwiseGenerator;

//    private int numberOfBitColumn;

    private void init() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/hadoop_home");

        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        this.sparkSession = sparkSessionInitializer.getSparkSession();
        this.sqlContext = new SQLContext(sparkSession);

        this.prop = new Properties_2();
        this.fileHelper = new FileHelper();
        this.bitwiseHelper = new BitwiseHelper(prop);
        this.bitwiseGenerator = new BitwiseGenerator(sparkSession, prop);
    }

    private void run() {

        System.out.println("########### read dataSource from parquet");
        Dataset<Row> dataSource = fileHelper.readCSV(sqlContext, prop.getDataSourceIdPath());
        dataSource.show();

        System.out.println("########### build ColumnLocationMapping");
        Map<String, ColumnLocation> columnLocationMap = bitwiseGenerator.buildColumnLocationMapping(dataSource);
        System.out.println(columnLocationMap);

        System.out.println("########### create IntegrityDataSet");
        Dataset<Row> integrityDS = createIntegrityDataSet(sparkSession, dataSource);
        integrityDS.show();

        System.out.println("########### convert IntegrityToBitwiseDataSet");
        Dataset<Row> integrityBitwiseDS = bitwiseGenerator.convertIntegrityToBitwiseDataSet(integrityDS, columnLocationMap);
        integrityBitwiseDS.show();
        bitwiseHelper.printOnlyRowWithValue(integrityBitwiseDS);

        Dataset<Row> idsDS = bitwiseHelper.getAllIntegrityIdsForSpecificColumn(integrityBitwiseDS, columnLocationMap);
        idsDS.show();

        Dataset<Row> joined = bitwiseHelper.joinIdsWithFullData(dataSource, idsDS, columnLocationMap);
        joined.show();
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

            Long longVal = (long) record.getInt(record.fieldIndex(prop.getId()));
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

    public static void main(String[] args) {
        SparkIntegrityBitwiseUsingJoin app = new SparkIntegrityBitwiseUsingJoin();
        app.init();
        app.run();
    }
}
