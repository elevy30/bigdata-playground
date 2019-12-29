package poc.sql.integrity.percolumn;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import poc.commons.SparkSessionInitializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;


/**
 * Created by eyallevy on 08/01/17.
 */
public class SparkIntegrityPerColumn implements Serializable {

    private static final String COLUMN_NAME = "columnName";
    private static final String TR_SEQUENCE = "Id";
    private static final String CS_BYTES = "cs_bytes";
    private static final String SC_BYTES = "sc_bytes";
//    private static final String CSV_PATH = "file:///Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/data/proxy_fixed.csv";
//    private static final String PARQUET_PATH = "file:///Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/data/proxy_fixed";

    private static final String CSV_PATH = "file:///opt/Dropbox/dev/poc/_resources/data/proxy_fixed.csv";
    private static final String PARQUET_PATH = "file:///opt/Dropbox/dev/poc/_resources/data/proxy_fixed";

    private SparkSession spark;
    private SQLContext sqlContext;

    public void init() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/01_poc/hadoop_home");
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        this.spark = sparkSessionInitializer.getSparkSession("SparkIntegrityPerColumn");
        this.sqlContext = new SQLContext(spark);
    }

    private void run() {
        System.out.println("###########_read file from parquet");

        convertCSVToParquet(sqlContext);

        Dataset<Row> dataSetParquet = readParquet(sqlContext);
        dataSetParquet.show();

        Dataset<Row> integrityDS = createIntegrityDataSet(dataSetParquet);
        integrityDS.show();

        Dataset<Row> csBytesDataSet = integrityDS.filter((FilterFunction<Row>) record -> record.getAs(COLUMN_NAME).equals(CS_BYTES)).sort(new Column(TR_SEQUENCE));
        csBytesDataSet.show();

//      Dataset<Row> join = csBytesDataSet.join(dataSetParquet, "Id");
        Column joinedColumn = csBytesDataSet.col(TR_SEQUENCE).equalTo(dataSetParquet.col(TR_SEQUENCE));
        Dataset<Row> join = csBytesDataSet.join(dataSetParquet, joinedColumn, "left_outer")
                .drop(csBytesDataSet.col(TR_SEQUENCE))
                .drop(csBytesDataSet.col(COLUMN_NAME));
        join.show();

    }

    private void convertCSVToParquet(SQLContext sqlContext) {
        Dataset<Row> dataSetCSV = sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .csv(CSV_PATH);

        dataSetCSV.write().mode(SaveMode.Overwrite).parquet(PARQUET_PATH);
    }

    private Dataset<Row> readParquet(SQLContext sqlContext) {
        return sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .parquet(PARQUET_PATH);
    }

    private Dataset<Row> createIntegrityDataSet(Dataset<Row> dataSetParquet) {
        Dataset<Row> zero_cs_sc_bytesDS = dataSetParquet.where(col(CS_BYTES).equalTo("0.0").or(col(SC_BYTES).equalTo("0.0")));
        zero_cs_sc_bytesDS.show();
        JavaRDD<Row> zero_cs_sc_bytesRDD = zero_cs_sc_bytesDS.toJavaRDD();

        // Convert records of the RDD (people) to Rows/?/??
        JavaRDD<Row> zero_cs_sc_bytesRowRDD = zero_cs_sc_bytesRDD.map((Function<Row, Row>) record -> {
            if (record.getDouble(7) == 0.0d) {
                return RowFactory.create(CS_BYTES, record.getAs(TR_SEQUENCE));
            } else if (record.getDouble(6) == 0.0d) {
                return RowFactory.create(SC_BYTES, record.getAs(TR_SEQUENCE));
            } else {
                return RowFactory.create("null", 0);
            }
        });

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        StructField field1 = DataTypes.createStructField(COLUMN_NAME, DataTypes.StringType, false);
        StructField field2 = DataTypes.createStructField(TR_SEQUENCE, DataTypes.IntegerType, false);
        fields.add(field1);
        fields.add(field2);
        StructType schema = DataTypes.createStructType(fields);

        return spark.createDataFrame(zero_cs_sc_bytesRowRDD, schema);
    }


    public static void main(String[] args) {
        SparkIntegrityPerColumn app = new SparkIntegrityPerColumn();
        app.init();
        app.run();
    }
}
