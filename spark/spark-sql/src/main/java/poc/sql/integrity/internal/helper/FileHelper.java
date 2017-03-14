package poc.sql.integrity.internal.helper;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 * Created by eyallevy on 27/02/17.
 */
public class FileHelper implements Serializable{


    public JavaRDD<String> readFile(JavaSparkContext sparkContext, String filePath) {
        return sparkContext.textFile(filePath);
    }

    public Dataset<Row> readCSV(SQLContext sqlContext, String csvPath) {
        return sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .csv(csvPath);
    }

    public Dataset<Row> readCSV(SQLContext sqlContext, String csvPath, StructType schema) {
        return sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .schema(schema)
                .csv(csvPath);
    }

    public void writeCSV(Dataset<Row> idsDataset, String filePath) {
        idsDataset.coalesce(1)
                .write()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .csv(filePath);
    }

    @SuppressWarnings("unused")
    public void convertCSVToParquet(SQLContext sqlContext, String filePath) {
        Dataset<Row> dataSetCSV = sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .csv(filePath);

        dataSetCSV.write().mode(SaveMode.Overwrite).parquet(filePath);
    }

    @SuppressWarnings("unused")
    public Dataset<Row> readParquet(SQLContext sqlContext, String filePath) {
        return sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .parquet(filePath);
    }

    @SuppressWarnings("unused")
    private void convertCSVToParquet(SQLContext sqlContext, String csvPath, String parquetPath) {
        Dataset<Row> dataSetCSV = sqlContext.read()
                .option("header", true)
                .option("sep", ",")
                .option("inferSchema", "true")
                .csv(csvPath);

        dataSetCSV.write().mode(SaveMode.Overwrite).parquet(parquetPath);
    }



}
