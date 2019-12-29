package poc.spark.protobuf;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import poc.commons.SparkSessionInitializer;
import poc.helpers.FileHelper;

@Slf4j
public class SparkToProtobuf {

    public static void main(String[] args) {
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        SparkSession sparkSession = sparkSessionInitializer.getSparkSession("SparkToProtobuf");


        FileHelper fileHelper = new FileHelper();
        SQLContext sqlContext = new SQLContext(sparkSession);
        Dataset<Row> dataset = fileHelper.readParquet(sqlContext, System.getProperty("user.dir") + "/_resources/bigdata/tr_batch");

        dataset.show(false);

    }



}
