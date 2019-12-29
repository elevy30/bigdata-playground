package poc.spark.streaming;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

import java.net.URI;

import static org.apache.spark.sql.functions.*;

@Slf4j
public class SparkJavaStreamStreamManagement {

    public static void main(String[] args) {
        try {

            SparkSession spark = SparkSession.builder()
                    .config("spark.sql.shuffle.partitions", 20)
                    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
                    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "elasticpassword")
                    .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
                    .config(ConfigurationOptions.ES_PORT, "9200")
                    .appName("Streaming112")
                    .master("local[*]")
                    .getOrCreate();



        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
