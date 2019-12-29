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
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.spark.sql.functions.*;

@Slf4j
public class SparkJavaStreamSqlTest {

    public static void main(String[] args) {
        try {
            if (args.length != 2 ) {
                log.info("Usage: SparkJavaStreamTest <input_dir> <checkpoint_dir>");
                System.exit(-1);
            }

            SparkSession spark = SparkSession.builder()
                    .config("spark.sql.shuffle.partitions", 20)
                    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
                    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "elasticpassword")
                    .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
                    .config(ConfigurationOptions.ES_PORT, "9200")
                    .appName("Streaming112")
                    .master("local[*]")
                    .getOrCreate();

            startQuery(args[0], args[1], spark);

            System.out.println("Waiting...");
            while (true) {
                StreamingQuery[] active = spark.streams().active();
                //todo -  check this query object - the triggering flag
//                if(active.length > 0) {
//                    StreamingQuery streamingQuery = active[0];
//                    streamingQuery.stop();
//                }else{
//                    startQuery(args[0], args[1], spark);
//                }
                Thread.sleep(10000);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startQuery(String inputDir, String checkpointDir, SparkSession spark) throws IOException, URISyntaxException, org.apache.spark.sql.streaming.StreamingQueryException {
        FileSystem fs = FileSystem.get(new URI(inputDir), new Configuration());
        Path inputDirPath = new Path(inputDir);
        fs.delete(inputDirPath, true);
        fs.mkdirs(inputDirPath);
        log.info("Using Input Dir dir: {}", inputDirPath);

        Path checkpointDirPath = new Path(checkpointDir);
        fs.delete(checkpointDirPath, true);
        log.info("Using checkpoint dir: {}", checkpointDirPath);

        /////// ON MESSGAE ///////
        StructType schema = new StructType(new StructField[]{
                 //new StructField("batch", DataTypes.StringType, false, Metadata.empty())
                 new StructField("datetime", DataTypes.TimestampType, false, Metadata.empty())
                ,new StructField("s_action", DataTypes.StringType, false, Metadata.empty())
                ,new StructField("sc_bytes", DataTypes.LongType, false, Metadata.empty())
        });


        /////////////////////////   Read stream   ////////////////////////
        Dataset<Row> file = spark
                .readStream()
//                .read()
                .schema(schema)
                .csv(inputDirPath.toString())
        .withWatermark("datetime", "1 seconds");
        //*************************************************************************************



        /////////////////////////////////  Aggregate data //////////////////////////////
        Dataset<Row> grouped = file
                .withWatermark("datetime", "1 minutes")
                //.groupBy(window(col("time"), "10 minutes", "10 minutes"), col("key"))
                .groupBy(window(col("datetime"), "1 months", "1 days"), col("s_action"))
                .agg(
                        sum(col("sc_bytes")).as("sum"),
                        count("*").as("count")
                );
        //*************************************************************************************



        /////////////////////////////////  Aggregate SQL //////////////////////////////
        file.createOrReplaceTempView("ds");
        //Dataset<Row> grouped = spark.sql("select  hour(datetime) as hourOfDay, window(datetime, '1 minutes') as window, s_action, count(*) as count, sum(sc_bytes) as sum_sc from ds group by hour(datetime), window(datetime, '1 minutes'), s_action");
        grouped.printSchema();
        log.info("Using checkpoint dir: {}", checkpointDirPath);
        grouped.show(false);
        //*************************************************************************************



        /////////////////////////   Write stream   ////////////////////////
        DataStreamWriter<Row> rowDataStreamWriter = grouped.writeStream();
        //complete - will send out --- ALL ---  Aggregated window
        //append   - will send out --- ONLY --- the closed window
        //update   - will send out --- NEW WIN ONLY  ---
        DataStreamWriter<Row> streamWriter = rowDataStreamWriter.outputMode("complete");
        StreamingQuery query =  streamWriter
                .format("console")
                .option("checkpointLocation", checkpointDirPath.toString())
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .start();
        //*************************************************************************************



        query.awaitTermination(1);
        //Add query to cache
        /////// ON END MESSGAE ///////
    }

}
