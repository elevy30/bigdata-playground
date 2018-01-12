package poc.elastic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.util.Map;


import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Slf4j
public class IndexerTest {

    @InjectMocks
    private Indexer indexer;

    private JavaSparkContext jsc;
    private SparkSession sparkSession;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        SparkSession.Builder builder = SparkSession.builder()
                // .config("spark.local.dir", dataSourcePath)
                .config("spark.driver.allowMultipleContexts", "true").config("spark.task.maxFailures", "1")
                //.config("spark.sql.warehouse.dir", "file:///" + dataSourcePath + "/spark-warehouse")
                .appName("Elastics").master("local[*]");
        sparkSession = builder.getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
    }

    @After
    public void tearDown() {
    }

    @Test
    @Ignore
    public void saveToEs() {
        Map<String, ?> numbers = ImmutableMap.of("one", 11, "two", 22);
        Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni1", "SFO", "San Fran1");

        JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));

        indexer.saveRDDToEs(javaRDD, "spark/docs");
    }

    @Test
    @Ignore
    public void indexParquet() throws IOException {
        String index = "spark";
        String type = "docs";

        Client client = null;
        try {
            String data = getResourceFilePath("elastic_big");

            Dataset<Row> dataset = sparkSession.read().option("inferSchema", true).parquet(data);
            StructType schema = dataset.schema();

            XContentBuilder xContentBuilder = createXContentBuilder(schema);

            client = indexer.getClient();

            try {
                CreateIndexResponse twitter = indexer.createIndex(client, index, type, xContentBuilder);
                System.out.println(twitter.isAcknowledged());
            } catch (ResourceAlreadyExistsException e) {
                indexer.deleteIndex(client, index);
                CreateIndexResponse twitter = indexer.createIndex(client, index, type, xContentBuilder);
                System.out.println(twitter.isAcknowledged());
            }

//            Broadcast<String[]> broadcast = jsc.broadcast(schema.fieldNames());
//            System.out.println(schema);
//            JavaRDD<Map<Object, String>> rowJavaRDD = dataset.javaRDD().map((Function<Row, Map<Object, String>>) row -> {
//                String[] value = broadcast.getValue();
//                HashMap<Object, String> map = new HashMap<>();
//                for (String fieldName : value) {
//                    map.put(fieldName, row.getAs(fieldName));
//                }
//                return map;
//            });
//            long startTime = System.currentTimeMillis();
//            indexer.saveRDDToEs(dataset, "spark/docs");
//            long endTime = System.currentTimeMillis();

            long startTime = System.currentTimeMillis();
            indexer.saveDatasetToEs(dataset, "spark/docs");
            long endTime = System.currentTimeMillis();


            log.info("######### DURATION #######: {} MS", (endTime - startTime));

            indexer.search(client, index);
        } catch (Exception e) {
            log.error("error msg:", e);
        } finally {
            indexer.closeClient(client);
        }
    }

    private XContentBuilder createXContentBuilder(StructType schema) throws IOException {
        XContentBuilder xcb = jsonBuilder();
        xcb.startObject().startObject("properties");

        xcb = addFields(xcb, schema);

        xcb.endObject().endObject();

        return xcb;
    }

    private XContentBuilder addFields(XContentBuilder xcb, StructType schema) throws IOException {
        StructField[] fields = schema.fields();
        for (StructField field : fields) {
            String index = "false";
            log.info("handel field : {}", field.name());
            if ("tr-sequence".equals(field.name()) || "MONTHS1".equals(field.name()) || "MONTHS2".equals(field.name()) || "NORMALValue1".equals(field.name())) {
                index = "true";
            }
            String elasticsType = getElasticsType(field.dataType());
            xcb.startObject(field.name()).field("type", elasticsType).field("index", index).endObject();
        }
        return xcb;
    }

    private String getElasticsType(DataType dataType) {
        log.info("field type:{}", dataType.typeName());
        String sparkFieldType = dataType.typeName();
        if ("long".equals(sparkFieldType) || "integer".equals(sparkFieldType)) {
            return "long";
        } else {
            return "text";
        }
    }


    private String getResourceFilePath(String path) {
//        String absolutePath = new File(getClass().getResource("/").getFile()).getAbsolutePath();
//        return new File(getClass().getResource("/" + path).getFile()).getAbsolutePath();
        return new File("/opt/tr/data-loader/ds/input/" + path).getAbsolutePath();
    }

    @Test
   // @Ignore
    public void index() throws IOException {
        String index = "rowdb";
        String type = "rowtable";

        Client client = null;
        try {
            client = indexer.getClient();


            try {
                CreateIndexResponse twitter = indexer.createIndexWithMapping(client, index, type);
                System.out.println(twitter.isAcknowledged());
            } catch (ResourceAlreadyExistsException e) {
                indexer.deleteIndex(client, index);
                CreateIndexResponse twitter = indexer.createIndexWithMapping(client, index, type);
                System.out.println(twitter.isAcknowledged());
            }

            String json = getJson();

            indexer.addToIndex(client, index, type, json);

            indexer.search(client, index);
        } catch (Exception e) {
            log.error("error msg:", e);
        } finally {
            indexer.closeClient(client);
        }
    }

    private String getJson() {
        return "{" + "\"name\":\"kimchy\"," + "\"a1\":\"hello world\"," + "\"a2\":\"boy girl\"," + "\"a3\":\"dog cat mouse\"," + "\"a4\":\"today tomorrow\"," + "\"age\":\"25\"," + "\"message\":\"trying out Elasticsearch\"" + "}";
    }

}