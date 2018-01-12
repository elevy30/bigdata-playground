package poc.elastic;

import lombok.extern.slf4j.Slf4j;
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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@SuppressWarnings("ALL")
@Slf4j
public class Traceback {

    @InjectMocks
    private Indexer indexer;
    @InjectMocks
    private Searcher searcher;

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
    public void tracebackIndexingTest() throws IOException {
        Client client = null;
        try {
            client = indexer.getClient();
            indexer.deleteIndex(client, "batch_id");

            loadData(client, "batch_id", "ds_id", "/Data/thetaray/zeppelin/data/traceback_ds");
            loadData(client, "batch_id", "an_id", "/Data/thetaray/zeppelin/data/traceback_an_start_end");
            loadData(client, "batch_id", "an_ds", "/Data/thetaray/zeppelin/data/traceback_an_to_ds");
        } finally {
            indexer.closeClient(client);
        }
    }




    @Test
    @Ignore
    public void tracebackSearchingTest() throws IOException {
        Client client = null;
        try {
            client = indexer.getClient();
            TermQueryBuilder an_row_id = QueryBuilders.termQuery("an_row_id", 60129542145L);
            List<Long> dsList = searcher.searchAnDsList(client, "batch_id", "an_ds", an_row_id);

            TimeWindow searchTimeWindow = searcher.searchTimeWindow(client, "batch_id", "an_id", an_row_id);

            RangeQueryBuilder datetimeRange = QueryBuilders.rangeQuery("datetime").from(searchTimeWindow.beforeStartDate).to(searchTimeWindow.afterEndDate);

            searcher.searchDsInWindow(client, "batch_id", "ds_id", datetimeRange, dsList, 1000);

        } finally {
            indexer.closeClient(client);
        }
    }


    private void loadData(Client client, String index, String type, String path) {
        try {

            Dataset<Row> dataset = sparkSession.read().option("inferSchema", true).parquet(path);
            StructType schema = dataset.schema();

            XContentBuilder xContentBuilder = createXContentBuilder(schema);
            log.info("content builder created");

            try {
                CreateIndexResponse indexerIndex = indexer.createIndex(client, index, type, xContentBuilder);
                log.info("Index Acknowledged:{}", indexerIndex.isAcknowledged());
            } catch (ResourceAlreadyExistsException e) {
//                indexer.deleteIndex(client, index);
//                CreateIndexResponse indexerIndex = indexer.createIndex(client, index, type, xContentBuilder);
                log.warn("Index {} already Exist", index);
            }

            long startTime = System.currentTimeMillis();
            log.info("Starting saveDatasetToEs index:{}, type:{} started time:{}", index, type, (new Date(startTime)));
            indexer.saveDatasetToEs(dataset, index + "/" + type);
            long endTime = System.currentTimeMillis();
            log.info("Ending saveDatasetToEs index:{}, type:{} started time:{}", index, type, (new Date(endTime)));

            log.info("######### DURATION #######: {} MS", (endTime - startTime));

        } catch (Exception e) {
            log.error("Error msg:", e);
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
            if ("ds_row_id".equals(field.name()) || "an_row_id".equals(field.name()) || "before_start_date".equals(field.name()) || "after_start_date".equals(field.name()) || "datetime".equals(field.name())) {
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
        } else if ("timestamp".equals(sparkFieldType)) {
            return "date";
        } else {
            return "text";
        }
    }
}