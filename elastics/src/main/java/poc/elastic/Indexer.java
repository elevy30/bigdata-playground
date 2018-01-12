package poc.elastic;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by eyallevy on 11/08/17
 */
@Slf4j
public class Indexer {

    private static final String HOST = "localhost";
    private static final int PORT = 9300;

    void saveDatasetToEs(Dataset<Row> dataset, String docName) {
        JavaEsSparkSQL.saveToEs(dataset, docName);//);
    }

    void saveRDDToEs(JavaRDD javaRDD, String docName) {
        JavaEsSpark.saveToEs(javaRDD, docName);//);
    }

     Client getClient() throws UnknownHostException {
        Settings settings = settings();

        //fix exception  --- availableProcessors is already set to [8], rejecting [8]
        System.setProperty("es.set.netty.runtime.available.processors", "false");

        PreBuiltTransportClient preBuiltTransportClient = new PreBuiltTransportClient(settings);
        return preBuiltTransportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(HOST), PORT));
    }

    void closeClient(Client client){
        if(client != null) {
            client.close();
        }
    }

    IndexResponse addToIndex(Client client, String index, String type, String json) throws IOException {
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type, "1");

        Map<String, Object> map = JsonToMap(json);
        IndexResponse response = indexRequestBuilder.setSource(map).get();
        log.debug(response.toString());

        return response;
    }

    void deleteIndex(Client client, String index){
        client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    }


    private Settings settings() {
        try {
            final Path tmpDir = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "elasticsearch_data");
            log.debug("TMP folder {}",tmpDir.toAbsolutePath().toString());

            return Settings.builder()
                    //.put("cluster.name", "mynewclustername")
                   // .put("http.enabled", "false")
                    //.put("path.data", tmpDir.toAbsolutePath().toString())
                    .build();

        } catch (final IOException e) {
            log.error("Cannot create temp dir", e);
            throw new RuntimeException();
        }
    }

    CreateIndexResponse createIndex(Client client, String index) {
       return client
               .admin()
               .indices()
               .prepareCreate(index)
               .setSettings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 2))
               .get();
    }

    CreateIndexResponse createIndex(Client client, String index, String type, XContentBuilder xcb) throws IOException {
        return client.admin().indices().prepareCreate(index).addMapping(type, xcb).execute().actionGet();
    }

    CreateIndexResponse createIndexWithMapping(Client client, String index, String type) throws IOException {
        XContentBuilder xcb = jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("name").field("type", "text").endObject()
                        .startObject("a1").field("type", "text").field("index", "true").endObject()
                        .startObject("a2").field("type", "text").field("index", "false").endObject()
                        .startObject("a3").field("type", "text").field("index", "false").endObject()
                        .startObject("a4").field("type", "text").field("index", "false").endObject()
                        .startObject("age").field("type", "long").field("index", "false").endObject()
                    .endObject()
                .endObject();
        return client.admin().indices().prepareCreate(index).addMapping(type, xcb).execute().actionGet();
    }

    void search(Client client, String index){
            SearchResponse response = client.prepareSearch(index).execute().actionGet();
            SearchHit[] results = response.getHits().getHits();
            for (SearchHit hit : results) {
                log.debug("------------------------------");
                Map<String, Object> result = hit.getSourceAsMap();
                log.debug("result:{}", result);
            }
    }


    private Map<String, Object> JsonToMap(String json) {
        Map<String, Object> map = null;
        try {
            ObjectMapper mapper = new ObjectMapper();

            // convert JSON string to Map
            map = mapper.readValue(json, new TypeReference<Map<String, String>>() {});

            log.debug(map.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

}