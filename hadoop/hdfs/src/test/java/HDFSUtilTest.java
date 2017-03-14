import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import poc.HDFSUtil;

import java.util.ArrayList;
import java.util.Arrays;

import static com.sun.xml.internal.ws.dump.LoggingDumpTube.Position.Before;

/**
 * Created by eyallevy on 28/02/17
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {HDFSUtilTest.Config.class, HDFSUtil.class})
@TestPropertySource(properties = {
        "hdfs.root.path=.",
        "name.node.url=file:///"
})
public class HDFSUtilTest {
    private JavaSparkContext jsc;

    @Autowired
    private HDFSUtil hdfsUtil;

    @Configuration
    static class Config {
        @Bean
        public static PropertySourcesPlaceholderConfigurer propertiesResolver() {
            return new PropertySourcesPlaceholderConfigurer();
        }
    }

    @Before
    public void before() throws Exception {
        setInfra();
        System.setProperty("user.dir", this.getClass().getResource(".").getPath());
        LogManager.getLogger(HDFSUtil.class).setLevel(Level.DEBUG);
    }

    private void setInfra() {
        // Set Up Spark Context
        SparkConf conf = new SparkConf().setAppName("hdfs-util")
                .setMaster("local[2]")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.task.maxFailures", "1");
        this.jsc = new JavaSparkContext(conf);
    }

    @After
    public void tearDown() {
        if (jsc != null) {
            jsc.stop();
            jsc = null;
        }
    }


    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @Test
    //@Repeat( times = 10000 )
    public void saveClusteringModelSchema() throws Exception {
        String analysisId = "analysis_1";
        try {
            ArrayList<String> clusteringModelOrder = new ArrayList<String>(Arrays.asList("1", "2", "3", "4"));
            hdfsUtil.saveClusteringModelSchema(jsc, analysisId, clusteringModelOrder);

            String clusteringModelSchemaPath = hdfsUtil.getClusteringModelSchemaPath(analysisId);
            JavaRDD<ArrayList<String>> objectJavaRDD = jsc.objectFile(clusteringModelSchemaPath);
            ArrayList<String> featuresOrder = objectJavaRDD.first();
            System.out.println("Expected : "+clusteringModelOrder);
            System.out.println("Actual : "+featuresOrder);
            Assert.assertThat(featuresOrder, IsIterableContainingInOrder.contains(clusteringModelOrder.toArray()));
        } finally {
            hdfsUtil.deletePath(analysisId);
        }
    }

}