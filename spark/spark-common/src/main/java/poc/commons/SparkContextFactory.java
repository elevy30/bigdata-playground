package poc.commons;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.URISyntaxException;
import java.net.URL;

public class SparkContextFactory {

    @Autowired
    SparkConfiguration sparkConfiguration;

    protected static final Logger LOGGER = LoggerFactory.getLogger(SparkContextFactory.class);

    public void setSparkConfiguration(SparkConfiguration sparkConfiguration) {
        this.sparkConfiguration = sparkConfiguration;
    }

    public static JavaStreamingContext createSimpleStreamingContext(String master, String appName) {
        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        if (master != null) {
            sparkConf.setMaster(master);
        }

        JavaStreamingContext streamContext = new JavaStreamingContext(sparkConf, Durations.seconds(4));
        streamContext.checkpoint("./spark_checkpoint");

        return streamContext;
    }

    public JavaSparkContext createSparkContext(String executionModeStr) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Titan");
        ExecutionMode executionMode = ExecutionMode.getExecutionMode(executionModeStr);
        if (executionMode == null) {
            throw new SparkCoreException("Failed during spark context creation. " + executionModeStr + " is not a valid ExecutionMode value");
        }

        String[] jarsFullPath = new String[0];
        try {
            jarsFullPath = sparkConfiguration.getJarsFullPath();
        } catch (URISyntaxException e) {
            LOGGER.error("Unable to load Spark jar dependencies, please check properties file");
        }
        switch (executionMode) {
            case Local:
                conf.setMaster("local[8]");
                URL url = getClass().getClassLoader().getResource("SparkLocalSchedulerPool.xml");
                conf.set("spark.scheduler.allocation.file", url.getPath());
                conf.set("spark.executor.memory", sparkConfiguration.getExecutorMemory());
                conf.set("spark.driver.memory", sparkConfiguration.getExecutorMemory());

                break;
            case Remote:
                conf.setMaster(sparkConfiguration.getSparkMasterRemote());
                if (jarsFullPath != null) {
                    conf.setJars(jarsFullPath);
                    conf.set("spark.executor.memory", sparkConfiguration.getExecutorMemory());
                    conf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC -XX:-UseGCOverheadLimit -XX:+UseCompressedOops");
                    conf.set("spark.default.parallelism", "32");
                }
                break;
            case Mesos:
                conf.setMaster("mesos://16.59.58.1:5050");
                if (jarsFullPath != null) {
                    conf.setJars(jarsFullPath);
                    conf.set("spark.executor.memory", sparkConfiguration.getExecutorMemory());
                    conf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC -XX:-UseGCOverheadLimit -XX:+UseCompressedOops");
                    //conf.set("spark.default.parallelism", "32");
                }
                conf.set("spark.executor.uri", "/root/spark/spark-1.1.0-bin-hadoop2.4.tgz");
//				conf.set("spark.mesos.executor.home", "/root/spark/mesos/executor/home");
                break;
            default:
                throw new SparkCoreException("Failed during spark context creation. Unable to create context of type : " + executionMode);
        }

        conf.set("spark.scheduler.mode", "FAIR");
//		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		conf.set("spark.kryo.registrator", "bigdata.platform.commons.spark.Line");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLocalProperty("spark.scheduler.pool", "titan");
        return sparkContext;
    }

    public JavaStreamingContext createStreamingContext(String executionModeStr, String appName) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appName);
        ExecutionMode executionMode = ExecutionMode.getExecutionMode(executionModeStr);
        if (executionMode == null) {
            throw new SparkCoreException("Failed during spark context creation. " + executionModeStr + " is not a valid ExecutionMode value");
        }

        String[] jarsFullPath = new String[0];
        try {
            jarsFullPath = sparkConfiguration.getJarsFullPath();
        } catch (URISyntaxException e) {
            LOGGER.error("Unable to load Spark jar dependencies, please check properties file");
        }
        switch (executionMode) {
            case Local:
                sparkConf.setMaster("local[4]");
//                URL url = getClass().getClassLoader().getResource("SparkLocalSchedulerPool.xml");
//                conf.set("spark.scheduler.allocation.file", url.getPath());
                sparkConf.set("spark.driver.memory", sparkConfiguration.getExecutorMemory());
                sparkConf.set("spark.executor.memory", sparkConfiguration.getExecutorMemory());

                break;
            case Remote:
                sparkConf.setMaster(sparkConfiguration.getSparkMasterRemote());
                if (jarsFullPath != null) {
                    sparkConf.setJars(jarsFullPath);
                    sparkConf.set("spark.executor.memory", sparkConfiguration.getExecutorMemory());
                    sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC -XX:-UseGCOverheadLimit -XX:+UseCompressedOops");
                    sparkConf.set("spark.default.parallelism", "32");
                }
                break;
            case Mesos:
                sparkConf.setMaster("mesos://16.59.58.1:5050");
                if (jarsFullPath != null) {
                    sparkConf.setJars(jarsFullPath);
                    sparkConf.set("spark.executor.memory", sparkConfiguration.getExecutorMemory());
                    sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC -XX:-UseGCOverheadLimit -XX:+UseCompressedOops");
                    //conf.set("spark.default.parallelism", "32");
                }
                sparkConf.set("spark.executor.uri", "/opt/spark/spark-1.1.0-bin-hadoop2.4.tgz");
//				conf.set("spark.mesos.executor.home", "/root/spark/mesos/executor/home");
                break;
            default:
                throw new SparkCoreException("Failed during spark context creation. Unable to create context of type : " + executionMode);
        }

        sparkConf.set("spark.scheduler.mode", "FAIR");
//		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		conf.set("spark.kryo.registrator", "bigdata.platform.commons.spark.Line");
        JavaStreamingContext streamContext = new JavaStreamingContext(sparkConf, Durations.seconds(4));
        streamContext.checkpoint("./spark_checkpoint");
        //streamContext.setLocalProperty("spark.scheduler.pool", "titan");
        return streamContext;
    }
}