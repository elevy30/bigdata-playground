package poc.commons;

import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;


public class SparkConfiguration {
    public static final String SPARK_EXECUTION_MODE = "spark.execution.mode";
    public static final String SPARK_HOST = "spark.host";
    public static final String SPARK_PORT = "spark.port";
    public static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
    public static final String SPARK_SHUFFLE_SPILL = "spark.shuffle.spill";
    public static final String SPARK_SHUFFLE_MEMORY = "spark.shuffle.memoryFraction";
    public static final String SPARK_TASK_MAX_FAILURE = "spark.task.maxFailures";


    @Value("${spark.host}")
    private String sparkHost = "localhost";

    @Value("${spark.port}")
    private String sparkPort;

    @Value("${spark.context.pool.size}")
    private int poolSize;

    @Value("${spark.executor.memory}")
    public String executorMemory = "4";

    @Value("${spark.shuffle.spill}")
    public String shuffleSpill = "true";

    @Value("${spark.shuffle.memoryFraction}")
    public String shuffleMemoryFraction = "0.3";

    @Value("${spark.task.maxFailures}")
    public String taskMaxFailures = "1";

    @Value("${spark.jar.dependencies}")
    private String jarsString;

    public SparkConfiguration() {
    }


    public String getSparkHost() {
        return sparkHost;
    }

    public void setSparkHost(String sparkHost) {
        this.sparkHost = sparkHost;
    }

    public String getSparkPort() {
        return sparkPort;
    }

    public void setSparkPort(String sparkPort) {
        this.sparkPort = sparkPort;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public String getSparkMasterRemote() {
        return "spark://" + sparkHost + ":" + sparkPort;
    }

    public void setJarsString(String jarsString) {
        this.jarsString = jarsString;
    }

    public String getSparkMasterLocal() {
        return "local[2]";
    }

    public String[] getJarsFullPath() throws URISyntaxException {
		String[] jarsFullPath = null;

		if (jarsString != null) {
			URL sparkJars = this.getClass().getClassLoader().getResource(jarsString);
			Path sparkJarsPath = Paths.get(sparkJars.toURI());
			File sparkJarsFolder = sparkJarsPath.toFile();
			File[] sparkJarFiles = sparkJarsFolder.listFiles();
			jarsFullPath = new String[sparkJarFiles.length];
			for (int i = 0; i < sparkJarFiles.length; i++) {
				jarsFullPath[i] = sparkJarFiles[i].getAbsolutePath();
			}
		}

        return jarsFullPath;
    }

    public int getPoolSize() {
        return poolSize;
    }
}
