package poc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;

import static java.util.Arrays.*;
import static org.apache.spark.sql.functions.max;

/**
 * Created by eyallevy on 28/02/17.
 */
@Service
public class HDFSUtil {


    private String hdfsAnalysisBasePath;


    String nameNodeUrl;

    // HDFS Paths
    private static final String REPORTS = "/reports";
    private static final String ANOMALIES = "/anomalies";
    private static final String CLUSTERING_MODEL = "/clusteringmodel";
    private static final String CLUSTERING_MODEL_SCHEMA = "/clusteringmodelschema";

    private Logger logger = LoggerFactory.getLogger(HDFSUtil.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Java API demo");
        conf.setMaster("local[2]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(javaSparkContext);


        DataFrame existingReports = sqlContext.read().parquet("/opt/data/analysis-56/hdfs/56/reports");
        existingReports.show();
        HDFSUtil hdfsUtil = new HDFSUtil();
        hdfsUtil.notifyReportsTable(javaSparkContext,sqlContext,"/opt/data/analysis-56/hdfs/56/reports", 1998814L);
        String maxIdFromReport = hdfsUtil.getMaxIdFromReport("/opt/data/analysis-56/hdfs/56/reports", sqlContext);
        System.out.println(maxIdFromReport);
        hdfsUtil.notifyReportsTable(javaSparkContext,sqlContext,"/opt/data/analysis-56/hdfs/56/reports", 1998816L);
        maxIdFromReport = hdfsUtil.getMaxIdFromReport("/opt/data/analysis-56/hdfs/56/reports", sqlContext);
        System.out.println(maxIdFromReport);
    }

    public String getMaxIdFromReport(String reportsPath, SQLContext sqlContext) {
        DataFrame existingReports = sqlContext.read().parquet(reportsPath);
        existingReports.show();
        DataFrame select = existingReports.select(max("tr_identifier"));
        select.show();
        String lastClusteredAnomalyTrSequenceId = String.valueOf(select.first().getLong(0));

        return lastClusteredAnomalyTrSequenceId;
    }

    public void notifyReportsTable(JavaSparkContext javaSparkContext, SQLContext sqlContext, String reportsPath, long l) {

        Encoder<ClusterReportRecord> clusterReportRecordEncoder = Encoders.bean(ClusterReportRecord.class);
//        Encoder<Tuple3<String, String, Long>> tuple = Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.LONG());
        ArrayList<ClusterReportRecord>  clusterReportRecords = new ArrayList<>();

        clusterReportRecords.add(new ClusterReportRecord("AB_hhs_67546","hello" , l));
        JavaRDD<ClusterReportRecord> rdd = javaSparkContext.parallelize(clusterReportRecords);

        sqlContext.createDataset(rdd.rdd(), clusterReportRecordEncoder).toDF()
                .write()
                .mode(SaveMode.Append)
                .parquet(reportsPath);
    }


    public void cleanClusteringDataForAnalysis(String analysisId){
        String reportsData = getReportsPath(analysisId);
        String clusteringModel = getClusteringModelPath(analysisId);
        String clusteringModelSchema = getClusteringModelSchemaPath(analysisId);
        try {
            deletePath(reportsData);
            deletePath(clusteringModel);
            deletePath(clusteringModelSchema);
            logger.info("Deleted Clustering Model + Schema and Reports for Analysis : {} ", analysisId);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Can not clean Clustering Model + Schema and Reports for Analysis {} , because :\n {} ", analysisId, e.getMessage());
        }

    }

    public void deletePath(String path) throws IOException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", nameNodeUrl);
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path pathReturn=new Path(path);
            if(hdfs.exists(pathReturn)){
                hdfs.delete(pathReturn,true);
            }
            logger.info("Deleted: {} ", path);
        } catch (IOException e) {
            logger.error("Can not clean delete {} , because :\n {} ", path, e.getMessage());
            throw e;
        }
    }

    private String getReportsPath(String analysisId){
        return new Path(hdfsAnalysisBasePath, analysisId).toString() + REPORTS;
    }

    private String getClusteringModelPath(String analysisId){
        return new Path(hdfsAnalysisBasePath, analysisId).toString() + CLUSTERING_MODEL;
    }

    public String getAnomaliesPath(String analysisId){
        return new Path(hdfsAnalysisBasePath, analysisId).toString() + ANOMALIES;
    }

    public String getClusteringModelSchemaPath(String analysisId) {
        return new Path(hdfsAnalysisBasePath, analysisId).toString() + CLUSTERING_MODEL_SCHEMA;
    }

    public void saveClusteringModelSchema(JavaSparkContext jsc, String analysisId, ArrayList<String> clusteringModelSchema) {
        JavaRDD<ArrayList<String>> featureNameOrderRdd = jsc.parallelize(asList(clusteringModelSchema));
        String path = getClusteringModelSchemaPath(analysisId);
        featureNameOrderRdd.saveAsObjectFile(path);
    }
}
