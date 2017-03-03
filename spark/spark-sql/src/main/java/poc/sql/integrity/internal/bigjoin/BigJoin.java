package poc.sql.integrity.internal.bigjoin;

import org.apache.spark.sql.*;
import poc.commons.time.Stream;
import poc.sql.integrity.internal.helper.DatasetHelper;
import poc.sql.integrity.internal.helper.FileHelper;
import poc.sql.integrity.internal.helper.SparkSessionInitializer;
import poc.sql.integrity.internal.prop.Prop;
import poc.sql.integrity.internal.prop.Properties_1;

import java.io.Serializable;


/**
 * Created by eyallevy on 08/01/17.
 */
public class BigJoin implements Serializable {

    Prop prop = new Properties_1();
    DatasetHelper datasetHelper = new DatasetHelper();
    FileHelper fileHelper = new FileHelper();

    public SparkSession init() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/hadoop_home");
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();

        return sparkSessionInitializer.getSparkSession();
    }

    private void run(SparkSession sc) {
        SQLContext sqlContext = new SQLContext(sc);

        System.out.println("####### Read datasource from CSV file");
        Dataset<Row> dataSource = fileHelper.readCSV(sqlContext, prop.getDataSourceIdPath());
        //System.out.println("#OfRow in the Full Dataset " + fullDataset.count());
        //fullDataset.printSchema();

        System.out.println("####### Filter 20 precent of lines");
        Dataset<Row> idsOnly20PrecentDataset = datasetHelper.filter20Precent(dataSource, prop.getId());
        //System.out.println("#OfRow in the Filtered Dataset " + idsOnly20PrecentDataset.count());
        //idsOnly20PrecentDataset.show();

        System.out.println("####### Join 2 dataset");
        Stream streamJoin = new Stream();
        streamJoin.start();
        Dataset<Row> joined = join(dataSource, idsOnly20PrecentDataset);
        //System.out.println("#OfRow in the Joined Dataset " + joined.count());
        //joined.show();
        streamJoin.stop();
        System.out.println("JOIN Duration" + streamJoin.getDuration());

        System.out.println("####### Paging");
        Stream streamFilter = new Stream();
        streamFilter.start();
        Dataset<Row> page = datasetHelper.readPage(joined, 1000, 1000, true, false, prop.getId());
        page.show();
        streamFilter.stop();
        System.out.println("Filter Duration" + streamFilter.getDuration());
        //Filter Duration 98432
    }

    private Dataset<Row> join(Dataset<Row> fullDataset, Dataset<Row> idsDataset) {
        //HashPartitioner hashPartitioner = new HashPartitioner(1000);
        Column joinedColumn = fullDataset.col(prop.getId()).equalTo(idsDataset.col(prop.getId()));
        //fullDataset = fullDataset.repartition(8);
        //idsDataset = idsDataset.repartition(8);
        Dataset<Row> joined = idsDataset.join(fullDataset, joinedColumn).drop(idsDataset.col(prop.getId()));
        return joined;
    }

    public static void main(String[] args) {
        BigJoin app = new BigJoin();
        SparkSession sparkSession = app.init();
        app.run(sparkSession);
    }
}

