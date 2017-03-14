package poc.sql.integrity.internal.bigjoin;

import org.apache.spark.sql.*;
import poc.commons.time.StreamTimer;
import poc.sql.integrity.internal.helper.DatasetHelper;
import poc.sql.integrity.internal.helper.FileHelper;
import poc.commons.SparkSessionInitializer;
import poc.sql.integrity.internal.prop.Prop;
import poc.sql.integrity.internal.prop.Properties_1;

import java.io.Serializable;


/**
 * Created by eyallevy on 08/01/17.
 */
public class BigJoin implements Serializable {

    private Prop prop = new Properties_1();
    private DatasetHelper datasetHelper = new DatasetHelper();
    private FileHelper fileHelper = new FileHelper();

    private void run(SparkSession sc) {
        SQLContext sqlContext = new SQLContext(sc);

        System.out.println("####### Read dataSource from CSV file");
        Dataset<Row> dataSource = fileHelper.readCSV(sqlContext, prop.getDataSourceIdPath());
        //System.out.println("#OfRow in the Full Dataset " + fullDataset.count());
        //fullDataset.printSchema();

        System.out.println("####### Filter 20 precent of lines");
        Dataset<Row> idsOnly20PrecentDataset = datasetHelper.filter20Precent(dataSource, prop.getId());
        //System.out.println("#OfRow in the Filtered Dataset " + idsOnly20PrecentDataset.count());
        //idsOnly20PrecentDataset.show();

        System.out.println("####### Join 2 dataset");
        StreamTimer streamTimerJoin = new StreamTimer();
        streamTimerJoin.start();
        Dataset<Row> joined = join(dataSource, idsOnly20PrecentDataset);
        //System.out.println("#OfRow in the Joined Dataset " + joined.count());
        //joined.show();
        streamTimerJoin.stop();
        System.out.println("JOIN Duration" + streamTimerJoin.getDuration());

        System.out.println("####### Paging");
        StreamTimer streamTimerFilter = new StreamTimer();
        streamTimerFilter.start();
        Dataset<Row> page = datasetHelper.readPage(joined, 1000, 1000, true, false, prop.getId());
        page.show();
        streamTimerFilter.stop();
        System.out.println("Filter Duration" + streamTimerFilter.getDuration());
        //Filter Duration 98432
    }

    private Dataset<Row> join(Dataset<Row> fullDataset, Dataset<Row> idsDataset) {
        //HashPartitioner hashPartitioner = new HashPartitioner(1000);
        Column joinedColumn = fullDataset.col(prop.getId()).equalTo(idsDataset.col(prop.getId()));
        //fullDataset = fullDataset.repartition(8);
        //idsDataset = idsDataset.repartition(8);
        return idsDataset.join(fullDataset, joinedColumn).drop(idsDataset.col(prop.getId()));
    }

    public static void main(String[] args) {
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        SparkSession sparkSession = sparkSessionInitializer.init();

        BigJoin app = new BigJoin();
        app.run(sparkSession);
    }
}

