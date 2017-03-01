package poc.sql.integrity.internal.generator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import poc.sql.integrity.internal.helper.DatasetHelper;
import poc.sql.integrity.internal.helper.FileHelper;

import java.io.Serializable;

import static org.apache.spark.sql.functions.monotonicallyIncreasingId;

/**
 * Created by eyallevy on 27/02/17.
 */
public class FileGenerator implements Serializable {

    private static final String TR_ID = "ID";
    private static final String CSV_PATH = "file:///opt/Dropbox/dev/poc/_resources/bigdata/QR_500K.csv";
    private static final String IDS_WITH_IDS_PATH = "file:///opt/Dropbox/dev/poc/_resources/bigdata/QR_500K_ID.csv";
    private static final String ONLY_IDS_PATH = "file:///opt/Dropbox/dev/poc/_resources/bigdata/ID.csv";

    FileHelper fileHelper = new FileHelper();
    DatasetHelper datasetHelper = new DatasetHelper();

    public void generateFilesWithIDS(SQLContext sqlContext) {
        System.out.println("Read src data-frame from CSV file");
        Dataset<Row> fullDataset = fileHelper.readCSV(sqlContext, CSV_PATH);

        System.out.println("Add IDs to data-frame and create new CSV file");
        Dataset<Row> datasetWithId = addIdsToCSV(fullDataset, IDS_WITH_IDS_PATH);

        System.out.println("write ids to CSV file");
        Dataset<Row> idsOnly20PrecentDataset = writeIntegrityIdsToCSV(datasetWithId, ONLY_IDS_PATH);
    }

    private Dataset<Row> addIdsToCSV(Dataset<Row> fullDataset, String csvPath) {
        System.out.println("Adding ids to the src Data-Frame");
        Dataset<Row> datasetWithId = fullDataset.withColumn(TR_ID, monotonicallyIncreasingId());
        datasetWithId = datasetWithId.cache();
        System.out.println(datasetWithId.count());

        System.out.println("write data with Ids to CSV file");
        fileHelper.writeCSV(datasetWithId, csvPath);
        return datasetWithId;
    }

    private Dataset<Row> writeIntegrityIdsToCSV(Dataset<Row> datasetWithId, String csvPath) {
        System.out.println("Filter 20% of the data into new DF and get only the IDs");
        Dataset<Row> idsOnly20PrecentDataset = datasetHelper.filter20Precent(datasetWithId, TR_ID);
        idsOnly20PrecentDataset = idsOnly20PrecentDataset.cache();
        System.out.println(idsOnly20PrecentDataset.count());

        System.out.println("write ids to CSV file");
        fileHelper.writeCSV(idsOnly20PrecentDataset, csvPath);
        return idsOnly20PrecentDataset;
    }
}
