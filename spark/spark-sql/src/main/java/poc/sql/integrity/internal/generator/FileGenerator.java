package poc.sql.integrity.internal.generator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import poc.sql.integrity.internal.helper.DatasetHelper;
import poc.sql.integrity.internal.helper.FileHelper;
import poc.sql.integrity.internal.prop.Prop;

import java.io.Serializable;

import static org.apache.spark.sql.functions.monotonicallyIncreasingId;

/**
 * Created by eyallevy on 27/02/17.
 */
public class FileGenerator implements Serializable {

    private Prop prop;
    private FileHelper fileHelper = new FileHelper();
    private DatasetHelper datasetHelper = new DatasetHelper();

    public FileGenerator(Prop prop) {
        this.prop = prop;
    }

    public void generateFilesWithIDS(SQLContext sqlContext) {
        System.out.println("Read src data-frame from CSV file");
        Dataset<Row> fullDataset = fileHelper.readCSV(sqlContext, prop.getCsvPath());

        System.out.println("Add IDs to data-frame and create new CSV file");
        Dataset<Row> datasetWithId = addIdsToCSV(fullDataset, prop.getDataSourceIdPath());

        System.out.println("write ids to CSV file");
        Dataset<Row> idsOnly20PrecentDataset = writeIntegrityIdsToCSV(datasetWithId, prop.getIdsOnlyPath());
    }

    private Dataset<Row> addIdsToCSV(Dataset<Row> fullDataset, String csvPath) {
        System.out.println("Adding ids to the src Data-Frame");
        Dataset<Row> datasetWithId = fullDataset.withColumn(prop.getId(), monotonicallyIncreasingId());
        datasetWithId = datasetWithId.cache();
        System.out.println(datasetWithId.count());

        System.out.println("write data with Ids to CSV file");
        fileHelper.writeCSV(datasetWithId, csvPath);
        return datasetWithId;
    }

    private Dataset<Row> writeIntegrityIdsToCSV(Dataset<Row> datasetWithId, String csvPath) {
        System.out.println("Filter 20% of the data into new DF and get only the IDs");
        Dataset<Row> idsOnly20PrecentDataset = datasetHelper.filter20Precent(datasetWithId, prop.getId());
        idsOnly20PrecentDataset = idsOnly20PrecentDataset.cache();
        System.out.println(idsOnly20PrecentDataset.count());

        System.out.println("write ids to CSV file");
        fileHelper.writeCSV(idsOnly20PrecentDataset, csvPath);
        return idsOnly20PrecentDataset;
    }
}
