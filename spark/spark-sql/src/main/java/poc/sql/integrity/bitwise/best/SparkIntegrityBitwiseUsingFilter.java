package poc.sql.integrity.bitwise.best;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import poc.commons.time.Stream;
import poc.sql.integrity.bitwise.ColumnLocation;
import poc.sql.integrity.internal.bigfilter.BigFilterPageBeforeFilter_Map;
import poc.sql.integrity.internal.generator.BitwiseGenerator;
import poc.sql.integrity.internal.helper.BitwiseHelper;
import poc.sql.integrity.internal.helper.FileHelper;
import poc.sql.integrity.internal.helper.SparkSessionInitializer;
import poc.sql.integrity.internal.prop.Prop;
import poc.sql.integrity.internal.prop.Properties_1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by eyallevy on 08/01/17.
 */
public class SparkIntegrityBitwiseUsingFilter implements Serializable {

    private Prop prop = new Properties_1();
    private BigFilterPageBeforeFilter_Map bigFilter = new BigFilterPageBeforeFilter_Map(prop);

    private SparkSession init() {
        System.setProperty("hadoop.home.dir", "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/hadoop_home");
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        return sparkSessionInitializer.getSparkSession();
    }

    private void run(SparkSession sc) {
        SQLContext sqlContext = new SQLContext(sc);
        BitwiseGenerator fileBitwiseGenerator = new BitwiseGenerator(sc, prop);
        BitwiseHelper bitwiseHelper = new BitwiseHelper(prop);
        FileHelper fileHelper = new FileHelper();

        Stream stream = new Stream();
        stream.start();
        System.out.println("########### Read datasource csv file");
        Dataset<Row> dataSource = fileHelper.readCSV(sqlContext, prop.getDataSourcePath());
        stream.stop();
        System.err.println(stream.getDuration());//32110

        stream.start();
        System.out.println("########### Read columnLocationMap object file");
        Map<String, ColumnLocation> columnLocationMap = fileBitwiseGenerator.readColumnLocationFromFile();
        StructType schema = createSchema(columnLocationMap);
        stream.stop();
        System.err.println(stream.getDuration());//106

        stream.start();
        System.out.println("########### Read integrity bitwise file");
        Dataset<Row> integrityBitwiseDS = fileHelper.readCSV(sqlContext,  prop.getBitwisePath(), schema);
        stream.stop();
        System.err.println(stream.getDuration());//15

        stream.start();
        Dataset<Row> idsDS = bitwiseHelper.getAllIntegrityIdsForSpecificColumn(integrityBitwiseDS, columnLocationMap);
        //idsDS.show();
        stream.stop();
        System.err.println(stream.getDuration());//40

        bigFilter.bigFilter(dataSource, idsDS);//2936 per page

    }

    private StructType createSchema(Map<String, ColumnLocation> columnLocationMap) {
        int numberOfBitColumn = getMaxColumnId(columnLocationMap) + 1;
        List<StructField> fields = new ArrayList<>();
        StructField idField = DataTypes.createStructField(prop.getId(), DataTypes.LongType, false);
        fields.add(idField);
        for (int i = 0; i < numberOfBitColumn; i++) {
            StructField bitColumnField = DataTypes.createStructField(String.valueOf(i), DataTypes.LongType, false);
            fields.add(bitColumnField);
        }
        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }

    private int getMaxColumnId(Map<String, ColumnLocation> columnLocationMap) {
        final int[] maxColumnId = {0};
        columnLocationMap.forEach((s, columnLocation) -> {
            int id = columnLocation.bitColumnId;
            if(id > maxColumnId[0]){
                maxColumnId[0] = id;
            }
        });
        return maxColumnId[0];
    }

    public static void main(String[] args) {
        SparkIntegrityBitwiseUsingFilter app = new SparkIntegrityBitwiseUsingFilter();
        SparkSession sparkSession = app.init();
        app.run(sparkSession);
    }
}
