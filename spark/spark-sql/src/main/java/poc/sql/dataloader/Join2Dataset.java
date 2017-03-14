package poc.sql.dataloader;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import poc.commons.SparkSessionInitializer;
import poc.sql.integrity.internal.helper.DatasetHelper;
import poc.sql.integrity.internal.helper.FileHelper;

/**
 * Created by eyallevy on 05/03/17.
 */
public class Join2Dataset {

    FileHelper fileHelper = new FileHelper();
    DatasetHelper datasetHelper = new DatasetHelper();

    public static void main(String[] args) {
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        SparkSession sparkSession = sparkSessionInitializer.init();


        Join2Dataset rddConverter = new Join2Dataset();
//        rddConverter.createBigSchema(sparkSession, 0, FIELD_COUNT);
        rddConverter.run(sparkSession);
    }

    private void run(SparkSession sparkSession) {
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(sparkSession.sparkContext());

        //maptopair
        JavaRDD<String> stringJavaRDD = fileHelper.readFile(jsc, "file:///opt/Dropbox/dev/git-hub/poc/_resources/data/dataloader/ds1.csv");
        stringJavaRDD.mapToPair((PairFunction<String, String, String>) line -> {
            //new Tuple2<Long, Boolean>(row.getLong(0), true)
            return null;
        });
//        return rowDataset.select(columnName)
//                .toJavaRDD()
//                .mapToPair((PairFunction<Row, Long, Boolean>) row -> new Tuple2<Long, Boolean>(row.getLong(0), true));
//        public JavaPairRDD<Long, Boolean> convertToPairRDD (Dataset < Row > rowDataset, String columnName){
//            return rowDataset.select(columnName)
//                    .toJavaRDD()
//                    .mapToPair((PairFunction<Row, Long, Boolean>) row -> new Tuple2<Long, Boolean>(row.getLong(0), true));

        //filter in loop of 1000
    }


}

