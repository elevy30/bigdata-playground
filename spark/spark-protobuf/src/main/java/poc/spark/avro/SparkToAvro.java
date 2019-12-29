package poc.spark.avro;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import poc.commons.SparkSessionInitializer;
import poc.helpers.FileHelper;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static scala.collection.JavaConversions.seqAsJavaList;

@Slf4j
public class SparkToAvro {

    public static void main(String[] args) {
        SparkSessionInitializer sparkSessionInitializer = new SparkSessionInitializer();
        SparkSession sparkSession = sparkSessionInitializer.getSparkSession("SparkToAvro");


        FileHelper fileHelper = new FileHelper();
        SQLContext sqlContext = new SQLContext(sparkSession);

        Dataset<Row> datasetcsv = fileHelper.readCSV(sqlContext, System.getProperty("user.dir") + "/_resources/bigdata/iris/iris.csv");
        System.out.println(datasetcsv.count());
        datasetcsv.printSchema();

        datasetcsv.write().mode(SaveMode.Overwrite).parquet(System.getProperty("user.dir") + "/_resources/bigdata/iris/iris_parquet");

//        System.out.println(System.currentTimeMillis() + " 0000000000000000000000");
//        Dataset<Row> dataset = fileHelper.readParquet(sqlContext, System.getProperty("user.dir") + "/_resources/iris/QR_500K_parquet");
//        dataset.show(false);
//        dataset.printSchema();

//        avroInMemory(dataset);

        System.out.println(System.currentTimeMillis() + " 11111111111111");
        datasetcsv.write().mode(SaveMode.Overwrite).format("com.databricks.spark.avro").save(System.getProperty("user.dir") + "/_resources/bigdata/iris/iris_avro");

        System.out.println(System.currentTimeMillis() + " 222222222222222222");
        Dataset<Row> load = sqlContext.read().format("com.databricks.spark.avro").load(System.getProperty("user.dir") + "/_resources/bigdata/QR_500K_avro");

        System.out.println(System.currentTimeMillis() + " 333333333333333");
        //load.collectAsList();

        System.out.println(System.currentTimeMillis() + " 4444444444444444");
    }

    private static void avroInMemory(Dataset<Row> dataset) {
        JavaRDD<String> lines = dataset.toJavaRDD().map(row -> {
            List<Object> objects = seqAsJavaList(row.toSeq());
            String[] strValues = objects.stream().map(Object::toString).toArray(String[]::new);
            return Arrays.toString(strValues);
        });
        List<String> collect = lines.collect();

        int size = collect.size();
        String[] array = new String[size];
        for (int i = 0; i < size ; i++) {
            array[i] = collect.get(i);
        }

        // Serialize user1, user2 and user3 to disk

        try {
            Schema schema = new Schema.Parser().parse(new File("/opt/user.avsc"));

            GenericRecord user1 = new GenericData.Record(schema);
            user1.put("arr", collect);

            File file = new File("/opt/qr500.avro");
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            dataFileWriter.create(schema, file);
            dataFileWriter.append(user1);
            dataFileWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
