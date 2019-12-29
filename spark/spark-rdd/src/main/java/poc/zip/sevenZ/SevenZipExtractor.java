package poc.zip.sevenZ;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/***
 * Created by eyallevy on 09/03/17 .
 */
@SuppressWarnings("Duplicates")
public class SevenZipExtractor implements Serializable {
    private static final String ZIP_FILE_PATH ="/opt/Dropbox/dev/git-hub/poc/_resources/bigdata/ZIP/proxy_fixed.csv.7z";
    private static final String OUTPUT_DIR ="/opt/Dropbox/dev/git-hub/poc/_resources/bigdata/ZIP/output";


    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[8]");

        SparkSession spark = SparkSession
                .builder()
                .appName("loadZipFile")
                .config(conf)
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        SevenZipExtractor sevenZipExtractor = new SevenZipExtractor();
//        sevenZipExtractor.runWithSpark(jsc);
       sevenZipExtractor.run();
    }

    private void run() {
        unzip(ZIP_FILE_PATH);
    }

    private void runWithSpark(JavaSparkContext jsc) {
        JavaPairRDD<String, String> files = jsc.wholeTextFiles("/opt/Dropbox/dev/git-hub/poc/_resources/bigdata/ZIP");
        JavaRDD<String> map = files.map(file -> {
            System.out.println(file._1());
            return unzip(file._1());
        });
        List<String> collect = map.collect();
        System.out.println(collect);
    }


    private String unzip(String zipFilePath){
        zipFilePath = zipFilePath.substring(zipFilePath.indexOf('/'));
        try (SevenZFile sevenZFile = new SevenZFile(new File(zipFilePath))) {

            Iterable<SevenZArchiveEntry> entries = sevenZFile.getEntries();

            entries.forEach(entry -> {
                File destinationFile = new File(OUTPUT_DIR,  entry.getName());
                System.out.println(destinationFile.getAbsoluteFile());
                if (entry.isDirectory()) {
                    destinationFile.mkdirs();
                } else {
                    destinationFile.getParentFile().mkdirs();
                    extractFile(sevenZFile, destinationFile);
                }
            });
        }catch (IOException e){
            e.printStackTrace();
        }
        return "";
    }

    private void extractFile(SevenZFile sevenZFile, File destinationFile) {
        int bufferSize = 1024 * 1024 * 1000 ;
        try (BufferedOutputStream buffOut = new BufferedOutputStream(new FileOutputStream(destinationFile))) {
            sevenZFile.getNextEntry();
            byte[] content = new byte[bufferSize];
            int available = -1;
            int loopNumber = 1;
            while ((available = sevenZFile.read(content, 0, bufferSize)) > 0) {
                System.out.println("buffer # " + loopNumber + " size = " + available);
                buffOut.write(content, 0, available);
                loopNumber++;
            }
            buffOut.flush();

        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
