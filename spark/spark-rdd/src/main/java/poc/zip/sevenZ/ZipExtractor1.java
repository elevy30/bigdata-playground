package poc.zip.sevenZ;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/***
 * Created by eyallevy on 09/03/17 .
 */
@SuppressWarnings("Duplicates")
public class ZipExtractor1 implements Serializable{
    private static final String ZIP_FILE_PATH = "/opt/Dropbox/dev/git-hub/poc/_resources/bigdata/ZIP/proxy_fixed.csv.7z";

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");

        SparkSession spark = SparkSession
                .builder()
                .appName("loadZipFile")
                .config(conf)
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        ZipExtractor1 zipExtractor1 = new ZipExtractor1();
        zipExtractor1.runWithSpark(jsc);
//       zipExtractor1.run();
    }

    private void run() {
        unzip(ZIP_FILE_PATH);
    }

    private void runWithSpark(JavaSparkContext jsc) {
        JavaPairRDD<String, String> files = jsc.wholeTextFiles("file:///opt/Dropbox/dev/git-hub/poc/_resources/bigdata/ZIP/");
        files.map(file -> unzip(file._2()));
    }

    private String unzip(String zipFilePath) {
        File file = new File(zipFilePath);
        return unzip(file);
    }

    private String unzip(File zipfile) {
        try (SevenZFile sevenZFile = new SevenZFile(zipfile)) {
            SevenZArchiveEntry entry = sevenZFile.getNextEntry();
            while (entry != null) {
                System.out.printf("name: %-20s | size: %6d \n", entry.getName(), entry.getSize());
                boolean mkdirs = true;
                File parentPath = zipfile.getParentFile();
                if (!parentPath.exists()) {
                    mkdirs = parentPath.mkdirs();
                }
                if (mkdirs) {
                    String destinationFile = parentPath + File.separator + entry.getName();
                    System.out.println(destinationFile);

                    extractFile(sevenZFile, destinationFile);
                    entry = sevenZFile.getNextEntry();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    private void extractFile(SevenZFile sevenZFile, String destinationFile) {
        int bufferSize = 1024 * 1024;
        try (BufferedOutputStream buffOut = new BufferedOutputStream(new FileOutputStream(destinationFile))) {
            byte[] content = new byte[bufferSize];
            int available = -1;
            int loopNumber = 1;
            while ((available = sevenZFile.read(content, 0, bufferSize)) > 0) {
                System.out.println("buffer # " + loopNumber + " size = " + available);
                buffOut.write(content, 0, available);
                loopNumber++;
            }
            buffOut.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


//*************************************
    //SCALA
//****************************************
//        Stream.continually((Function)zipInputStream.getNextEntry()).takeWhile(entry -> entry != null).foreach((File) file -> {
//            if (!file.isDirectory()) {
//                String outPath = destination.resolve(file)
//                String outPathParent = outPath.getParent();
//                if (!(new File(outPathParent)).exists()) {
//                    (new File(outPathParent)).mkdirs();
//                }
//
//                File outFile = new File(outPath);
//                FileOutputStream out = new FileOutputStream(outFile);
//                List buffer = new Arrays.asList(new Byte[4096]);
//                Stream.continually(zipInputStream.read(buffer)).takeWhile(_ != -1).foreach(out.write(buffer, 0, _))
//            }
//        }
}
