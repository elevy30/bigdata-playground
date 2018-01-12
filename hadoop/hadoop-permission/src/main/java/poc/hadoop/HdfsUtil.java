package poc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Created by eyallevy on 24/01/17.
 */
public class HdfsUtil {

    public static void main(String[] args) {
        HdfsUtil hdfsUtil = new HdfsUtil();
        Configuration conf = hdfsUtil.initConfiguration(false);
        hdfsUtil.writeToFile(conf, "/user/hduser/test/hello.txt");
    }

    public Configuration initConfiguration(boolean localConfiguration) {
        Configuration conf = new Configuration();
        if (!localConfiguration){
            //configuration.set(MAPRED_JOB_TRACKER,mapredJobTracker + ":" + mapredJobTrackerPort);
            //conf.set(FS_DEFAULT_NAME,   fsDefaultName    + ":" + fsDefaultPort);
            //configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            //configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            conf.set("hadoop.job.ugi",    "hduser");
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            System.out.print("Conf......");
        }
        return conf;
    }


    public void writeToFile(Configuration conf, String filePath) {
        FileSystem hdfs = null;
        BufferedWriter br = null;
        try {
            hdfs = FileSystem.get(conf);
            Path file = new Path(filePath);

            OutputStream os = hdfs.create(file, () -> System.out.println("...bytes written: [ ]"));
            br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            br.write("Hello World");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) br.close();
                if (hdfs != null) hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void writeToFileUri(Configuration conf, String filePath) {
        FileSystem hdfs = null;
        BufferedWriter br = null;
        try {
            hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
            Path file = new Path("hdfs://localhost:9000" + filePath);

            OutputStream os = hdfs.create(file, () -> System.out.println("...bytes written: [ ]"));
            br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            br.write("Hello World");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } finally {
             try {
                 if (br != null) br.close();
                if (hdfs != null) hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public int run(Configuration configuration) throws Exception {
        for (Map.Entry<String, String> entry : configuration) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }
        return 0;
    }
}
