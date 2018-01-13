package poc.avro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@RestController
@CrossOrigin
public class Controller {

    @RequestMapping("/avro")
    public String avro(HttpServletResponse response) {
        String msg =  "Hello Docker World";
        FSDataInputStream dataInputStream = null;

        try {
            Configuration config = new Configuration();
            FileSystem hdfs = FileSystem.get(config);

            Path inputFilePath = new Path(System.getProperty("user.dir") + "/spring-boot-avro/src/main/resources/iris.avro");

            dataInputStream = hdfs.open(inputFilePath);

            IOUtils.copyBytes(dataInputStream, response.getOutputStream(),1000000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return msg;
    }

    @RequestMapping("/parquet")
    public String parquet(HttpServletResponse response) {
        String msg =  "Hello Docker World";

        try {
            Configuration config = new Configuration();
            FileSystem hdfs = FileSystem.get(config);
            Path inputFilePath = new Path(System.getProperty("user.dir") + "/spring-boot-avro/src/main/resources/iris.parquet");
            FSDataInputStream dataInputStream = hdfs.open(inputFilePath);

            response.setContentType("multipart/form-data");
            response.setHeader("Content-disposition", "attachment; filename= iris.parquet");
            try (ServletOutputStream outputStream = response.getOutputStream()) {
                org.apache.commons.io.IOUtils.copy(dataInputStream, outputStream);
            }
            dataInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return msg;
    }

}