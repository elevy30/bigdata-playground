package poc.streaming.rddtostream;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


/**
 * Created by elevy on 01/02/17.
 */
public class GenerateFile {
    public static void main(String[] args) {
        String filePath = "Z:/Backup_Cloud/i.eyal.levy/Dropbox/dev/poc/_resources/bigdata/bigFile.txt";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filePath))) {

            String content = "hello\nhome\nlive\nday\nminos\nhour\n";
            for (int i = 0; i < 11500000; i++) {
                bw.write(content);
            }
            System.out.println("Done");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
