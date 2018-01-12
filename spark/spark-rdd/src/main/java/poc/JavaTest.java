package poc;

import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by haimcohen on 09/08/2016.
 */
public class JavaTest {

    static Logger logger = LoggerFactory.getLogger(JavaTest.class);

    public static void main(String[] args) {
        try {
            logger.info("Started...");
            String file = "/opt/tr/data-loader/ds/input/QR_500K.csv";
            int skip = 100000;
            int linesToRead = 10;

            runBr(skip, linesToRead, file);
//            runJava(skip, linesToRead, file);
//            runLineNumberReader(skip, linesToRead, file);
            logger.info("Done");
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runJava(int skip, int linesToRead, String file) throws IOException {
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        StringBuilder sb = new StringBuilder();
        boolean finished = false;
        int linesSkipped = 0;
        int lineRead = 0;
        int linesTraversed = 0;
        while(!finished) {
            linesTraversed++;
            String line = br.readLine();
            if (line == null) {
                break;
            }
            if (linesSkipped > skip) {
                sb.append(line);
                sb.append("\n");
                lineRead++;
            }
            else {
                linesSkipped++;
            }
            if (lineRead >= linesToRead) {
                finished = true;
            }
        }
        fr.close();
        logger.info("Lines skipped: {}. Lines read: {}. Lines traversed: {}", linesSkipped, lineRead, linesTraversed);
    }

    public static void runLineNumberReader(int skip, int linesToRead, String file) throws IOException {
        try (LineNumberReader rdr = new LineNumberReader(new FileReader(file))) {
            int linesRead = 0;
            int linesTraversed = 0;
            StringBuilder sb = new StringBuilder();
            for (String line = null; (line = rdr.readLine()) != null && linesRead < linesToRead;) {
                linesTraversed++;
                if (rdr.getLineNumber() > skip) {
                    linesRead++;
                    sb.append(line);
                    sb.append("\n");
                }
            }
            logger.info("Lines read: {}. Lines traversed: {}", linesToRead, linesTraversed);
        } catch (IOException e) {
            throw e;
        }
    }

    public static void runBr(int skip, int linesToRead, String file) throws IOException {
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        File tmpFile = File.createTempFile("data", ".csv");
        FileWriter fw = new FileWriter(tmpFile);
        BufferedWriter bw = new BufferedWriter(fw);
        MutableLong linesRead = new MutableLong(0);
        br.lines()
                .skip(skip)
                .limit(linesToRead)
                .forEach(line -> {
                    try {
                        bw.write(line + "\n");
                        linesRead.add(1);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        bw.close();
        br.close();
        logger.info("Lines file: {}, lines Read:{}, Lines skipped: {}", tmpFile.getAbsolutePath(), linesRead.longValue(), skip);
    }
}
