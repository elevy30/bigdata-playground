package poc.spark.streaming;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by eyallevy on 08/12/17 .
 */
@Slf4j
public class FileGenerator {



    public static void main(String[] args) {
        FileGenerator fileGenerator = new FileGenerator();
        fileGenerator. runTimer();
    }

    private  void runTimer() {
        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        //run only in round minutes
        scheduledExecutor.scheduleAtFixedRate(new MyTask(), millisToNextMinutes(), 60*1000, TimeUnit.MILLISECONDS);

        scheduledExecutor.scheduleAtFixedRate(new MyTask(), 1L, 60*1000, TimeUnit.MILLISECONDS);
       // scheduledExecutor.scheduleAtFixedRate(FileGenerator::writeFile, millisToNextMinutes(), 10*1000, TimeUnit.MILLISECONDS);
    }

    private long millisToNextMinutes() {
        LocalDateTime nextMinutes = LocalDateTime.now().plusMinutes(1).truncatedTo(ChronoUnit.MINUTES);
        return LocalDateTime.now().until(nextMinutes, ChronoUnit.MILLIS);
    }


    public class MyTask implements Runnable{
        private Long index;
        private String fileName = "trx";
        private String basePath = "/opt/tr/data-loader/ds/input/spark_streaming/";
        private String header   = "datetime,s_action,sc_bytes\n";

        @Override
        public void run() {
            try {
                for (int i = 0; i < 3; i++) {
                    Date date = new Date();
                    log.info("HHHH " + date + "HHHH");
                    //index = index + 1;
                    String filePath = basePath + fileName + date.getTime();
                    log.info("file path:" + filePath);

                    String content = header;
                    String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(date);
                    if( i%3 == 1 ){
                        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
                        time = localDateTime.minusMinutes(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"));
                    }
                    content = content + time + ",123,100\n";
                    //content = content +  i + ",456," + newString + ",100\n";
                    log.info("content:\n" + content);

                    BufferedWriter bw = new BufferedWriter(new FileWriter(filePath));
                    bw.write(content);
                    bw.close();

                    try {
                        Thread.sleep(20000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String roundMinutes(int sec) {
        Clock minuteTickingClock = Clock.tickMinutes(ZoneId.systemDefault());
        LocalDateTime now = LocalDateTime.now(minuteTickingClock);
        LocalDateTime roundCeiling = now.plusSeconds(sec);
        return roundCeiling.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"));
    }
}
