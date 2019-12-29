package poc.commons.time;

import java.io.Serializable;

public class StreamTimer implements Serializable{
    public long startTime;
    public long endTime;

    public long totalDuration;

    public long getDuration() {
        return endTime - startTime;
    }

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void stop() {
        endTime = System.currentTimeMillis();
    }

    public void updateTotal(){
        totalDuration += getDuration();
    }

    public void reset(){
        startTime = 0;
        endTime = 0;
        totalDuration = 0;
    }

}