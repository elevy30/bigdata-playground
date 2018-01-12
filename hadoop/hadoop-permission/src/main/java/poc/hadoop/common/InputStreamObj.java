package poc.hadoop.common;

import java.io.InputStream;

public class InputStreamObj {

    private InputStream in;
    private long size;

    public InputStreamObj(InputStream in, long size) {
        this.in = in;
        this.size = size;
    }

    public InputStream getIn() {
        return in;
    }

    public long getSize() {
        return size;
    }


}
