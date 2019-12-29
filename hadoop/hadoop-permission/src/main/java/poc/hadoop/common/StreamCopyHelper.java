package poc.hadoop.common;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamCopyHelper {

    public void copyAndClose(InputStream in, OutputStream out) throws IOException {
        try {
            copyWithoutClosing(in, out);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    // blank, ignored
                }
            }
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    // blank, ignored
                }
            }
        }
    }

    public void copyWithoutClosing(InputStream inputStream, OutputStream outputStream) throws IOException {
        int read;
        byte[] bytes = new byte[1024];
        while ((read = inputStream.read(bytes)) != -1) {
            outputStream.write(bytes, 0, read);
        }
    }

    public void copyLine(byte[] bytes, OutputStream outputStream) throws IOException {
            outputStream.write(bytes);
    }
}
