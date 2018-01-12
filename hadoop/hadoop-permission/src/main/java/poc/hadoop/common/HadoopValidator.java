package poc.hadoop.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: levyey
 * Date: 15/03/14
 * Time: 11:58
 * To change this template use File | Settings | File Templates.
 */
public class HadoopValidator {
    protected static final Logger LOGGER = LoggerFactory.getLogger(HadoopValidator.class);

    public static void validatePath(Path path, Configuration configuration) {
        // make sure the output path does not exist
        try {
            FileSystem ofs = path.getFileSystem(configuration);
            if (ofs.exists(path)) {
                ofs.delete(path, true);
            }
        } catch (IOException e) {
            LOGGER.error("Unable to get file system configurations: " + configuration.toString());
            throw new RuntimeException(e);
        }

        // make sure the temporary output path does not exist
        try {
            FileSystem fs = FileSystem.get(configuration);
            Path tempPath = new Path(fs.getWorkingDirectory(), "output");
            if (fs.exists(tempPath)) {
                fs.delete(tempPath, true);
                LOGGER.warn("delete the temporary output path...");
            }
        } catch (IOException e) {
            LOGGER.error("Unable to delete temporary files: " + "output");
            throw new RuntimeException(e);
        }
    }

}
