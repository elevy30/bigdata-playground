package poc.hadoop.common;


import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;


public class HadoopFileHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopFileHelper.class);

    public static final String FS_DEFAULT_NAME = "fs.default.name";

    protected String executionModeStr;
    
    private StreamCopyHelper copier = new StreamCopyHelper();
    private ConfigurationHelper configurationHelper = new ConfigurationHelper();


    protected Configuration remoteConf;
    protected Configuration localConf;
    private Configuration systemConfiguration;

    @PostConstruct
    public void initConfiguration() {
        localConf = configurationHelper.initConfiguration(true);
        remoteConf = configurationHelper.initConfiguration(false);
    }

    public void uploadFile(final InputStream in, final FileAlreadyExistsPolicy fileExistsPolicy) throws IOException {
        final OutputStream out = createFile(fileExistsPolicy);
        copier.copyAndClose(in, out); // LN TODO add append() support
        LOG.info("(Re-)created file pdr.getPattern()");
    }

    public void uploadFile(final byte[] line, final OutputStream outputStream) throws IOException {
        copier.copyLine(line, outputStream);
    }

    public DataOutputStream createFile(final FileAlreadyExistsPolicy fileExistsPolicy) throws IOException {
        final Configuration conf = getConfiguration();
        return runPrivileged(conf, new PrivilegedExceptionAction<DataOutputStream>() {
            @Override
            public DataOutputStream run() throws IOException {
                final Path path = getPath();
                LOG.debug("Creating: " + path.toString());
                FileSystem fs = getFileSystem(path, conf);
                if (shouldContinueIfFileExists(fileExistsPolicy, path, fs)) {
                    FSDataOutputStream out = fs.create(path);
                    return out;
                } else {
                    assert FileAlreadyExistsPolicy.SILENTLY_ABORT == fileExistsPolicy;
                    return new DataOutputStream(new NullOutputStream()); // silently ignores whatever is written to it.
                }
            }
        });
    }

    public List<InputStream> getFileDataFromUriAsInputStream() throws IOException {
        final Configuration conf = getConfiguration();
        return runPrivileged(conf, new PrivilegedExceptionAction<List<InputStream>>() {
            @Override
            public List<InputStream> run() throws IOException {
                final Path path = getPath();
                LOG.debug("Opening: {}", path);

                List<InputStream> inputStreams = new ArrayList<>();
                CompressionCodecFactory factory = new CompressionCodecFactory(conf);
                FileStatus[] files;
                FileSystem fs;
                try {
                    fs = getFileSystem(path, conf);
                    files = fs.listStatus(path, new CSVPathFilter());
                    if (files == null || files.length == 0) {
                        LOG.warn("Warning: no files match the path pdr.getPattern()");
                    } else {
                        for (FileStatus file : files) {
                            LOG.debug("Reading {}", file.getPath().toString());
                            CompressionCodec codec = factory.getCodec(file.getPath());
                            final FSDataInputStream fileInputStream = fs.open(file.getPath());
                            if (codec != null) {
                                inputStreams.add(codec.createInputStream(fileInputStream));
                            } else {
                                inputStreams.add(fileInputStream);
                            }
                        }
                    }
                } catch (IOException e) {
                    LOG.error("problem with reading file for pdr: " , e);
                    throw e; // rethrow!
                }
                return inputStreams;
            }
        });
    }


    public List<InputStreamObj> getFileDataFromUriAsInputStreamObj() throws IOException {
        final Configuration conf = getConfiguration();
        return runPrivileged(conf, new PrivilegedExceptionAction<List<InputStreamObj>>() {
            @Override
            public List<InputStreamObj> run() throws IOException {
                final Path path = getPath();
                LOG.debug("Opening: {}", path);

                List<InputStreamObj> inputStreams = new ArrayList<>();
                CompressionCodecFactory factory = new CompressionCodecFactory(conf);
                FileStatus[] files;
                FileSystem fs;
                try {
                    fs = getFileSystem(path, conf);
                    files = fs.listStatus(path, new CSVPathFilter());
                    if (files == null || files.length == 0) {
                        LOG.warn("Warning: no files match the path pdr.getPattern()");
                    } else {
                        for (FileStatus file : files) {
                            LOG.debug("Reading {}", file.getPath().toString());
                            CompressionCodec codec = factory.getCodec(file.getPath());
                            final FSDataInputStream fileInputStream = fs.open(file.getPath());
                            if (codec != null) {
                                inputStreams.add(new InputStreamObj(codec.createInputStream(fileInputStream), file.getLen()));
                            } else {
                                inputStreams.add(new InputStreamObj(fileInputStream, file.getLen()));
                            }
                        }
                    }
                } catch (IOException e) {
                    LOG.error("problem with reading file for pdr: ", e);
                    throw e; // rethrow!
                }
                return inputStreams;
            }
        });
    }


    public List<InputStream> getFileDataFromUriAsInputStream(final String fullPath, final boolean allFiles) throws IOException {
        final Configuration conf =  remoteConf;
        return getInputStreams(fullPath, allFiles, conf);
    }


    private List<InputStream> getInputStreams(final String fullPath, final boolean allFiles, final Configuration conf) throws IOException {
        return runPrivileged(conf, new PrivilegedExceptionAction<List<InputStream>>() {
            @Override
            public List<InputStream> run() throws IOException {
                final Path path = new Path(fullPath);
                LOG.debug("Opening: {}", path);

                List<InputStream> inputStreams = new ArrayList<InputStream>();
                CompressionCodecFactory factory = new CompressionCodecFactory(conf);
                try {
                    FileSystem fs = getFileSystem(path, conf);
                    FileStatus[] files = fs.listStatus(path, new SupportedFileTypeFilter());
                    if (files == null || files.length == 0) {
                        LOG.warn("Warning: no files match the path " + fullPath);
                    } else {
                        for (FileStatus file : files) {
                            LOG.debug("Reading {}", file.getPath().toString());
                            CompressionCodec codec = factory.getCodec(file.getPath());
                            final FSDataInputStream fileInputStream = fs.open(file.getPath());
                            if (codec != null) {
                                inputStreams.add(codec.createInputStream(fileInputStream));
                            } else {
                                inputStreams.add(fileInputStream);
                            }
                            if(!allFiles){
                                return inputStreams;
                            }
                        }
                    }
                } catch (IOException e) {
                    LOG.error("problem with reading file for pdr: " + fullPath, e);
                    throw e; // rethrow!
                }
                return inputStreams;
            }
        });
    }

    public InputStream getInputStream(final String fullPath) throws IOException {
        List<InputStream> inputStreamList = getFileDataFromUriAsInputStream(fullPath, false);
        InputStream inputStream = null;
        if(inputStreamList != null && inputStreamList.size() > 0 ){
            inputStream = inputStreamList.get(0);
        }
        return inputStream;
    }

    public boolean createFolder(final String folderName, final FileAlreadyExistsPolicy fileExistsPolicy) throws IOException {
        return runPrivileged(systemConfiguration, new PrivilegedExceptionAction<Boolean>() {
            @Override
            public Boolean run() throws IOException {
                LOG.info("Creating folder {} under HDFS: ", folderName);
                boolean isCreated = false;

                Path path = getPath();

                FileSystem fs = getFileSystem(null, systemConfiguration);
                if (shouldContinueIfFileExists(fileExistsPolicy, path, fs)) {
                    isCreated = fs.mkdirs(path);
                }
                return isCreated;
            }
        });
    }

    public <T> T runPrivilegedWithSystemConf(PrivilegedExceptionAction<T> action) throws IOException {
        return runPrivileged(systemConfiguration, action);
    }

    public <T> T runPrivileged(Configuration conf, PrivilegedExceptionAction<T> action) throws IOException {
        assert remoteConf.get(ConfigurationHelper.HADOOP_JOB_UGI) != null : "remote configuration must have a user";
        assert localConf.get(ConfigurationHelper.HADOOP_JOB_UGI) == null : "local configuration must NOT have a user";

        final String user = conf.get(ConfigurationHelper.HADOOP_JOB_UGI);
        final T result = (user == null) ? runUnprivileged(action) : runPrivileged(action);
        return result;
    }

    // invokes the action's run() method in a privileged context.
    private <T> T runPrivileged(PrivilegedExceptionAction<T> action) {
        T result;
        result = new HadoopDoAs<T>(remoteConf).runAsUser(action);
        return result;
    }

    // invokes the action's run() method directly
    private <T> T runUnprivileged(PrivilegedExceptionAction<T> action) throws IOException{
        try {
            return action.run();
        } catch (IOException e) {
        	LOG.error("problem to run action's run() method", e);
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // in the future, this will honor the tenant/user config. Currently this returns either remote or local config, based on ExecutionMode.
    public Configuration getSystemConfiguration() {
        return systemConfiguration;
    }

    public Configuration getConfiguration() {
        return remoteConf;
    }
    

    /**
     * Provides Hadoop FileSystem API, either LocalFileSystem or a remote FileSystem. mocked in tests.
     * <p/>
     * WARNING!!!  This method MUST be run within a privileged UGI context, or the result may fail to execute privileged APIs!!!
     *
     * @return the FileSystem object to work with.
     */
    public FileSystem getFileSystem(Path path, Configuration conf) throws IOException {
        if (path == null) {
            return FileSystem.get(conf);
        } else {
            assert path.isAbsolute() : "path must be absolute";
            // String user = conf.get(HADOOP_JOB_UGI);  String scheme = path.toUri().getScheme(); // assert (user != null && "hdfs".equals(scheme)) || (user == null && "file".equals(scheme)) : "path scheme and user credentials do not match
            return path.getFileSystem(conf);
        }
    }

    /**
     * Converts a destination string to a path into HDFS, by preceding it with the HDFS location + file-separator.
     * if the path contains folder(s), they should be compliant with Hadoop, that is be using FILE_SEPARATOR
     *
     */
    public Path getPath() {
        return new Path(remoteConf.get(FS_DEFAULT_NAME) + "/thetaray/permission/test");
    }

	public String getFullPathFolder() {
		String inputFolder = getFullPathUri();
		if (inputFolder != null && !inputFolder.isEmpty()) {
			inputFolder = inputFolder.substring(0, inputFolder.lastIndexOf('/') + 1);
		}
		return inputFolder;
	}

    /**
     * Converts a destination string to a path into HDFS, by preceding it with the HDFS location + file-separator.
     * if the path contains folder(s), they should be compliant with Hadoop, that is be using FILE_SEPARATOR
     * retunr the string of the full URI
     *
     * @param
     */
    public String getFullPathUri() {
        Path path = getPath();
        return path.toUri().toString();
    }

//    public Path validatePath(Path path, Path inputPath) {
//        Path fixedPath = path;
//        if(pdr.getType().equals(PDR_TYPE.CLASSPATH) && (path == null || path.toString().length() == 0)){
//            String inputFullPath = inputPath.toUri().toString();
//            String rootPath = inputFullPath.substring(0, inputFullPath.lastIndexOf('/') + 1);
//            fixedPath = new Path(rootPath + "/" + pdr.getPattern());
//        }
//        return fixedPath;
//    }
    private boolean shouldContinueIfFileExists(FileAlreadyExistsPolicy fileExistsPolicy, Path path, FileSystem fs) throws IOException {
        // handle the case where the file exists.
        final boolean shouldContinue;
        if (fs.exists(path)) {
            shouldContinue = fileExistsPolicy.shouldContinueIfFileExists();
            switch (fileExistsPolicy) {
                case OVERRIDE_IF_EXISTS:
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("File already exists in HDFS and will be overridden.");
                    }
                    break;
                case IGNORE_IF_EXISTS:
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("File already exists in HDFS and create new folder will be ignore.");
                    }
                    break;
                case FAIL_IF_EXISTS:
                    LOG.warn("File already exists in HDFS, aborting the creation operation.");
                    throw new FileExistsException("File already exists in HDFS: " + path.toString());
                case SILENTLY_ABORT:
                    // silently aborts...
                    break;
                case APPEND_IF_EXISTS:
                    // silently continues...
                    break;
                default:
                    throw new IllegalStateException("Sorry, unsupported policy, " + fileExistsPolicy);
            }
        } else {
            shouldContinue = true;
        }
        return shouldContinue;
    }
    
    public void deleteAllOptimizatonSessionFiles(String optimizationSessionID, boolean recursive) throws IOException {
        final Configuration configuration = getConfiguration();
        final Path path = getPath();
        final FileSystem fs = getFileSystem(path, configuration);
        Path sessionFolderPath = new Path(path.getParent().toUri().toString());
        if (sessionFolderPath != null && fs.exists(sessionFolderPath)) {
            fs.delete(sessionFolderPath, recursive);
        }
    }

    public void deleteIfExists(boolean recursive) throws IOException {
        final Configuration configuration = getConfiguration();
        final Path path = getPath();
        final FileSystem fs = getFileSystem(path, configuration);
        if (path != null && fs.exists(path)) {
            fs.delete(path, recursive);
        }
    }

    public void cleanWorkingSubdirectory(String subdirName) throws IOException {
        FileSystem fs = getFileSystem(null, systemConfiguration);
        Path tempPath = new Path(fs.getWorkingDirectory(), subdirName);
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
            LOG.warn("delete the temporary output path...");
        }
    }

    public boolean exists() throws IOException {
        final Path path = getPath();
        final FileSystem fileSystem = getFileSystem(path, getConfiguration());
        boolean exists = fileSystem.exists(path);
        return exists;
    }

    public Configuration getRemoteConfiguration() {
        return remoteConf;
    }

    public Configuration getLocalConfiguration() {
        return localConf;
    }

    public void mergeReducedFiles(String srcDir, String dstFile) {
            Configuration config = getConfiguration();
            mergeReducedFiles(srcDir,dstFile,config);

    }
    
    public void mergeReducedFiles(String srcDir, String dstFile, Configuration config) {
        try {
            FileSystem fs = FileSystem.get(config);
            Path destFilePath = new Path(dstFile);
            Path srcDirPath = new Path(srcDir);
            if (!fs.exists(srcDirPath)) {
                return;
            }
            if (fs.exists(destFilePath)) {
                fs.delete(destFilePath, true);
            }

            FileUtil.copyMerge(fs, srcDirPath, fs, destFilePath, false, config, null);
        } catch (IOException e) {
            throw new IllegalStateException("Sorry, could not merge files, due to: " + e.getMessage(), e);
        }

    }

    private static class CSVPathFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            return path.getName().endsWith("csv");
        }
    }
    
    private static class SupportedFileTypeFilter implements PathFilter {
        
        @Override
        public boolean accept(Path path) {
//            return path.getName().endsWith(SupportedFileType.CSV.getText()) || path.getName().endsWith(SupportedFileType.JSON.getText());
            return true;
        }
    }

	public void uploadFile(InputStream inputStream, OutputStream outputStream) throws IOException {
		copier.copyAndClose(inputStream, outputStream);
		
	}
	public void setCopier(StreamCopyHelper copier) {
		this.copier = copier;
	}

	public void setConfigurationHelper(ConfigurationHelper configurationHelper) {
		this.configurationHelper = configurationHelper;
	}

	public void setExecutionModeStr(String executionModeStr) {
		this.executionModeStr = executionModeStr;
	}

}
