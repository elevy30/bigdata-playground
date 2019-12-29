package poc.hadoop.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StopWatch;

import javax.crypto.Cipher;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;

/**
 * Created by eyallevy on 24/01/17.
 */
public class HdfsKerberos {

    public static void main(String[] args) {
        //check the JAVA JCE deployment
        int maxKeyLen = 0;
        try {
            maxKeyLen = Cipher.getMaxAllowedKeyLength("AES");
            System.out.println(maxKeyLen);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        HdfsKerberos hdfsCommand = new HdfsKerberos();
        Configuration confKerb = hdfsCommand.initKerberosConfiguration(false);
        hdfsCommand.createFolder(confKerb);

//      writer.writeToFile(configuration, "/user/hduser/kerberos5");
//      writer.appendToFileWithDistributedFileSystem(configuration, "/user/hduser/test/hello.txt");
    }

    public Configuration initKerberosConfiguration(boolean localConfiguration) {
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
		System.setProperty("java.security.krb5.realm","thetaray.com");
		System.setProperty("java.security.krb5.kdc","thetaray.com");
//		System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
//		System.setProperty("java.security.auth.login.config","C:/mongodb/UnixKeytab/gss-jaas.conf");

        Configuration conf = new Configuration();
        if (!localConfiguration){
            conf.set("hadoop.job.ugi",    "hduser");
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("fs.defaultFS", "hdfs://eyallevy:50475");
            conf.set("fs.webhdfs.impl", org.apache.hadoop.hdfs.web.WebHdfsFileSystem.class.getName());
            conf.set("com.sun.security.auth.module.Krb5LoginModule", "required");
            conf.set("debug", "true");
            //conf.set("ticketCache", "DIR:/etc/");
            System.out.print("Conf......");

            UserGroupInformation.setConfiguration(conf);
            try {
                UserGroupInformation.loginUserFromKeytab("elevy/admin@thetaray.com", "/opt/admin.keytab");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return conf;
    }

    public String createFolder(Configuration conf) {
        return runWithPrivileged(conf, new PrivilegedExceptionAction<String>() {
            FileSystem hdfs = null;
            BufferedWriter br = null;

            @Override
            public String run() throws IOException {
                try {
                    FileSystem fs = FileSystem.get(conf);
                    if (fs.mkdirs(new Path("/user/hduser/testKerb4")))
                        System.out.print("Directory created...");
                } catch (IOException e) {
                    e.printStackTrace();
                }

                return "done";
            }
        });
        //return "done";
    }

    public String writeToFile(Configuration conf, String filePath) {
        return runWithPrivileged(conf, new PrivilegedExceptionAction<String>() {
            FileSystem hdfs = null;
            BufferedWriter br = null;

            @Override
            public String run() throws IOException {
                try {
                    hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
                    Path file = new Path("hdfs://localhost:9000" + filePath);
                    if (hdfs.exists(file)) {
                        hdfs.delete(file, true);
                        System.out.println("file deleted");
                    }

                    OutputStream os = hdfs.create(file, () -> System.out.println("...bytes written: [ ]"));
                    br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
                    br.write("Hello World");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                } finally {
                    if (br != null) br.close();
                    if (hdfs != null) hdfs.close();
                }
                return "done";
            }
        });
    }

    public String appendToFile(Configuration conf, String filePath) {
        return runWithPrivileged(conf, new PrivilegedExceptionAction<String>() {
            FileSystem hdfs = null;
            BufferedWriter br = null;

            @Override
            public String run() throws IOException {
                try {
                    FileSystem hdfs = null;
                    hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);

                    Path file = new Path("hdfs://localhost:9000" + filePath);

                    OutputStream os = hdfs.append(file);
                    br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
                    br.write("Hello World again");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                } finally {
                    if (br != null) br.close();
                    if (hdfs != null) hdfs.close();
                }
                return "done";
            }
        });
    }

    public String appendToFileWithDistributedFileSystem(Configuration conf, String filePath) {
        return runWithPrivileged(conf, new PrivilegedExceptionAction<String>() {
            DistributedFileSystem hdfs = new DistributedFileSystem();
            BufferedWriter br = null;
            Path file = null;

            @Override
            public String run() throws IOException {
                try {
                    hdfs = (DistributedFileSystem) FileSystem.get(new URI("hdfs://localhost:9000"), conf);
                    file = new Path("hdfs://localhost:9000" + filePath);

                    checkLease(hdfs, file);

                    OutputStream os = hdfs.append(file);
                    br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
                    br.write("Hello World HHHHHHHHHHHHHHHHHHH");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                } finally {
                    //if (br != null) br.close();
                    if (hdfs != null) hdfs.close();
                }
                return "done";
            }
        });
    }

    private void checkLease(DistributedFileSystem dFileSystem, Path hdfspath) {
        System.out.println("try to recover file Lease : " + hdfspath);
        try {
            boolean b = dFileSystem.recoverLease(hdfspath);
            boolean isclosed = dFileSystem.isFileClosed(hdfspath);
            StopWatch sw = new StopWatch().start();
            while (!isclosed) {

                System.out.println("file open : " + hdfspath);
                if (sw.now() > 60 * 1000)
                    throw new RuntimeException();
                try {
                    Thread.currentThread().sleep(1000);
                } catch (InterruptedException e1) {
                }
                isclosed = dFileSystem.isFileClosed(hdfspath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // invokes the action's run() method in a privileged context.
    private <T> T runWithPrivileged(Configuration conf, PrivilegedExceptionAction<T> action) {
        T result;
        result = new HadoopDoAs<T>(conf).runAsUser(action);
        return result;
    }
}
