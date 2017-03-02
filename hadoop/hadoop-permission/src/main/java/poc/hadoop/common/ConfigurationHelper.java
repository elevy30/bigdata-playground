package poc.hadoop.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;


public class ConfigurationHelper {

	public static final String HADOOP_JOB_UGI = "hadoop.job.ugi";

	public ConfigurationHelper() {
        // blank
	}

	public Configuration initConfiguration(boolean localConfiguration) {
		//System.setProperty( "java.security.krb5.conf", "/etc/krb5.conf");

		Configuration conf = new Configuration();
		if (!localConfiguration){
			//configuration.set(MAPRED_JOB_TRACKER,mapredJobTracker + ":" + mapredJobTrackerPort);
			//conf.set(FS_DEFAULT_NAME,   fsDefaultName    + ":" + fsDefaultPort);
			//configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			//configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

			conf.set(HADOOP_JOB_UGI,    "hduser");
			conf.set("fs.defaultFS", "hdfs://localhost:9000");
			System.out.print("Conf......");
		}
		return conf;
	}

	public Configuration initKerberosConfiguration(boolean localConfiguration) {
		System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
//		System.setProperty("java.security.krb5.realm","elevy");
//		System.setProperty("java.security.krb5.kdc","elevy");
//		System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
//		System.setProperty("java.security.auth.login.config","C:/mongodb/UnixKeytab/gss-jaas.conf");

		Configuration conf = new Configuration();
		if (!localConfiguration){
			conf.set(HADOOP_JOB_UGI,    "hduser");
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
}
