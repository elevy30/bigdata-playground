package poc.hadoop.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class HadoopDoAs<T> {

    private Configuration configuration;

    public HadoopDoAs(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * executes an action (e.g. map reduce job) as a specific user
     *
     * @return T
     */
    public T runAsUser(PrivilegedExceptionAction<T> privilegedExceptionAction) {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(configuration.get("hadoop.job.ugi"));

        try {
            return ugi.doAs(privilegedExceptionAction);
        } catch (IOException | InterruptedException e1) {
            throw new RuntimeException("Failed to execute action in " + HadoopDoAs.class.getSimpleName() + " due to a " + e1.getClass().getSimpleName(), e1);
        }
    }

}