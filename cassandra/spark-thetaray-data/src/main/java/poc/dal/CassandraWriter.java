package poc.dal;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by eyallevy on 09/01/17.
 */
public class CassandraWriter {
    public void writeProxyData(JavaRDD rdd) {
        System.out.println("33333333333333333333333_Inserting Data to cassandra");
        javaFunctions(rdd).writerBuilder("thetaray", "proxy", CassandraJavaUtil.mapToRow(String.class)).saveToCassandra();
    }

//    public void writeProxyData(Dataset<Row> rdd) {
//        System.out.println("33333333333333333333333_Inserting Data to cassandra");
//        javaFunctions(rdd).writerBuilder("thetaray", "proxy", CassandraJavaUtil.mapToRow(Proxy.class)).saveToCassandra();
//    }
}
