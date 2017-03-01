package poc.ddl;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by eyallevy on 09/01/17.
 */
public class Creator {
    public void createProxyTable(CassandraConnector connector) {
        System.out.println("111111111111111111111_Creating KeySpaces and tables");
        // Prepare the schema
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS thetaray");
            session.execute("CREATE KEYSPACE thetaray WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE thetaray.proxy (id INT PRIMARY KEY, cs_username TEXT)");
        }
    }
}
