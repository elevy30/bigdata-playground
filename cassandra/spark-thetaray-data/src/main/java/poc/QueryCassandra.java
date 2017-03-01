package poc;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import shade.com.datastax.spark.connector.google.common.reflect.TypeToken;

import java.util.List;

/**
 * Created by eyallevy on 08/01/17.
 */
public class QueryCassandra {


    static String[] CONTACT_POINTS = {"127.0.0.1"};
    static int PORT = 9042;

    private Cluster cluster;

    private Session session;

    public static void main(String[] args) {

        QueryCassandra client = new QueryCassandra();

        try {

            client.connect(CONTACT_POINTS, PORT);
            client.querySchema();

        } finally {
            client.close();
        }
    }

    /**
     * Initiates a connection to the cluster
     * specified by the given contact point.
     *
     * @param contactPoints the contact points to use.
     * @param port          the port to use.
     */
    public void connect(String[] contactPoints, int port) {

        cluster = Cluster.builder()
                .addContactPoints(contactPoints)
                .withPort(port)
                .build();

        System.out.printf("Connected to cluster: %s%n", cluster.getMetadata().getClusterName());

        session = cluster.connect();
    }

    public void querySchema() {

        ResultSet results = session.execute("SELECT * FROM java_api.products;");

        System.out.printf("%-30s\t%-20s\t%-20s%n", "title", "album", "artist");
        System.out.println("-------------------------------+-----------------------+--------------------");

        for (Row row : results) {

            System.out.printf("%-30s\t%-20s\t%-20s%n",
                    row.getInt("id"),
                    row.getString("name"),
                    row.get("parents", new TypeToken<List<Integer>>(){}));

        }

    }


    /**
     * Closes the session and the cluster.
     */
    public void close() {
        session.close();
        cluster.close();
    }
}
