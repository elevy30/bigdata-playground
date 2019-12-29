package poc;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.cassandra.thrift.SuperColumn;

import java.util.Properties;

/**
 * Created by eyallevy on 04/01/17.
 */
public class CassandraClient {

    public static void main(String[] args) {
        AstyanaxContext.Builder builder = new AstyanaxContext.Builder()
                .forCluster("Test Cluster")
                .forKeyspace("thetaray")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                        .setPort(9160)
                        .setMaxConnsPerHost(1)
                        .setSeeds("127.0.0.1:9160")
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

        AstyanaxContext<Keyspace> context = builder.buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getClient();

        System.out.println("started context and get the keyspace ");

        ColumnFamily<String, String> CF_USER_INFO = new ColumnFamily<String, String>(
                "Standard1",              // Column Family Name
                StringSerializer.get(),   // Key Serializer
                StringSerializer.get());  // Column Serializer
        ColumnFamily<Long, String> CF_COUNTER1 = new ColumnFamily<Long, String>(
                        "CounterColumnFamily",
                        LongSerializer.get(),
                        StringSerializer.get());

        try {
            Properties standard1 = keyspace.getColumnFamilyProperties("Standard1");
            if (standard1 == null) {
                keyspace.createColumnFamily(CF_USER_INFO, null);
            }


        } catch (ConnectionException e) {
            e.printStackTrace();
        }


        try {
            Properties  standard2 = keyspace.getColumnFamilyProperties("CounterColumnFamily");
            if (standard2 == null) {
                keyspace.createColumnFamily(CF_COUNTER1, null);
            }
        } catch (ConnectionException e) {
            e.printStackTrace();
            try {
                keyspace.createColumnFamily(CF_COUNTER1, null);
            } catch (ConnectionException e1) {
                e1.printStackTrace();
            }
        }


        try {
            keyspace.prepareColumnMutation(CF_COUNTER1, 0l, "CounterColumn1")
                    .incrementCounterColumn(1)
                    .execute();
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
        // Inserting data
        MutationBatch m = keyspace.prepareMutationBatch();

        System.out.println("prepare batch ");

        m.withRow(CF_USER_INFO, "acct1234")
                .putColumn("firstname", "john", null)
                .putColumn("lastname", "smith", null)
                .putColumn("address", "555 Elm St", null)
                .putColumn("age", 30, null);
//
        m.withRow(CF_USER_INFO, "acct1234")
                .incrementCounterColumn("CounterColumn1", 1);

        try {
            OperationResult<Void> result = m.execute();
            System.out.println("result " + result);
        } catch (ConnectionException e) {
            e.printStackTrace();
        }


        OperationResult<ColumnList<String>> result = null;
        try {
            result = keyspace.prepareQuery(CF_USER_INFO)
                    .getKey("acct1234")
                    .execute();
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
        ColumnList<String> columns = result.getResult();

        // Lookup columns in response by name
        int age = columns.getColumnByName("age").getIntegerValue();
        System.out.println("age " + age);
        //long counter   = columns.getColumnByName("loginCount").getLongValue();
        String address = columns.getColumnByName("address").getStringValue();
        System.out.println("address " + address);

        // Or, iterate through the columns
        for (Column<String> c : result.getResult()) {
            System.out.println(c.getName());
        }
    }
}
