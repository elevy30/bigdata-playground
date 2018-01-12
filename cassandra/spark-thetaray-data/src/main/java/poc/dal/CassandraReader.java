package poc.dal;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import poc.model.Proxy;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

/**
 * Created by eyallevy on 09/01/17.
 */
public class CassandraReader {
    public JavaPairRDD<Integer, Proxy> readProxyFromCassandra(JavaSparkContext sc){
        JavaPairRDD<Integer, Proxy> productsRDD = javaFunctions(sc)
                .cassandraTable("thetaray", "proxy", CassandraJavaUtil.mapRowTo(Proxy.class))
                .keyBy((Function<Proxy, Integer>) product -> product.getId());

        return productsRDD;
    }
}
