package poc.driver;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import poc.model.Product;
import poc.model.Sale;
import poc.model.Summary;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;


/**
 * Created by eyallevy on 08/01/17.
 */
public class SparkDriver implements Serializable {

    private transient SparkConf conf;

    public SparkDriver(SparkConf conf) {
        this.conf = conf;
    }

    public void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        generateData(sc);
        compute(sc);
        showResults(sc);
        sc.stop();
    }

    private void generateData(JavaSparkContext sc) {
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());

        System.out.println("111111111111111111111_Creating Keyspaces and tables");


        // Prepare the schema
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS java_api");
            session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE java_api.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
            session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
            session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
        }

        System.out.println("22222222222222222222222_Build Products RDD");
        // Prepare the products hierarchy
        List<Product> products = Arrays.asList(
                new Product(0, "All products", Collections.<Integer>emptyList()),
                new Product(1, "Product A", Arrays.asList(0)),
                new Product(4, "Product A1", Arrays.asList(0, 1)),
                new Product(5, "Product A2", Arrays.asList(0, 1)),
                new Product(2, "Product B", Arrays.asList(0)),
                new Product(6, "Product B1", Arrays.asList(0, 2)),
                new Product(7, "Product B2", Arrays.asList(0, 2)),
                new Product(3, "Product C", Arrays.asList(0)),
                new Product(8, "Product C1", Arrays.asList(0, 3)),
                new Product(9, "Product C2", Arrays.asList(0, 3))
        );
        JavaRDD<Product> productsRDD = sc.parallelize(products);

        System.out.println("33333333333333333333333_Inserting Products to cassandra");
        javaFunctions(productsRDD).writerBuilder("java_api", "products", CassandraJavaUtil.mapToRow(Product.class)).saveToCassandra();


        System.out.println("44444444444444444444444_Build Sales RDD");
        JavaRDD<Sale> salesRDD = productsRDD.filter((Function<Product, Boolean>) product -> product.getParents().size() == 2)
                .flatMap((FlatMapFunction<Product, Sale>) product -> {
                    System.out.println("555555555555555555555_Build Sales for product " + product.getName());
                    Random random = new Random();
                    List<Sale> sales = new ArrayList<>(1000);
                    for (int i = 0; i < 1000; i++) {
                        sales.add(new Sale(UUID.randomUUID(), product.getId(), BigDecimal.valueOf(random.nextDouble())));
                    }
                    return sales.iterator();
                });

        System.out.println("66666666666666666666666_Inserting Sales to cassandra");
        javaFunctions(salesRDD).writerBuilder("java_api", "sales", CassandraJavaUtil.mapToRow(Sale.class)).saveToCassandra();
    }


    private void compute(JavaSparkContext sc) {
        JavaPairRDD<Integer, Product> productsRDD = javaFunctions(sc)
                .cassandraTable("java_api", "products", CassandraJavaUtil.mapRowTo(Product.class))
                .keyBy((Function<Product, Integer>) product -> product.getId());


        JavaPairRDD<Integer, Sale> salesRDD = javaFunctions(sc)
                .cassandraTable("java_api", "sales", CassandraJavaUtil.mapRowTo(Sale.class))
                .keyBy((Function<Sale, Integer>) sale -> sale.getProduct());

        JavaPairRDD<Integer, Tuple2<Sale, Product>> joinedRDD = salesRDD.join(productsRDD);

        //(PairFlatMapFunction<Tuple2<Integer, Tuple2<Sale, Product>>, Integer, BigDecimal>)
        JavaPairRDD<Integer, BigDecimal> allSalesRDD = joinedRDD.flatMapToPair( input -> {
            Tuple2<Sale, Product> saleWithProduct = input._2();
            List<Tuple2<Integer, BigDecimal>> allSales = new ArrayList<>(saleWithProduct._2().getParents().size() + 1);
            allSales.add(new Tuple2<>(saleWithProduct._1().getProduct(), saleWithProduct._1().getPrice()));
            for (Integer parentProduct : saleWithProduct._2().getParents()) {
                allSales.add(new Tuple2<>(parentProduct, saleWithProduct._1().getPrice()));
            }
            return allSales.iterator();
        });

        JavaRDD<Summary> summariesRDD = allSalesRDD
                .reduceByKey((Function2<BigDecimal, BigDecimal, BigDecimal>) (v1, v2) -> v1.add(v2))
                .map((Function<Tuple2<Integer, BigDecimal>, Summary>) input -> new Summary(input._1(), input._2()));

        javaFunctions(summariesRDD).writerBuilder("java_api", "summaries", CassandraJavaUtil.mapToRow(Summary.class)).saveToCassandra();
    }

    private void showResults(JavaSparkContext sc) {
        JavaPairRDD<Integer, Summary> summariesRdd = javaFunctions(sc)
                .cassandraTable("java_api", "summaries", CassandraJavaUtil.mapRowTo(Summary.class))
                .keyBy((Function<Summary, Integer>) summary -> summary.getProduct());

        JavaPairRDD<Integer, Product> productsRdd = javaFunctions(sc)
                .cassandraTable("java_api", "products", CassandraJavaUtil.mapRowTo(Product.class))
                .keyBy((Function<Product, Integer>) product -> product.getId());

        JavaRDD<Tuple2<Product, Optional<Summary>>> values = productsRdd.leftOuterJoin(summariesRdd).values();
        JavaRDD<Tuple2<Product, Optional<Summary>>> sortedValues = values.sortBy(tuple -> tuple._1().getId(), true, 1);

        List<Tuple2<Product, Optional<Summary>>> results = sortedValues.collect();



        for (Tuple2<Product, Optional<Summary>> result : results) {
            System.out.println(result);
        }
    }



}
