package de.hpi.fgis.tpch;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Spark job solving the Shipping Priority Query (Q3) of the TPC-H benchmark partially.<br/>
 * <br/>
 * <b>Query:</b>
<pre>
select l.ORDERKEY, sum(EXTENDEDPRICE*(1-DISCOUNT)) as revenue
  from
    ORDERS o,
    LINEITEM l
    where
      l.ORDERKEY = o.ORDERKEY
      and ORDERDATE < date '[DATE]'
      and SHIPDATE > date '[DATE]'
      group by
        l.ORDERKEY
        order by
          revenue desc
</pre>
 */
public class Q3 {
  public static void main(String[] args) {
    // get job parameters
    final String lineItemFile = args[0];
    final String ordersFile = args[1];
    final String resultFile = args[2];
    final String DATE = "1995-03-15";
    
    // initialize spark environment 
    SparkConf config = new SparkConf().setAppName(Q3.class.getName());
    config.set("spark.hadoop.validateOutputSpecs", "false");
    try(JavaSparkContext context = new JavaSparkContext(config)) {
      
      // load lineitems
      JavaPairRDD<Integer, LineItem> lineItems = context
          .textFile(lineItemFile)
          // map to key(ORDERKEY)-value(lineitem)-pair
          .mapToPair( (String line) -> {
            LineItem li = new LineItem(line);
            return new Tuple2<Integer, LineItem>(li.ORDERKEY, li);
          })
          // filter by SHIPDATE>DATE
          .filter( (Tuple2<Integer, LineItem> v1) -> v1._2.SHIPDATE.compareTo(DATE)>0 );
      
      // aggregate sum(PRICE *(1-DISCOUNT)) by ORDERKEY
      JavaPairRDD<Integer, Double> lineItemRevenue = lineItems
          .aggregateByKey(0D,
            // aggregate within partitions
            (Double v1, LineItem li)->v1 + li.PRICE * (1 - li.DISCOUNT)
            ,
            // aggregate between partitions
            (Double v1, Double v2) -> v1 + v2);
      
      // load orders
      JavaPairRDD<Integer, Void> ordersIds = context
          .textFile(ordersFile)
          // map to key(ORDERKEY)-value(order)-pair  
          .mapToPair( (String line) -> {
            Order o = new Order(line);
            return new Tuple2<Integer, Order>(o.ORDERKEY, o);
          } )
          // filter by SHIPDATE>DATE
          .filter( (Tuple2<Integer, Order> v1) -> v1._2.ORDERDATE.compareTo(DATE)<0 )
          // remove order date from pair
          .mapValues( (Order o) -> null );
      
      // join by ORDERKEY
      JavaPairRDD<Integer, Tuple2<Double, Void>> joined = 
          lineItemRevenue.join(ordersIds);

      // modify tuple format
      joined.mapToPair( (Tuple2<Integer, Tuple2<Double, Void>> tuple) -> 
          new Tuple2<Tuple2<Integer, Double>, Void>(new Tuple2<>(tuple._1, tuple._2._1), null)
        )
        // sort by revenue (desc)
        .sortByKey(new RevenueComp(), false)
        // cleanup tuple format
        .mapToPair( (Tuple2<Tuple2<Integer, Double>, Void> tuple) -> tuple._1 )
        // save results
        .saveAsTextFile(resultFile);
    }
  }
  static class LineItem implements Serializable {
    Integer ORDERKEY;
    String SHIPDATE;
    Double PRICE;
    Double DISCOUNT;
    
    LineItem(String line) {
      String[] values = line.split("\\|");
      ORDERKEY = Integer.parseInt(values[0]);
      SHIPDATE = values[10];
      PRICE = Double.parseDouble(values[5]);
      DISCOUNT = Double.parseDouble(values[6]);
    }
  }
  static class Order implements Serializable {
    Integer ORDERKEY;
    String ORDERDATE;
    
    Order(String line) {
      String[] values  = line.split("\\|");
      
      ORDERKEY = Integer.parseInt(values[0]);
      ORDERDATE = values[4];
    }
  }
  static class RevenueComp implements Comparator<Tuple2<Integer, Double>>, Serializable {
    public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
      return Double.compare(o1._2, o2._2);
    }
  }
}
