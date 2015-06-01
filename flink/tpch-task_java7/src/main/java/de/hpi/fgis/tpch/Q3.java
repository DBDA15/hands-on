package de.hpi.fgis.tpch;

import java.io.Serializable;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Flink job solving the Shipping Priority Query (Q3) of the TPC-H benchmark partially.<br/>
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

  public static void main(String[] args) throws Exception {
    // get job parameters
    final String lineItemFile = args[0];
    final String ordersFile = args[1];
    final String resultFile = args[2];
    final String DATE = "1995-03-15";

    // set up the execution environment
    final ExecutionEnvironment env = ExecutionEnvironment
        .getExecutionEnvironment();

    // load lineitems
    DataSet<LineItem> lineItems = env.readTextFile(lineItemFile)
    // map to lineitem
        .map(new MapFunction<String, LineItem>() {
          @Override
          public LineItem map(String line) throws Exception {
            return new LineItem(line);
          }
        })
        // filter by SHIPDATE>DATE
        .filter(new FilterFunction<LineItem>() {
          @Override
          public boolean filter(LineItem li) throws Exception {
            return li.SHIPDATE().compareTo(DATE) > 0;
          }
        });

    // aggregate sum(PRICE *(1-DISCOUNT)) by ORDERKEY
    DataSet<Tuple2<Integer, Double>> lineItemRevenue = lineItems
        .map(new MapFunction<Q3.LineItem, Tuple2<Integer, Double>>() {
          @Override
          public Tuple2<Integer, Double> map(LineItem li) throws Exception {
            return new Tuple2<Integer, Double>(li.ORDERKEY(), li.PRICE() * (1 - li.DISCOUNT()));
          }
        }).groupBy(0).sum(1);

    // load orders
    DataSet<Order> orders = env.readTextFile(ordersFile)
    // map to order
        .map(new MapFunction<String, Order>() {
          @Override
          public Order map(String line) throws Exception {
            return new Order(line);
          }
          // filter by SHIPDATE>DATE
        }).filter(new FilterFunction<Order>() {
          @Override
          public boolean filter(Order order) throws Exception {
            return order.ORDERDATE().compareTo(DATE) < 0;
          }
        });

    // join by ORDERKEY
    DataSet<Tuple2<Integer, Double>> joined = lineItemRevenue
        .join(orders)
        .where(0)
        .equalTo(0)
        // modify / cleanup tuple format
        .map(new MapFunction<Tuple2<Tuple2<Integer, Double>, Order>, Tuple2<Integer, Double>>() {
          @Override
          public Tuple2<Integer, Double> map(Tuple2<Tuple2<Integer, Double>,Order> value) throws Exception {
            return value.f0;
          }
        })
        // sort by revenue (desc) <-- only partition-wise
        .sortPartition(1, org.apache.flink.api.common.operators.Order.DESCENDING)
        ;
    // save results
    joined.writeAsCsv(resultFile, "\n", "\t");

    /*
    // execute program
    env.execute("TPC-H Q3");
    /*/
    System.out.println(env.getExecutionPlan());
    //*/
  }
  
  public static class LineItem extends Tuple5<Integer, Integer, Double, Double, String> implements Serializable {
    Integer ORDERKEY() {return f0;};
    Integer ITEM() {return f1;};
    Double PRICE() {return f2;};
    Double DISCOUNT() {return f3;};
    String SHIPDATE() {return f4;};
    // define default constructor (used for deserialization)
    public LineItem() { }
    public LineItem(String line) {
      this(line.split("\\|"));
    }
    public LineItem(String[] values) {
      super(
        Integer.parseInt(values[0]),
        Integer.parseInt(values[3]),
        Double.parseDouble(values[5]),
        Double.parseDouble(values[6]),
        values[10]);
    }
  }
  
  public static class Order extends Tuple2<Integer, String> implements Serializable {
    Integer ORDERKEY() {return f0;};
    String ORDERDATE() {return f1;};
    // define default constructor (used for deserialization)
    public Order() { }
    public Order(String line) {
      this(line.split("\\|"));
    }
    public Order(String[] values) {
      super(Integer.parseInt(values[0]), values[4]);
    }
  }
}
