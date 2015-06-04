package de.hpi.fgis.tpch;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem.WriteMode;

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
    final String resultFile = args.length>=3?args[2]:null;
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
        //// sort by revenue (desc) <-- only partition-wise
        //.sortPartition(1, org.apache.flink.api.common.operators.Order.DESCENDING)
        ;
    // save results
    if(resultFile!=null) {
      joined.writeAsCsv(resultFile, "\n", "\t", WriteMode.OVERWRITE);
    } else {
      joined.count();
    }

    /*
    System.out.println(env.getExecutionPlan());
    /*/
    // execute program & measure run-time
    String name = Q3.class.getName();
    long start = System.currentTimeMillis();
    JobExecutionResult result = env.execute(name);
    long runtime = System.currentTimeMillis()-start;
    long netRuntime = result.getNetRuntime();
    // print
    print(runtime, netRuntime, env.getParallelism(), -1, name);
    //*/
  }
  static void print(long time, long netTime, int cores, int slaves, String appName) throws IOException {
    String line = slaves+"\t"+cores+"\t"+time+"\t"+netTime;
    Files.write(Paths.get("runtime."+appName+".txt"), Arrays.asList(line), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    //System.out.println(line);
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
