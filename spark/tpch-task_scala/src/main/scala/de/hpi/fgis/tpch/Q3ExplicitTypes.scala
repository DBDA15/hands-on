package de.hpi.fgis.tpch

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import de.hpi.fgis.tpch.Q3.LineItem
import de.hpi.fgis.tpch.Q3.Order
import org.apache.spark.SparkContext
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.charset.StandardCharsets

/**
 * Spark job solving the Shipping Priority Query (Q3) of the TPC-H benchmark partially w/ explicit type statements.<br/>
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
object Q3ExplicitTypes extends App {
  // get parameters
  val lineItemFile = args(0)
  val ordersFile = args(1)
  val resultFile = args(2)
  val DATE = "1995-03-15"

  // initialize spark environment
  val conf = new SparkConf()
  conf.setAppName(Q3.getClass.getName)
  conf.set("spark.hadoop.validateOutputSpecs", "false");
  val context = new SparkContext(conf)

  val lineItems: RDD[(Int, Double)] =
    // load lineitems
    context.textFile(lineItemFile)
      // map to key(ORDERKEY)-value(lineitem)-pair
      .map[(Int, LineItem)]((line: String) => new LineItem(line.split("\\|")).asTuple)
      // filter by SHIPDATE>DATE
      .filter((lineItemTuple: (Int, LineItem)) => lineItemTuple._2.SHIPDATE.compareTo(DATE) > 0)
      // aggregate sum(PRICE *(1-DISCOUNT)) by ORDERKEY
      .aggregateByKey[Double](0D)(
        // aggregate within partitions
        (v: Double, li: LineItem) => v + li.PRICE * (1 - li.DISCOUNT),
        // aggregate between partitions
        (v1: Double, v2: Double) => v1 + v2)

  val orders: RDD[(Int, Unit)] =
    // load orders
    context.textFile(ordersFile)
      // map to key(ORDERKEY)-value(order)-pair  
      .map[(Int, Order)]((line: String) => new Order(line.split("\\|")).asTuple)
      // filter by SHIPDATE>DATE
      .filter((orderTuple: (Int, Order)) => orderTuple._2.ORDERDATE.compareTo(DATE) < 0)
      // remove order date from pair
      .mapValues[Unit]((order:Order) => Nil)

  val joined: RDD[(Int, (Double, Unit))] =
    // join by ORDERKEY
    lineItems.join[Unit](orders)

  // modify tuple format
  val result = joined.map[((Double, Int), Unit)]((tuple: (Int, (Double, Unit))) => ((tuple._2._1, tuple._1), Nil))
    //// sort by revenue (desc)
    //.sortByKey(false)
    // cleanup tuple format
    .map[(Int, Double)]((tuple: ((Double, Int), Unit)) => (tuple._1._2, tuple._1._1))
    
    
  // execute program & measure run-time
  val start = System.currentTimeMillis;
  // save results
  result.saveAsTextFile(resultFile);
  // print
  print(System.currentTimeMillis-start, context.getExecutorStorageStatus.length)

  def print (time:Long, slaves:Int) : Unit = {
    val line = slaves+"\t"+time+"\n"
    Files.write(Paths.get("runtime."+conf.get("spark.app.name")+".txt"), line.getBytes(StandardCharsets.UTF_8))
    //println(line);
  }
}