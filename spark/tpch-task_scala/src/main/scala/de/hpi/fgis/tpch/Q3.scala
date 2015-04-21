package de.hpi.fgis.tpch

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

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
object Q3 extends App {
  case class LineItem(ORDERKEY: Int, SHIPDATE: String, PRICE: Double, DISCOUNT: Double) {
    def this(values: Array[String]) =
      this(values(0).toInt, values(10), values(5).toDouble, values(6).toDouble)
    def asTuple = (ORDERKEY, this)
  }
  case class Order(ORDERKEY: Int, ORDERDATE: String) {
    def this(values: Array[String]) = this(values(0).toInt, values(4))
    def asTuple = (ORDERKEY, this)
  }

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

  val lineItems =
    // load lineitems
    context.textFile(lineItemFile)
      // map to key(ORDERKEY)-value(lineitem)-pair
      .map(line => new LineItem(line.split("\\|")).asTuple)
      // filter by SHIPDATE>DATE
      .filter(_._2.SHIPDATE.compareTo(DATE) > 0)
      // aggregate sum(PRICE *(1-DISCOUNT)) by ORDERKEY
      .aggregateByKey(0D)(
        // aggregate within partitions
        (v, li) => v + li.PRICE * (1 - li.DISCOUNT),
        // aggregate between partitions
        (v1, v2) => v1 + v2)

  val orders =
    // load orders
    context.textFile(ordersFile)
      // map to key(ORDERKEY)-value(order)-pair  
      .map(line => new Order(line.split("\\|")).asTuple)
      // filter by SHIPDATE>DATE
      .filter(_._2.ORDERDATE.compareTo(DATE) < 0)
      // remove order date from pair
      .mapValues(order => Nil)

  val joined =
    // join by ORDERKEY
    lineItems.join(orders)

  // modify tuple format for sorting
  joined.map(tuple => ((tuple._2._1, tuple._1), Nil))
    // sort by revenue (desc)
    .sortByKey(false)
    // cleanup tuple format
    .map(tuple => (tuple._1._2, tuple._1._1))
    // save results
    .saveAsTextFile(resultFile)
}