# hands-on
hands-on project solving the Shipping Priority Query (Q3) of the TPC-H benchmark partially.

## query
``` sql
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
```

## EC2 setup
The folloing steps help to initialize an AWS EC2 cluster w/ installations of Apache Spark and Apache Flink in Standalone mode.
To run a EC2 Apache Spark/Flink cluster, the follwing values are required:
 1. `[accessKeyID]` and `[secretAccessKey]`: Access keys used to sign programmatic requests to AWS ([see AWS Documentation](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html))
 1. `[username]` and `[pathToPemFile]`: Username and private key file to connect to AWS EC2 instances ([see AWS Documentation](http://docs.aws.amazon.com/gettingstarted/latest/wah/getting-started-prereq.html#create-a-key-pair))
 1. `[region]`: AWS region, e.g., eu-west-1 ([see AWS Documentation](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html))
 1. `[instaneType]`: Type of cluster nodes, e.g., t2.medium ([see AWS Documentation](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html))
 1. `[clusterSize]`: Number of slave nodes in the cluster
 1. `[clusterName]`: Name of the cluster to be launched (might be equal to `[username]`)


To initiaize and configure an EC2 cluster from a Unix shell (such as bash), proceed as follows:
 1. Set the AWS Access Key ID and Secret Access Key for the authentification
    ``` sh
    export AWS_ACCESS_KEY_ID=[accessKeyID]; export AWS_SECRET_ACCESS_KEY=[secretAccessKey]
    ```

 1. Initialize new AWS cluster using spark-ec2 script found in the spark binary package (extracted to `~/spark`). Note, this command starts a preconfigured cluster based on the Hadoop1 stack. To use the Hadoop2 stack apply the addiional parmeter `--hadoop-major-version=2` and adapt the `install_flink.sh` script accordingly (own responsibility).
    ``` sh
    ~/spark/ec2/spark-ec2 -k [username] -i [pathToPemFile] --region=[region] -s [clusterSize] --instance-type=[instaneType] --copy-aws-credentials launch [clusterName]
    ```
 wait some minutes until the following message occurs
    ```
    Spark standalone cluster started at http://[master]:8080
    ```
 keep the address of the master node (`[master]`)

 1. Connect to the EC2 master node
    ``` sh
    ssh -i [pathToPemFile] root@[master]
    ```

 1. Start sreen session on the master (optional)
    ``` sh
    screen
    ```

 1. Clone repository and install Apache Maven and Flink
    ``` sh
    git clone -b ec2 https://github.com/DBDA15/hands-on.git
    . hands-on/install_mvn.sh
    . hands-on/install_flink.sh
    . hands-on/tag_cluster.sh
    ```

## Running experiments
To run the experiments set `[cores]` to the number of executor cores to be used

 1. Distribute input files
    ``` sh
    $HADOOP_DIR/bin/start-all.sh
    $HADOOP_DIR/bin/hadoop distcp s3n://dbda-hands-on/data/lineitem.tbl s3n://dbda-hands-on/data/orders.tbl
    s3n://dbda-hands-on/data/lineitem.small.tbl s3n://dbda-hands-on/data/orders.small.tbl hdfs://$MASTER:9000/
    $HADOOP_DIR/bin/stop-all.sh
    ```

 1. Start Apache Spark
    ``` sh
    init-spark.sh
    ```
Run the experiment
    ``` sh
    cd hands-on/spark/tpch-task_java7/
    mvn clean install
    $SPARK_DIR/bin/spark-submit --total-executor-cores [cores] --class de.hpi.fgis.tpch.Q3 --master spark://$MASTER:7077 target/tpch-task_java7-0.0.1-SNAPSHOT.jar hdfs://$MASTER:9000/lineitem.tbl hdfs://$MASTER:9000/orders.tbl hdfs://$MASTER:9000/spark/hands-on
    ```

 1. Start Apache Flink
    ``` sh
    init-flink.sh
    ```
Run the experiment
    ``` sh
    cd hands-on/flink/tpch-task_java7/
    mvn clean install
    $FLINK_DIR/bin/flink run --parallelism [cores] --class de.hpi.fgis.tpch.Q3 -m $MASTER:6123 target/tpch-task_java7-0.0.1-SNAPSHOT.jar hdfs://$MASTER:9000/lineitem.tbl hdfs://$MASTER:9000/orders.tbl hdfs://$MASTER:9000/flink/hands-on
    ```

## Shutdown cluster
``` sh
~/spark/ec2/spark-ec2 --region=[region] destroy [clusterName]
```
