SINDY
=====

This example program finds unary inclusion dependencies within given CSV files using Apache Flink.

Execute it with:
`java -cp <...> -Dlog4j.properties=<URL to log4j.properties> [-Djava.rmi.server.hostname=<driver name/IP>] de.hpi.fgis.Sindy <URL pattern to input files> [--executor <host name>:<port> --jars <Sindy, fastutil, and JCommander jar or fatjar>] [--parallelism <number>] [--distinct-attribute-groups]` 

Explanation of parameters:
*   `-cp` is the class path that should include all the libraries mentioned in the `pom.xml`
*   `-Dlog4j.properties` helps to configure the logging, a pre-configured `log4j.properties` file is provided
*   `-Djava.rmi.server.hostname` tell the `RemoteCollectorOutputFormat` where to send job results, so this should be the IP or host name of the machine where you run this driver program; it is not always necessary as there is an auto-detect functionality for this
*   `--executor` points to a job manager that should execute the program, e.g., `tenemhead2:6123`; if omitted, the program will be executed locally
*  `--jars` is a list of jar that need to be shipped to the cluster for remote execution
*   `--parallelism` is the degree of parallelism with that this program shall be executed
*   `--distinct-attribute-groups` is used to trigger a variant of the algorithm that removes duplicate attribute groups in an intermediate processing step 