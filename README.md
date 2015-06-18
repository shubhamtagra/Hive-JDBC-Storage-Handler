#Hive Storage Handler for Kinesis#

The **Hive Storage Handler For Kinesis** helps users read from and write to Kinesis Streams using Hive, enabling them to run queries to analyze data that resides in Kinesis.

##Building from Source##
* Download the code from Github:
  ```
    $ git clone https://github.com/divyanshu25/Hive-JDBC-storage-Handler.git
    $ cd Hive-JDBC-storage-handler
  ```

* Build using Maven (add ```-DskipTests``` to build without running tests):

  ```
    $ mvn clean install -Phadoop-1 -DSkipTests
  ```

* The JARs for the storage handler can be found in the ```target/``` folder. Use ```qubole-hive-JDBC-0.0.4-jar-with-dependencies.jar``` in the hive session (see below).