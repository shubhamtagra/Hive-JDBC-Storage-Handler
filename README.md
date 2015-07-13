#Hive Storage Handler for JDBC#

The **Hive Storage Handler For JDBC** by [Qubole](www.qubole.com) helps users read from and write to JDBC databases using Hive, and also enabling them to run SQL queries to analyze data that resides in JDBC tables.
Optimizations such as [FilterPushDown](https://cwiki.apache.org/confluence/display/Hive/FilterPushdownDev) have also been added.


##Building from Source##
* Download the code from Github:
```
  $ git clone https://github.com/qubole/Hive-JDBC-storage-Handler.git
  $ cd Hive-JDBC-storage-Handler
```

* Build using Maven (add ```-DskipTests``` to build without running tests):

```
  $ mvn clean install -Phadoop-1
```

* The JARs for the storage handler can be found in the ```target/``` folder. Use ```qubole-hive-JDBC-0.0.4-jar-with-dependencies.jar``` in the hive session (see below).

##Usage##
* Add the JAR to the Hive session. ```<path-to-jar>``` is the path to the above mentioned JAR. For using this with Qubole hive, upload the JAR to an S3 bucket and provide its path.
  
``` 
  ADD JAR <path-to-jar>;
```

* Each record in the JDBC corresponds to a row in the Hive table.

* While creating the Hive table, use 
  
```
  STORED BY 'org.apache.hadoop.hive.jdbc.storagehandler.JdbcStorageHandler'
```
  
* For example, the following query would create a Hive table called 'HiveTable' that reads from a JDBC table called 'JDBCTable'.
  
```
DROP TABLE HiveTable;
CREATE EXTERNAL TABLE HiveTable(
  id INT,
  id_double DOUBLE,
  names STRING,
  test INT
)
STORED BY 'org.apache.hadoop.hive.jdbc.storagehandler.JdbcStorageHandler'
TBLPROPERTIES (
  "mapred.jdbc.driver.class"="com.mysql.jdbc.Driver",
  "mapred.jdbc.url"="jdbc:mysql://localhost:3306/rstore",
  "mapred.jdbc.username"="root",
  "mapred.jdbc.input.table.name"="JDBCTable",
  "mapred.jdbc.output.table.name"="JDBCTable",
  "mapred.jdbc.password"=""
);

```

##Sample Queries##

HIVE-JDBC Storage Handeler supports alomost all types of possible SQL queries. Some examples of supported queries are:
```
* select * from HiveTable;
* Select count(*) from HiveTable;
* select id from HiveTable where id > 50000;
* select names from HiveTable;
* select * from HiveTable where names like ‘D%’;
* SELECT * FROM HiveTable ORDER BY id DESC;
* select HiveTable_1.*, HiveTable_2.* from HiveTable_1 a join HiveTable_2 b 
  on (a.id = b.id) where a.id > 90000 and b.id > 97000 
* Insert Into Table HiveTable_1 select * from HiveTable_2
```

## Support For FilterPushDown ##

* Support for FilterPushDown has been added to the jar as described in the following [wiki] (https://cwiki.apache.org/confluence/display/Hive/FilterPushdownDev)
* To disable FilterPushDown 
```
 set hive.optimize.ppd = false;
```

