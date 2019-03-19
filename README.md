# spark-demo

## Requieres
* [Apache Spark](https://spark.apache.org/downloads.html)
* [Apache Cassandra](http://cassandra.apache.org/download)
* Maven

## Cassandra

Running CassandraApp and SparkApp's `writeToCassandra()` results in the follwing Cassandra table.
```
cqlsh> select * FROM movielens.movies 
   ... ;

 id     | genres                         | title
--------+--------------------------------+-------------------------------------------------------------------------
   1683 | Drama|Romance                  | Wings of the Dove, The (1997)
  97172 | Animation|Comedy|Horror|IMAX   | Frankenweenie (2012)
  26941 | Drama|War                      | Pretty Village, Pretty Flame (Lepa sela lepo gore) (1996)
   7479 | Drama                          | Watch on the Rhine (1943)                                                     
   ...
```

## Spark
SparkSql is implemented in SparkApp. `selectMovie(spark, 1683)` returns a Dataframe with the following result.
```
+----+--------------------+
|  id|               title|
+----+--------------------+
|1683|Wings of the Dove...|
+----+--------------------+
```
