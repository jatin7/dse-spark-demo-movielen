package no.sasoria.spark.app;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import no.sasoria.spark.cassandra.CassandraWriter;

public class SparkApp {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("insert-movies")
				.set("output.consistency.level", "1")
				.setMaster("local[*]");  		// disable for spark-submit
		

		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate();

		writeToCassandra(spark, createTable(spark));
	}

	/**
	 * Initializes spark with a table from a csv data source.
	 * @param spark
	 * @return dataframe for {@code movies}.
	 */
	private static Dataset<Row> createTable(SparkSession spark) {
		String inputLocation = SparkReader.resourceFolder + "movies.csv";		
		Dataset<Row> moviesDf = SparkReader.readCSV(spark, inputLocation);
		moviesDf.createOrReplaceTempView("movies");
		moviesDf.show();
		moviesDf.printSchema();
		
		return moviesDf;
	}

	/**
	 * Writes a dataframe to Cassandra. Keyspace and table must be defined through {@code cql} or 
	 * a cassandra driver.
	 * @param spark
	 * @param tableDf
	 */
	private static void writeToCassandra(SparkSession spark, Dataset<Row> tableDf) {
		CassandraWriter.writeDataFrame(spark, tableDf);
	}

}
