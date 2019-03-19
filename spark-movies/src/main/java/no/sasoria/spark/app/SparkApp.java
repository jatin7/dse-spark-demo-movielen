package no.sasoria.spark.app;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.datastax.driver.core.querybuilder.Select;

import no.sasoria.spark.cassandra.CassandraWriter;

/**
 * This spark application writes a dataframe loaded from a CSV file into Cassandra.
 *
 */
public class SparkApp {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("insert-movies")
				.set("output.consistency.level", "1")
				.setMaster("local[*]"); // disable for spark-submit
		

		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate();

		writeToCassandra(spark, createTable(spark));
		selectMovie(spark, 1683);
		selectMovie(spark, "Inside 1996");
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
		try {
			CassandraWriter.writeDataFrame(spark, tableDf);
		} catch (UnsupportedOperationException e) {
			System.out.println(e);
		}
	}
	
	private static Dataset<Row> selectMovie(SparkSession spark, int id) {
		Dataset<Row> df = spark.sql("SELECT id,title,genres FROM movies WHERE id=" + id);
		df.show();
		
		return df;
	}
	
	private static Dataset<Row> selectMovie(SparkSession spark, String title) {
		Dataset<Row> df = spark.sql("SELECT id FROM movies WHERE title='" + title + "'");
		df.show();
		
		return df;
	}

}
