package no.sasoria.spark.cassandra;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CassandraWriter {
	
	/**
	 * Writes a specified dataframe to Cassandra. The Keyspace and Table in Cassandra
	 * must be defined before this method is executed, otherwise a @code java.io.IOException}
	 * will be thrown (See {@code CassandraApp} for further details).
	 * 
	 * @param spark SparkSession
	 * @param tableDf Dataset<Row>
	 * @return true when inserts are performed on Cassandra.
	 */
	public static boolean writeDataFrame(SparkSession spark, Dataset<Row> tableDf) {
		if (tableDf == null)
			return false;

		tableDf.write()
		.format("org.apache.spark.sql.cassandra")
		.options(createMapConfig())
		.save();
		
		return true;
	}
	
	private static Map<String,String> createMapConfig() {
		Map<String,String> map = new HashMap<String,String>();
		map.put("table", "movies");
		map.put("keyspace", "movielens");
		
		return map;
	}
}
