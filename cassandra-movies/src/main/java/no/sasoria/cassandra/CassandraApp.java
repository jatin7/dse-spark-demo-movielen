package no.sasoria.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.google.common.collect.ImmutableMap;

/**
 * An instance of this class runs datastax' cassandra-driver to create a keyspace
 * and table for movies. 
 */
public class CassandraApp {
	public static void main(String[] args) {
		Cluster cluster = Cluster.builder()
				.withClusterName("cassandraCluster")
				.addContactPoint("127.0.0.1")
				.build();

		Session session = cluster.connect();

		session.execute(createKeyspaceQuery());
		session.execute(createTableQuery());
		
		session.close();
		cluster.close();
	}

	private static SchemaStatement createKeyspaceQuery() {
		//https://docs.datastax.com/en/latest-java-driver-api/com/datastax/driver/core/schemabuilder/class-use/Create.html
		return SchemaBuilder.createKeyspace("movielens")
				.with()
				// replication factor 1 to avoid QueryExecutor: Error in spark with the cassandra-connector.
				.replication(ImmutableMap.<String,Object>of("class", "SimpleStrategy", "replication_factor", 1));
	}

	private static SchemaStatement createTableQuery() {
		return SchemaBuilder.createTable("movielens", "movies")
				.addPartitionKey("id", DataType.text())
				.addColumn("title", DataType.text())
				.addColumn("genres", DataType.text());
		
	}
}
