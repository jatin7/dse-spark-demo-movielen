package no.sasoria.spark.app;

import java.util.ArrayList;
import java.util.List;

import javax.sound.midi.Sequence;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * SparkReader provides static methods for reading data sources in spark. 
 * Could be expanded to other types, like parquet...
 * 
 */
public class SparkReader {
	public static final String resourceFolder = System.getProperty("user.dir") + "/src/test/resources/";

	public static Dataset<Row> readCSV(SparkSession spark, String inputLocation) {

		return spark.read().format("csv")
				.schema(createSchema())
				.option("delimiter", ",")
				.option("header","true")
				.load(inputLocation);
	}
	
	private static StructType createSchema() {
		List<StructField> fields = new ArrayList<StructField>();
		String schemaString = "id title genres";

		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}

		StructType schema = DataTypes.createStructType(fields);
		return schema;
	}
}
