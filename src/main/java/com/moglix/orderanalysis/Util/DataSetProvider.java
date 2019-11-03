package com.moglix.orderanalysis.Util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.moglix.orderanalysis.database.factory.DatabaseFactory;
import com.moglix.orderanalysis.enumerator.DatabaseType;
import com.moglix.orderanalysis.service.AbstractSparkDatasetProvider;

public class DataSetProvider {

	public static Dataset<Row> cassandraDataSetProvider(String tableName, String keySpace) {
		Properties prop = getPropertyFile();
		AbstractSparkDatasetProvider instance = DatabaseFactory.getSparkExecutor(DatabaseType.CASSANDRA.name());
		Dataset<Row> response = null;
		try {
			response = instance.initiateDatasetProvider(tableName, keySpace, prop);
		} catch (SparkException e) {
			System.out.println("Error occur's during spark query executor " + e);
		}

		return response;

	}
	
	public static Dataset<Row> mysqlDataSetProvider(String tableName, String dbName) {
		Properties prop = getPropertyFile();
		AbstractSparkDatasetProvider instance = DatabaseFactory.getSparkExecutor(DatabaseType.MYSQL.name());
		Dataset<Row> response = null;
		try {
			response = instance.initiateDatasetProvider(tableName, dbName, prop);
		} catch (SparkException e) {
			System.out.println("Error occur's during spark query executor " + e);
		}

		return response;

	}
	
	public static Properties getPropertyFile() {
		Properties prop = new Properties();
		try (InputStream input = new FileInputStream("src/main/resources/dataSource.properties")) {

			// load a properties file
			prop.load(input);

		} catch (IOException ex) {
			ex.printStackTrace();
		}

		return prop;
	}

}
