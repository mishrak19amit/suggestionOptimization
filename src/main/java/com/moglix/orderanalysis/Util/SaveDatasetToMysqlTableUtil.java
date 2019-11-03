package com.moglix.orderanalysis.Util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class SaveDatasetToMysqlTableUtil {

	public static void saveDatasetToMysqlTable(Dataset<Row> dataset, String tableName) {
		Properties propFile = getPropertyFile();
		Properties prop = new Properties();
		prop.setProperty("driver", propFile.getProperty("mysql.driver"));
		prop.setProperty("user", propFile.getProperty("mysql.destination.username"));
		prop.setProperty("password", propFile.getProperty("mysql.destination.password"));

		String url = propFile.getProperty("mysql.destination.url");

		dataset.write().mode(SaveMode.Overwrite).jdbc(url, tableName, prop);

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
