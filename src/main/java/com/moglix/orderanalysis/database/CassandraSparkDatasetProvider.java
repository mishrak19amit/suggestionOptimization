package com.moglix.orderanalysis.database;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.moglix.orderanalysis.Util.OrderUtil;
import com.moglix.orderanalysis.service.AbstractSparkDatasetProvider;

public class CassandraSparkDatasetProvider<T> extends AbstractSparkDatasetProvider<T> {

	@Override
	public Dataset<Row> getDataSet(String tableName, String schemaName, Properties propFile) throws SparkException {
		Dataset<Row> productTempDF = null;
		boolean fromFile = false;
		if (fromFile) {
			productTempDF = OrderUtil.getDatasetFromJSONFile(
					propFile.getProperty("cass.mysql.file.path") + "/" + tableName + ".json", getSparkSession());
		} else {

			StringBuilder viewName = new StringBuilder();
			DataFrameReader dataFrameReader = getSparkSession().read().format("org.apache.spark.sql.cassandra")
					.option("spark.cassandra.connection.host", propFile.getProperty("cass.hostname"))
					.option("spark.cassandra.auth.username", propFile.getProperty("cass.username"))
					.option("spark.cassandra.auth.password", propFile.getProperty("cass.password"));
			if (!StringUtils.isBlank(schemaName)) {
				dataFrameReader = dataFrameReader.option("keyspace", schemaName);
			}
			dataFrameReader = dataFrameReader.option("table", tableName);
			viewName.append(tableName);

			switch (tableName) {
			case "product_data":
				productTempDF = dataFrameReader.load().select("id_product", "brand_id", "category_code","attribute_list");
				break;
			case "brand_details":
				productTempDF = dataFrameReader.load().select("id_brand", "brand_name");
				break;
			case "category_details":
				productTempDF = dataFrameReader.load().select("category_code", "category_name");
				break;
			case "attribute_details":
				productTempDF = dataFrameReader.load().select("id_attribute", "attribute_name");
				break;
			default:
				productTempDF = dataFrameReader.load();
			}

		}
		productTempDF.show();
		return productTempDF;
	}
	
	public static void main(String[] args) throws SparkException {
		Properties propFile=getPropertyFile();

		CassandraSparkDatasetProvider obj= new CassandraSparkDatasetProvider();
		obj.getDataSet("product_data", "products", propFile);
		obj.getDataSet("brand_details", "products", propFile);
		obj.getDataSet("category_details", "products", propFile);
		obj.getDataSet("attribute_details", "products", propFile);
		System.out.println("Amit");
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
