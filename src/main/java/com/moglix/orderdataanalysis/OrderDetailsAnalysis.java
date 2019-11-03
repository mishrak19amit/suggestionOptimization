package com.moglix.orderdataanalysis;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.moglix.orderanalysis.Util.SaveDatasetToMysqlTableUtil;
import com.moglix.orderanalysis.service.AbstractSparkDatasetProvider;

public class OrderDetailsAnalysis {

	public static void main(String[] args) throws SparkException {
		SparkSession sc = AbstractSparkDatasetProvider.getSparkSession();
		Properties propFile = getPropertyFile();
		Dataset<Row> orderDetails = CategoryAttributeValueOrderCountProvider.getPreparedDataset(sc, propFile);

		orderDetails.printSchema();
		orderDetails.cache();
		//findMostSoldCategoryAttribute(orderDetails, sc);
		//findMostSoldCategory(orderDetails, sc);
		findMostSoldAttributeValOfCategory(orderDetails, sc);
		findMostSoldCategoryOfBrand(orderDetails,sc);
	}

	private static void findMostSoldCategoryAttribute(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql(
				"select category_name, attribute_name, sum(orderCount) as totalOrders from orderDetails where group by category_name, attribute_name order by totalOrders desc, category_name");
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "mostSoldCategoryAttribute");
		
	}

	private static void findMostSoldCategory(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql("select category_name, sum(orderCount) as totalOrders from orderDetails group by category_name order by totalOrders desc");
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "mostSoldCategory");
		
	}
	
	private static void findMostSoldAttributeValOfCategory(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql("select category_name, attribute_name, value, sum(orderCount) as totalOrders from orderDetails where group by category_name, attribute_name, value order by totalOrders desc");
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "mostSoldAttributeValOfCategory");
		
	}
	
	private static void findMostSoldCategoryOfBrand(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql("select brand_name, category_name, sum(orderCount) as totalOrders from orderDetails where group by brand_name, category_name order by brand_name, totalOrders desc");
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "findMostSoldCategoryOfBrand");
		dataset.show();
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
