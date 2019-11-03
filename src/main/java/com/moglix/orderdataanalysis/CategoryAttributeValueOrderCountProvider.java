package com.moglix.orderdataanalysis;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.orderanalysis.Util.DataSetProvider;
import com.moglix.orderanalysis.Util.OrderUtil;
import com.moglix.orderanalysis.model.CategoryBrandAttributeValueOrderCount;
import com.moglix.orderanalysis.model.ProductBrandCategoryAttribute;
import com.moglix.orderanalysis.service.AbstractSparkDatasetProvider;

public class CategoryAttributeValueOrderCountProvider {

	private static Dataset<Row> addBrandAttributeCategoryName(
			Dataset<CategoryBrandAttributeValueOrderCount> prodBrandCatAttributeOrderCount, SparkSession sc,
			Properties propFile) {
		Dataset<Row> brandDetails = DataSetProvider.cassandraDataSetProvider(propFile.getProperty("cass.tables.brand"),
				propFile.getProperty("cass.keyspace"));
		Dataset<Row> catDetails = DataSetProvider.cassandraDataSetProvider(propFile.getProperty("cass.tables.category"),
				propFile.getProperty("cass.keyspace"));
		Dataset<Row> attriDetails = DataSetProvider.cassandraDataSetProvider(
				propFile.getProperty("cass.tables.attribute"), propFile.getProperty("cass.keyspace"));
		brandDetails.createOrReplaceTempView("brand");
		catDetails.createOrReplaceTempView("category");
		attriDetails.createOrReplaceTempView("attribute");
		prodBrandCatAttributeOrderCount.createOrReplaceTempView("MSNBCAO");

		Dataset<Row> orderPreparedDS = sc.sql(
				"select attributeID, attribute_name, brandID, brand_name, categoryID, category_name, orderCount, value from MSNBCAO join attribute on attribute.id_attribute=MSNBCAO.attributeID join brand on brand.id_brand=MSNBCAO.brandID join category on category.category_code=MSNBCAO.categoryID");

		return orderPreparedDS;
	}

	public static Dataset<Row> getPreparedDataset(SparkSession sc, Properties propFile) throws SparkException {
		Dataset<Row> orderDS = getProductBrandCategoryAttribute(sc, propFile);

		Dataset<CategoryBrandAttributeValueOrderCount> prodBrandCatAttributeOrderCount = createPojoForJavaObject(
				orderDS);

		Dataset<Row> orderPreparedDS = addBrandAttributeCategoryName(prodBrandCatAttributeOrderCount, sc, propFile);
		return orderPreparedDS;

	}

	private static Dataset<CategoryBrandAttributeValueOrderCount> createPojoForJavaObject(Dataset<Row> orderDS) {

		Encoder<CategoryBrandAttributeValueOrderCount> categoryAttributeCountEncoder = Encoders
				.bean(CategoryBrandAttributeValueOrderCount.class);

		Dataset<CategoryBrandAttributeValueOrderCount> categoryAttributeCountRDD = orderDS.toJSON().flatMap(x -> {

			List<CategoryBrandAttributeValueOrderCount> productBrandCategoryAttributeList = new ArrayList<>();
			try {
				ProductBrandCategoryAttribute productBrandCategoryAttribute = new ObjectMapper().readValue(x,
						ProductBrandCategoryAttribute.class);

				Map<String, List<String>> attributeKeyVal = productBrandCategoryAttribute.getAttributeList();

				String categoryID = productBrandCategoryAttribute.getCategoryID();
				String brandID = productBrandCategoryAttribute.getBrandID();
				String orderCount = productBrandCategoryAttribute.getOrderCount();

				for (String attribute : attributeKeyVal.keySet()) {

					CategoryBrandAttributeValueOrderCount categoryBrandAttributeValueOrderCount = new CategoryBrandAttributeValueOrderCount();
					categoryBrandAttributeValueOrderCount.setBrandID(brandID);
					categoryBrandAttributeValueOrderCount.setCategoryID(categoryID);
					categoryBrandAttributeValueOrderCount.setOrderCount(orderCount);
					categoryBrandAttributeValueOrderCount.setAttributeID(attribute);
					categoryBrandAttributeValueOrderCount
							.setValue(attributeKeyVal.get(attribute).toString().replace("[", "").replace("]", ""));
					productBrandCategoryAttributeList.add(categoryBrandAttributeValueOrderCount);
				}

			} catch (Exception e) {
				System.out.println(e);
			}

			return productBrandCategoryAttributeList.iterator();
		}, categoryAttributeCountEncoder);

		return categoryAttributeCountRDD;
	}

	public static Dataset<Row> getJoinedDSOrderItems(SparkSession sc, Properties propFile) {
		Dataset<Row> orderDS = DataSetProvider.mysqlDataSetProvider(propFile.getProperty("mysql.source.orderdetail"),
				"mysql.source.database");
		Dataset<Row> itemDS = DataSetProvider.mysqlDataSetProvider(propFile.getProperty("mysql.source.orderitem"),
				"mysql.source.database");

		orderDS.createOrReplaceTempView("OrderDetails");
		itemDS.createOrReplaceTempView("ItemDetails");

		itemDS.printSchema();

		Dataset<Row> orderItemDS = sc.sql(
				"select user_id,product_id,product_quantity,order_id,product_name,cart_id,total_amount,total_amount_with_offer,total_amount_with_taxes,total_offer,OrderDetails.total_payable_amount, OrderDetails.created_at, OrderDetails.updated_at from OrderDetails join ItemDetails on ItemDetails.order_id=OrderDetails.id where product_id !='' order by user_id");
		return orderItemDS;

	}

	public static Dataset<Row> getProductBrandCategoryAttribute(SparkSession sc, Properties propFile) {
		Dataset<Row> productDS = DataSetProvider.cassandraDataSetProvider(propFile.getProperty("cass.tables.product"),
				propFile.getProperty("cass.keyspace"));
		productDS.printSchema();
		productDS.createOrReplaceTempView("ProductView");

		Dataset<Row> productOrderedCountDS = getJoinedDSOrderItems(sc, propFile);

		productOrderedCountDS.createOrReplaceTempView("OrderItemCount");

		productOrderedCountDS.printSchema();
		
		Dataset<Row> productDSMap = sc.sql(
				"select ProductView.id_product as msn, brand_id as brandID, category_code[0] as categoryID, attribute_list as attributeList, product_quantity as orderCount from ProductView join OrderItemCount on OrderItemCount.product_id=ProductView.id_product");

		productDSMap.printSchema();
		return productDSMap;

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
