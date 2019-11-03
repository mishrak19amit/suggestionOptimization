package com.moglix.orderanalysis.database;

import java.util.Properties;

import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.moglix.orderanalysis.Util.OrderUtil;
import com.moglix.orderanalysis.service.AbstractSparkDatasetProvider;

public class MySqlSparkDatasetProvider<T> extends AbstractSparkDatasetProvider<T> {

	@Override
	public Dataset<Row> getDataSet(String tableName, String schemaName, Properties propFile) throws SparkException {
		boolean fromFile = false;
		Dataset<Row> payload = null;
		if (fromFile) {
			payload = OrderUtil.getDatasetFromJSONFile(
					propFile.getProperty("cass.mysql.file.path") + "/" + tableName + ".json", getSparkSession());
		} else {
			payload = getSparkSession().read().format("jdbc").option("driver", propFile.getProperty("mysql.driver"))
					.option("url", propFile.getProperty("mysql.source.url"))
					.option("dbtable", propFile.getProperty("mysql.source.database") + "." + tableName).load();
		}
		payload.show();
		return payload;
	}
}
