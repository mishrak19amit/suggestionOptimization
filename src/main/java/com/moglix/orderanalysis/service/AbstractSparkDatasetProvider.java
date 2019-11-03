package com.moglix.orderanalysis.service;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractSparkDatasetProvider<T> {

	private static SparkSession session;

	private static SparkConf getSparkConf() throws SparkException {
		int gegaBytes = 1024 * 1024 * 1024;
		long freeMemory = Runtime.getRuntime().freeMemory() / gegaBytes;
		if (freeMemory > 2.5 * gegaBytes) {
			throw new SparkException("Insufficient memory ");
		}
		SparkConf conf = new SparkConf();
		conf.set("spark.driver.memory", "8g");
		return conf;
	}

	public static SparkSession getSparkSession() throws SparkException {

		if (null == session) {
			session = SparkSession.builder().config(getSparkConf()).appName("OrderDataAnalysis").master("local[*]")
					.getOrCreate();
		}
		return session;
	}

	public abstract Dataset<Row> getDataSet(String tableName, String schemaName, Properties propFile) throws SparkException;

	public Dataset<Row> initiateDatasetProvider(String tableName, String schemaName, Properties propFile) throws SparkException {
		return getDataSet(tableName, schemaName, propFile);
	}

}
