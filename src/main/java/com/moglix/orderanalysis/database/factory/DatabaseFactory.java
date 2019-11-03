package com.moglix.orderanalysis.database.factory;

import com.moglix.orderanalysis.database.CassandraSparkDatasetProvider;
import com.moglix.orderanalysis.database.MySqlSparkDatasetProvider;
import com.moglix.orderanalysis.service.AbstractSparkDatasetProvider;

public class DatabaseFactory {

	@SuppressWarnings("rawtypes")
	public static <T> AbstractSparkDatasetProvider getSparkExecutor(String databaseType) {
		AbstractSparkDatasetProvider<T> instance = null;
		switch (databaseType) {
		case "CASSANDRA":
			instance = new CassandraSparkDatasetProvider<>();
			break;
		case "MYSQL":
			instance = new MySqlSparkDatasetProvider<>();
			break;
		default:
			return instance;
		}
		return instance;
	}

}
