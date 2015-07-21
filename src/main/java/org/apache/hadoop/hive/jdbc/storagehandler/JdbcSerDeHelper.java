/*
 * Copyright 2013-2015 Qubole
 * Copyright 2013-2015 Makoto YUI
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.jdbc.storagehandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import java.sql.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;

public class JdbcSerDeHelper {

	private static final Log LOG = LogFactory.getLog(JdbcSerDe.class);
	public static String columnNames;
	public static String columnTypeNames;
	private Connection dbConnection;
	private String tableName;

	public void initialize(Properties tblProps, Configuration sysConf) {
		setProperties(tblProps, sysConf);
		StringBuilder colNames = new StringBuilder();
		StringBuilder colTypeNames = new StringBuilder();
		DBConfiguration dbConf = new DBConfiguration(sysConf);
		try {

			dbConnection = dbConf.getConnection();
			String query = getSelectQuery(tblProps);
			Statement st = dbConnection.createStatement();
			ResultSet rs = st.executeQuery(query);
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnsNumber = rsmd.getColumnCount();

			int i = 0;
			for (i = 1; i < columnsNumber; i++) {
				String colName = rsmd.getColumnName(i);
				String colType = rsmd.getColumnTypeName(i);
				colNames.append(colName + ",");
				colTypeNames.append(sqlToHiveColumnTypeNames(colType) + ":");
			}
			colNames.append(rsmd.getColumnName(i));
			colTypeNames.append(rsmd.getColumnTypeName(i));

			columnNames = colNames.toString();
			columnTypeNames = colTypeNames.toString();

			rs.close();
			st.close();
			dbConnection.close();

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public void setProperties(Properties tblProps, Configuration sysConf) {

		for (String key : tblProps.stringPropertyNames()) {
			// LOG.info(">> " + key + ">> " + tblProps.getProperty(key));
			if (key.contains("jdbc.input.table.name")) {
				String value = tblProps.getProperty(key);
				tableName = value;
			}
			if (key.startsWith("mapred.jdbc.")) {
				String value = tblProps.getProperty(key);
				sysConf.set(key, value);
				key = key.replaceAll("mapred", "mapreduce");
				sysConf.set(key, value);
			}

		}
		for (String key : tblProps.stringPropertyNames()) {
			if (key.startsWith("mapreduce.jdbc.")) {
				String value = tblProps.getProperty(key);
				sysConf.set(key, value);
				key = key.replaceAll("mapreduce", "mapred");
				sysConf.set(key, value);
			}
		}
	}

	public String getSelectQuery(Properties tblProps) {
		StringBuilder query = new StringBuilder();
		query.append("Select * from ");
		query.append(tableName);
		query.append(" LIMIT 1");
		LOG.info(">> " + query.toString());
		return query.toString();
	}

	public String sqlToHiveColumnTypeNames(String sqlType)
			throws SerDeException {
		final String lctype = sqlType.toLowerCase();
		if ("varchar".equals(lctype)) {
			return "STRING";
		} else if ("float".equals(lctype)) {
			return "FLOAT";
		} else if ("double".equals(lctype)) {
			return "DOUBLE";
		} else if ("boolean".equals(lctype)) {
			return "BOOLEAN";
		} else if ("tinyint".equals(lctype)) {
			return "TINYINT";
		} else if ("smallint".equals(lctype)) {
			return "SMALLINT";
		} else if ("int".equals(lctype)) {
			return "INT";
		} else if ("bigint".equals(lctype)) {
			return "BIGINT";
		} else if ("timestamp".equals(lctype)) {
			return "TIMESTAMP";
		} else if ("binary".equals(lctype)) {
			return "BINARY";
		} else if (lctype.startsWith("array")) {
			return "ARRAY<";
		}
		throw new SerDeException("Unrecognized column type: " + sqlType);

	}

	public String getColumnNames() {
		return columnNames;
	}

	public String getColumnTypeNames() {
		return columnTypeNames;
	}

}