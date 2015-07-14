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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class JdbcSerDeHelper{
	
	private static final Log LOG = LogFactory.getLog(JdbcSerDe.class);
	public static String columnNames;
	public static String columnTypeNames;


	public static Connection getConnection(Properties tblProps) throws ClassNotFoundException, SQLException{
        LOG.info(">> " + tblProps.getProperty("mapreduce.jdbc.url")+" "+
            tblProps.getProperty("mapreduce.jdbc.username") + " " +
            tblProps.getProperty("mapreduce.jdbc.password") );


        if(tblProps.getProperty("mapreduce.jdbc.username") == null) {
            return DriverManager.getConnection(tblProps.getProperty("mapreduce.jdbc.url"));
        } else {
            return DriverManager.getConnection(
                 tblProps.getProperty("mapreduce.jdbc.url"),
                 tblProps.getProperty("mapreduce.jdbc.username"),
                 tblProps.getProperty("mapreduce.jdbc.password"));
        }
    }

    public static String getSelectQuery(Properties tblProps){
    	StringBuilder query = new StringBuilder();
    	query.append("Select * from ");
    	query.append(tblProps.getProperty("mapreduce.jdbc.input.table.name"));
    	query.append(" LIMIT 1");
    	LOG.info(">> "+ query.toString() );
    	return query.toString();
    }
    public static String sqlToHiveColumnTypeNames(String sqlType) throws SerDeException {
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
        } else if (lctype.startsWith("array<")) {
            return "ARRAY";
        }
        throw new SerDeException("Unrecognized column type: " + sqlType);

    }

}