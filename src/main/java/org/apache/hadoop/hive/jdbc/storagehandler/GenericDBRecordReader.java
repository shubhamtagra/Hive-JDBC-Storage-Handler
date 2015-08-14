package org.apache.hadoop.hive.jdbc.storagehandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBRecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.hadoop.hive.jdbc.storagehandler.Constants.DEFAULT_INPUT_FETCH_SIZE;
import static org.apache.hadoop.hive.jdbc.storagehandler.Constants.INPUT_FETCH_SIZE;
/**
 * Created by stagra on 8/14/15.
 */
public class GenericDBRecordReader<T extends DBWritable> extends DBRecordReader<T>
{
    public GenericDBRecordReader(DBInputFormat.DBInputSplit split, Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig, String cond, String[] fields, String table)
            throws SQLException
    {
        super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
    }

    protected ResultSet executeQuery(String query) throws SQLException {
        this.statement = getConnection().prepareStatement(query,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(conf.getInt(INPUT_FETCH_SIZE, DEFAULT_INPUT_FETCH_SIZE));
        return statement.executeQuery();
    }
}
