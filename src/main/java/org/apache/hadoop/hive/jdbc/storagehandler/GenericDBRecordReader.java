package org.apache.hadoop.hive.jdbc.storagehandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBRecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.hadoop.hive.jdbc.storagehandler.Constants.DEFAULT_INPUT_FETCH_SIZE;
import static org.apache.hadoop.hive.jdbc.storagehandler.Constants.INPUT_FETCH_SIZE;
/**
 * Created by stagra on 8/14/15.
 */
public class GenericDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {
    final Configuration conf;
    private static final Log LOG = LogFactory.getLog(GenericDBRecordReader.class);

    public GenericDBRecordReader(DBInputFormat.DBInputSplit split, Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig, String cond, String[] fields, String table)
            throws SQLException {
        super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
        this.conf = conf;
    }

    @Override
    protected ResultSet executeQuery(String query) throws SQLException {
        this.statement = getConnection().prepareStatement(query,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(conf.getInt(INPUT_FETCH_SIZE, DEFAULT_INPUT_FETCH_SIZE));
        return statement.executeQuery();
    }

    @Override
    public void close() throws IOException {
        try {
            statement.cancel();
        } catch (SQLException e) {
            // Ignore any errors in cancelling, this is not fatal
            LOG.error("Could not cancel query: "  + this.getSelectQuery());
        }
        super.close();
    }
}
