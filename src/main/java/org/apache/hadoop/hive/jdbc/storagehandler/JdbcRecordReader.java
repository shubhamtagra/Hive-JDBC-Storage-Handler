package org.apache.hadoop.hive.jdbc.storagehandler;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.wrapper.RecordReaderWrapper;
import java.io.IOException;

/**
 * Created by psrinivas on 09/06/14.
 */
public class JdbcRecordReader extends
        RecordReaderWrapper<LongWritable, BytesWritable> {

    public JdbcRecordReader(
            InputFormat<LongWritable, BytesWritable> newInputFormat,
            InputSplit oldSplit, JobConf oldJobConf, Reporter reporter)
            throws IOException {
        super(newInputFormat, oldSplit, oldJobConf, reporter);
    }

}
