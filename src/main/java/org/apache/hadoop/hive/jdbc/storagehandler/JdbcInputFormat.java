/*
 * Copyright 2013-2015 Qubole
 * Copyright 2013-2015 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.jdbc.storagehandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.fs.Path;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.InputFormat;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.hive.wrapper.InputFormatWrapper;

public class JdbcInputFormat extends InputFormatWrapper {
    private static final Log LOG = LogFactory.getLog(JdbcInputFormat.class);

    private boolean jobConfSet = false;
    private DBInputFormat delegate;

    public JdbcInputFormat() {
        super(new DBInputFormat());
    }

    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job,
            Reporter reporter) throws IOException {
        JdbcSerDeHelper.setFilters(job);
        ((org.apache.hadoop.mapreduce.lib.db.DBInputFormat) realInputFormat)
                .setConf(job);
        return super.getRecordReader(split, job, reporter);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        JdbcSerDeHelper.setFilters(job);
        ((org.apache.hadoop.mapreduce.lib.db.DBInputFormat) realInputFormat)
                .setConf(job);
        return super.getSplits(job, numSplits);

    }
}
