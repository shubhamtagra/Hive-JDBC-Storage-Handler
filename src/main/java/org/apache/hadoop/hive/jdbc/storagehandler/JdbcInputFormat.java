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
import org.apache.hadoop.mapreduce.lib.db.*;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class JdbcInputFormat extends InputFormatWrapper {
    private static final Log LOG = LogFactory.getLog(JdbcInputFormat.class);

    private boolean jobConfSet = false;
    private DBInputFormat delegate ;
    public JdbcInputFormat() {
        super(new DBInputFormat());
    }

    @Override
    public RecordReader getRecordReader(
            InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        JdbcSerDeHelper.setFilters(job);
        if (realInputFormat != null) {
          ((org.apache.hadoop.mapreduce.lib.db.DBInputFormat)realInputFormat).setConf(job);
          return new JdbcRecordReader(realInputFormat, split,
              job, reporter);
        } else {
          return null;
        }
    }
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
         JdbcSerDeHelper.setFilters(job);
        if (this.realInputFormat != null) {
            ((org.apache.hadoop.mapreduce.lib.db.DBInputFormat)realInputFormat).setConf(job);
            try {
                // create a MapContext to pass reporter to record reader (for
                // counters)
                TaskAttemptContext taskContext = ShimLoader.getHadoopShims()
                        .newTaskAttemptContext(job, null);

                List<org.apache.hadoop.mapreduce.InputSplit> splits = realInputFormat
                        .getSplits(taskContext);

                if (splits == null) {
                    return null;
                }

                InputSplit[] resultSplits = new InputSplit[splits.size()];
                int i = 0;
                for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
                    if (split.getClass() == org.apache.hadoop.mapreduce.lib.input.FileSplit.class) {
                        org.apache.hadoop.mapreduce.lib.input.FileSplit mapreduceFileSplit = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) split);
                        resultSplits[i++] = new FileSplit(
                                mapreduceFileSplit.getPath(),
                                mapreduceFileSplit.getStart(),
                                mapreduceFileSplit.getLength(),
                                mapreduceFileSplit.getLocations());
                    } else {
                        final Path[] paths = FileInputFormat.getInputPaths(job);
                        resultSplits[i++] = new InputSplitWrapper(split, paths[0]);
                    }
                }

                return resultSplits;

            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        } else {
            return null;
        }
    }
}
