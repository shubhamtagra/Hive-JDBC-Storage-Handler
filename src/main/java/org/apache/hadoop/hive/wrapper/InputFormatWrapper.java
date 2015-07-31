/*
 * Copyright 2013-2015 Qubole
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
package org.apache.hadoop.hive.wrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.*;
import org.apache.hadoop.hive.jdbc.storagehandler.JdbcDBInputSplit;
import org.apache.hadoop.hive.jdbc.storagehandler.Constants;

public class InputFormatWrapper<K, V> implements
        org.apache.hadoop.mapred.InputFormat {

    private static final Log LOG = LogFactory.getLog(InputFormatWrapper.class);

    protected InputFormat<K, V> realInputFormat;

    public InputFormatWrapper() {
        // real inputFormat is initialized based on conf.
    }

    public InputFormatWrapper(InputFormat<K, V> realInputFormat) {
        this.realInputFormat = realInputFormat;

    }
    @Override
    public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
            Reporter reporter) throws IOException {
        if (this.realInputFormat != null) {
            return new RecordReaderWrapper<K, V>(realInputFormat, split, job,
                    reporter);
        } else {
            return null;
        }
    }

    public List<org.apache.hadoop.mapreduce.InputSplit> getSplitsForVPC(JobConf job, 
        List<org.apache.hadoop.mapreduce.InputSplit> splits, TaskAttemptContext taskContext){
     
         try{
            if( ((job.get(Constants.VPC_SPLIT_MAPPERS)).toUpperCase()).equals("TRUE") ){
                int chunks = job.getInt("mapred.map.tasks", 1);
                splits = new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();
                for (int i = 0; i < chunks; i++) {
                    DBInputSplit split;
                    split = new JdbcDBInputSplit(i);
                    splits.add(split);
                }
            }
            else{
                    splits = realInputFormat.getSplits(taskContext);
            }
            return splits;
        } catch (Exception e) {
            
        }
        return null;
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        List<org.apache.hadoop.mapreduce.InputSplit> splits = null; 
        if (this.realInputFormat != null) {
            try {
                TaskAttemptContext taskContext = ShimLoader.getHadoopShims()
                        .newTaskAttemptContext(job, null);

                splits = getSplitsForVPC(job,splits,taskContext);
                if (splits == null) {
                    return null;
                }

                InputSplit[] resultSplits = new InputSplit[splits.size()];
                int i = 0;
                for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
                    if (split.getClass() == org.apache.hadoop.mapreduce.lib.input.FileSplit.class) {
                        org.apache.hadoop.mapreduce.lib.input.FileSplit mapreduceFileSplit = 
                           ((org.apache.hadoop.mapreduce.lib.input.FileSplit) split);
                        resultSplits[i++] = new FileSplit(
                                mapreduceFileSplit.getPath(),
                                mapreduceFileSplit.getStart(),
                                mapreduceFileSplit.getLength(),
                                mapreduceFileSplit.getLocations());
                    } else {
                        final Path[] paths = FileInputFormat.getInputPaths(job);
                        resultSplits[i++] = new InputSplitWrapper(split,
                                paths[0]);
                    }
                }

                return resultSplits;

            } catch (Exception e) {
                throw new IOException(e);
            }
        } else {
            return null;
        }
    }
}
