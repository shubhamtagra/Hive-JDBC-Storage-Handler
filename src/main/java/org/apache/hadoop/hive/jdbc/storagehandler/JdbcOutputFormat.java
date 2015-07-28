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

import java.io.IOException;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat.*;

public class JdbcOutputFormat<V> extends DBOutputFormat<DbRecordWritable, V>
		implements HiveOutputFormat<DbRecordWritable, V> {

	private static final Log LOG = LogFactory.getLog(JdbcOutputFormat.class);
	private org.apache.hadoop.mapreduce.RecordWriter recordWriter;
	private TaskAttemptContext taskContext;

	@Override
	public void checkOutputSpecs(FileSystem filesystem, JobConf job)
			throws IOException {
	}

	@Override
	public RecordWriter getHiveRecordWriter(JobConf jobConf, Path finalOutPath,
			Class<? extends Writable> valueClass, boolean isCompressed,
			Properties tableProperties, Progressable progress)
			throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("jobConf: " + jobConf);
			LOG.debug("tableProperties: " + tableProperties);
		}
		taskContext = ShimLoader.getHadoopShims().newTaskAttemptContext(
				jobConf, null);
		org.apache.hadoop.mapreduce.lib.db.DBOutputFormat delegate = new org.apache.hadoop.mapreduce.lib.db.DBOutputFormat();
		recordWriter = delegate.getRecordWriter(taskContext);
		// Wrapping DBRecordWriter in JdbcRecordWriter
		return new JdbcRecordWriter(
				(org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.DBRecordWriter) recordWriter);
	}

	@Override
	public org.apache.hadoop.mapred.RecordWriter<DbRecordWritable, V> getRecordWriter(
			FileSystem filesystem, JobConf job, String name,
			Progressable progress) throws IOException {
		throw new RuntimeException("Error: Hive should not invoke this method.");
	}

}
