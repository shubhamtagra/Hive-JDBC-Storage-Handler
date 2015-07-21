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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.fs.Path;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.lib.db.DBInputFormat.*;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapred.JobConf;

public class JdbcInputFormat extends DBInputFormat<DbRecordWritable> {
	private static final Log LOG = LogFactory.getLog(JdbcInputFormat.class);

	private boolean jobConfSet = false;

	// Wrapper Inner Class for DBInputSplit
	public static class JdbcInputSplit extends FileSplit {

		private DBInputSplit dbSplit;
		private Path path;

		JdbcInputSplit() {
			super((Path) null, 0, 0, new String[0]);
		}

		JdbcInputSplit(DBInputSplit dbSplit, JobConf job) {
			super((Path) null, 0, 0, new String[0]);
			this.dbSplit = dbSplit;
			if (FileInputFormat.getInputPaths(job).length > 0) {
				path = FileInputFormat.getInputPaths(job)[0];
			}
		}

		public JdbcInputSplit(long start, long end) {
			super((Path) null, start, end, new String[0]);
			this.dbSplit = new DBInputSplit(start, end);
		}

		public DBInputSplit getDBInputSplit() {
			return this.dbSplit;
		}

		/**
		 * Convenience Constructor
		 * 
		 * @param start
		 *            the index of the first row to select
		 * @param end
		 *            the index of the last row to select
		 */

		/** {@inheritDoc} */
		public String[] getLocations() throws IOException {
			return new String[] {};
		}

		@Override
		public long getLength() {
			long length = 0;
			try {
				length = dbSplit.getLength();
			} catch (IOException e) {
				LOG.warn(StringUtils.stringifyException(e));
			}
			return length;
		}

		@Override
		public Path getPath() {
			LOG.info("Inside Wrapper taking path");
			return path;
		}

		/*
		 * This method reads path and Input Split class name and make new
		 * instance of the class.
		 */
		public void readFields(DataInput in) throws IOException {
			path = new Path(in.readUTF());
			String className = (String) in.readUTF();
			Class<?> splitClass;
			try {
				splitClass = Class.forName(className);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
			dbSplit = (DBInputSplit) ReflectionUtils.newInstance(splitClass,
					null);
			dbSplit.readFields(in);
		}

		/*
		 * This method Stores path and class name of Input Split class
		 */
		public void write(DataOutput out) throws IOException {
			out.writeUTF(path.toString());
			out.writeUTF((String) dbSplit.getClass().getName());
			dbSplit.write(out);
		}

	}

	/**
	 * @see org.apache.hadoop.util.ReflectionUtils#setConf(Object,
	 *      Configuration)
	 */

	public void setConf(Configuration conf) {
		// delay for TableJobProperties is set to the jobConf
	}

	/**
	 * @see org.apache.hadoop.hive.ql.exec.FetchOperator#getRecordReader()
	 */
	@Override
	public void configure(JobConf jobConf) {
		// delay for TableJobProperties is set to the jobConf
	}

	@Override
	public RecordReader<LongWritable, DbRecordWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		setFilters(job);
		if (!jobConfSet) {
			super.configure(job);
			this.jobConfSet = true;
		}

		return super.getRecordReader(
				((JdbcInputSplit) split).getDBInputSplit(), job, reporter);
	}

	@Override
	public InputSplit[] getSplits(JobConf jobConf, int chunks)
			throws IOException {
		setFilters(jobConf);
		if (!jobConfSet) {
			super.configure(jobConf);
			this.jobConfSet = true;
		}
		InputSplit[] splits = super.getSplits(jobConf, chunks);
		InputSplit[] jSplits = new JdbcInputSplit[splits.length];
		for (int i = 0; i < splits.length; i++) {
			jSplits[i] = (InputSplit) (new JdbcInputSplit(
					(DBInputSplit) splits[i], jobConf));
		}
		return jSplits;
	}

	/*
	 * This method merges the input Conditions given at time of table creation
	 * and conditions passed with query and resets input condition variable in
	 * DBConfiguration
	 */
	public void setFilters(JobConf jobConf) {
		String filterConditions = jobConf
				.get(TableScanDesc.FILTER_TEXT_CONF_STR);
		String condition = jobConf
				.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY);
		if (filterConditions != null && condition != null) {
			condition = condition.concat(" AND ");
			condition = condition.concat(filterConditions);
		} else if (filterConditions != null) {
			condition = filterConditions;
		}
		LOG.info("FilterPushDown Conditions: " + condition);
		if (condition != null) {
			jobConf.set(DBConfiguration.INPUT_CONDITIONS_PROPERTY, condition);
		}
	}
}
