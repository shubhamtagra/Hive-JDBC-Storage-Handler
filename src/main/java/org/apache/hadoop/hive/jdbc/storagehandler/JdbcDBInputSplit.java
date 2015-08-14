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

import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit;
import java.io.IOException;

public class JdbcDBInputSplit extends org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit {
    private static final Log LOG = LogFactory.getLog(JdbcDBInputSplit.class);
	  private long start = 0;
	  private long length = 0;
    private long end = 0;
    public JdbcDBInputSplit() {
     
    }
    public JdbcDBInputSplit(long index) {
      super(index,index+1);
      this.start = index;
      this.end = index+1;
    }
    @Override
    public long getLength() throws IOException {
    	LOG.info("end index and length is: "+ this.end + " " + (this.end - this.start));
    	return this.end - this.start;
    }

    @Override
    public long getStart(){
    	LOG.info("Start index is: "+ this.start);
    	return this.start;
    }

    public void setStart(long chunkSize){
    	
    	this.start *=  chunkSize;
    }  

    public void setEnd(long chunkSize){
    		this.end *= chunkSize;
    
    }
 
    public void readFields(DataInput input) throws IOException {
      this.start = input.readLong();
      this.end = input.readLong();
     
    }

    /** {@inheritDoc} */
 
    public void write(DataOutput output) throws IOException {
      output.writeLong(this.start);
      output.writeLong(this.end);
    }
}