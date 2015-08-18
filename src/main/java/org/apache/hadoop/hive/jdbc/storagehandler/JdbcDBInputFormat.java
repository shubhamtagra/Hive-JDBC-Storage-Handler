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
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.MySQLDBRecordReader;
import org.apache.hadoop.mapreduce.lib.db.OracleDBRecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.hive.wrapper.InputFormatWrapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;

public class JdbcDBInputFormat<T extends DBWritable> 
   extends DBInputFormat {
    private static final Log LOG = LogFactory.getLog(JdbcDBInputFormat.class);
    private String conditions;

    private String tableName;

    private Connection connection;

    private String[] fieldNames;

    private DBConfiguration dbConf;
    
    public JdbcDBInputFormat(){
    
    }
    public void setConfLocal(){

        dbConf = super.getDBConf();
        tableName = dbConf.getInputTableName();
        fieldNames = dbConf.getInputFieldNames();
        conditions = dbConf.getInputConditions();
        try{

            connection = super.getConnection();
        }
        catch (Exception ex) {
          throw new RuntimeException(ex);
       }
    }
    
    @Override
    public RecordReader<LongWritable, T> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {  
        setConfLocal();
        Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
        try{
            LOG.info("DB Type >> " + super.getDBProductName());
            String dbProductName = super.getDBProductName().toUpperCase();
            Configuration conf = dbConf.getConf();
            if (dbProductName.startsWith("MICROSOFT SQL SERVER")) {
                
                return new MicrosoftDBRecordReader<T>((DBInputFormat.DBInputSplit)split, inputClass,
                dbConf.getConf(), connection, super.getDBConf(), conditions, fieldNames,
                tableName);
            
            } else if (dbProductName.startsWith("ORACLE")) {
                // use Oracle-specific db reader.
                return new OracleDBRecordReader<T>((DBInputFormat.DBInputSplit) split, inputClass,
                        conf, getConnection(), getDBConf(), conditions, fieldNames,
                        tableName);
            } else if (dbProductName.startsWith("MYSQL")) {
                // use MySQL-specific db reader.
                return new MySQLDBRecordReader<T>((DBInputFormat.DBInputSplit) split, inputClass,
                        conf, getConnection(), getDBConf(), conditions, fieldNames,
                        tableName);
            } else {
                // Generic reader.
                return new GenericDBRecordReader<>((DBInputFormat.DBInputSplit) split, inputClass,
                        conf, getConnection(), getDBConf(), conditions, fieldNames,
                        tableName);
            }
        }catch (SQLException ex) {
           throw new IOException(ex.getMessage());
        }
    }


}