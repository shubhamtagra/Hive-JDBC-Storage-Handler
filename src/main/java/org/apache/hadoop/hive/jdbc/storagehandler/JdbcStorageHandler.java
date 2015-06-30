package org.apache.hadoop.hive.jdbc.storagehandler;

import java.util.Map;
import java.util.Properties;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

/**
 * -- required settings
 * set mapred.jdbc.driver.class=..;
 * set mapred.jdbc.url=..;
 * 
 * -- optional settings
 * set mapred.jdbc.username=..;
 * set mapred.jdbc.password=..;
 * 
 * @see org.apache.hadoop.mapred.lib.db.DBConfiguration
 * @see org.apache.hadoop.mapred.lib.db.DBInputFormat
 * @see org.apache.hadoop.mapred.lib.db.DBOutputFormat
 */
public class JdbcStorageHandler extends DefaultStorageHandler implements HiveStoragePredicateHandler {
    private static final Log LOG = LogFactory.getLog(JdbcStorageHandler.class);

    private Configuration conf;

    public JdbcStorageHandler() {}

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new JDBCHook();
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    private void configureJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("tabelDesc: " + tableDesc);
            LOG.debug("jobProperties: " + jobProperties);
        }

        String tblName = tableDesc.getTableName();
        Properties tblProps = tableDesc.getProperties();
        String columnNames = tblProps.getProperty(Constants.LIST_COLUMNS);
        jobProperties.put(DBConfiguration.INPUT_CLASS_PROPERTY, DbRecordWritable.class.getName());
        jobProperties.put(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tblName);
        jobProperties.put(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tblName);
        jobProperties.put(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, columnNames);
        jobProperties.put(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, columnNames);

        for(String key : tblProps.stringPropertyNames()) {
            if(key.startsWith("mapred.jdbc.")) {
                String value = tblProps.getProperty(key);
                jobProperties.put(key, value);
            }
        }

    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return new DefaultHiveAuthorizationProvider();
    }

    /**
     * @see org.apache.hadoop.hive.ql.exec.FetchOperator#getInputFormatFromCache
     */
    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends InputFormat<? extends WritableComparable, ? extends Writable>> getInputFormatClass() {
        return JdbcInputFormat.class;
    }

    /*@SuppressWarnings("rawtypes")
    @Override
    public Class<? extends HiveOutputFormat> getOutputFormatClass() {
        // NOTE that must return subclass of HiveOutputFormat
        return null;
    }*/

    @Override
    public Class<? extends HiveOutputFormat> getOutputFormatClass() {
        // NOTE that must return subclass of HiveOutputFormat
        return JdbcOutputFormat.class;
    }
    
    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return JdbcSerDe.class;
    }

    /**
     * @see DBConfiguration#INPUT_CONDITIONS_PROPERTY
     */

    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf, 
        Deserializer deserializer, ExprNodeDesc predicate) {
        // TODO Auto-generated method stub
       /* LOG.info("Prediactes in ----->  "+ predicate.getExprString());
        DecomposedPredicate dp = new DecomposedPredicate() ;
        dp.pushedPredicate =(ExprNodeGenericFuncDesc) predicate;
        return dp;*/
        IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd");


        String keyColType = 
            jobConf.get(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS);

        //LOG.info("COl names are : "+ keyColType);
        StringTokenizer st = new StringTokenizer(keyColType,",");
        while (st.hasMoreTokens()){
            String columnName = (String) st.nextToken();
            //System.out.println("Next token : " + columnName);
            analyzer.allowColumnName(columnName);  
            LOG.info(columnName);      
        }

        List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(predicate, searchConditions);
       
         for(IndexSearchCondition e: searchConditions){
            LOG.info("COnditions fetched are: " + e.toString());
         }
         
        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        decomposedPredicate.pushedPredicate = (ExprNodeGenericFuncDesc)analyzer.translateSearchConditions(searchConditions);
        decomposedPredicate.residualPredicate = (ExprNodeGenericFuncDesc)residualPredicate;
        if(decomposedPredicate.pushedPredicate != null)
            LOG.info("Predicates pushed: "+ decomposedPredicate.pushedPredicate.getExprString());
        if(decomposedPredicate.residualPredicate != null)
            LOG.info("Predicates not Pushed: "+ decomposedPredicate.residualPredicate.getExprString());
        return decomposedPredicate;
    }

    private static class JDBCHook implements HiveMetaHook {

        @Override
        public void preCreateTable(Table tbl) throws MetaException {
            if(!MetaStoreUtils.isExternalTable(tbl)) {
                throw new MetaException("Table must be external.");
            }
            // TODO Auto-generated method stub
        }

        @Override
        public void commitCreateTable(Table tbl) throws MetaException {
            // TODO Auto-generated method stub
        }

        @Override
        public void preDropTable(Table tbl) throws MetaException {
            // nothing to do            
        }

        @Override
        public void commitDropTable(Table tbl, boolean deleteData) throws MetaException {
            // TODO Auto-generated method stub
        }

        @Override
        public void rollbackCreateTable(Table tbl) throws MetaException {
            // TODO Auto-generated method stub
        }

        @Override
        public void rollbackDropTable(Table tbl) throws MetaException {
            // TODO Auto-generated method stub
        }

    }

}
