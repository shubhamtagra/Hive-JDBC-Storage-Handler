package org.apache.hadoop.hive.jdbc.storagehandler;

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
import java.sql.*;


/**
 * Serialize/Deserialize a tuple.
 */
public class JdbcSerDe implements SerDe {
    private static final Log LOG = LogFactory.getLog(JdbcSerDe.class);

    private DbRecordWritable cachedWritable;

    private int fieldCount;

    private StructObjectInspector objectInspector;
    private List<Object> deserializeCache;

    public JdbcSerDe() {
    }

    

    @Override
    public void initialize(Configuration sysConf, Properties tblProps)
            throws SerDeException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("tblProps: " + tblProps);
        }
        Connection dbConnection = null;
        StringBuilder colNames = new StringBuilder();
        StringBuilder colTypeNames = new StringBuilder();
        try{

            dbConnection = JdbcSerDeHelper.getConnection(tblProps);
            String query = JdbcSerDeHelper.getSelectQuery(tblProps);
            Statement st = dbConnection.createStatement();
            ResultSet rs = st.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            int i =0;
            for (i = 1; i < columnsNumber; i++ ) {
              String colName = rsmd.getColumnName(i);
              String colType = rsmd.getColumnTypeName(i);
              colNames.append(colName+",");
              colTypeNames.append(JdbcSerDeHelper.sqlToHiveColumnTypeNames(colType)+":");
            }
            colNames.append(rsmd.getColumnName(i));
            colTypeNames.append(rsmd.getColumnTypeName(i));
            LOG.info("Column Names >>" + colNames.toString());
            LOG.info("Column Type Names >>" + colTypeNames.toString());

            JdbcSerDeHelper.columnNames = colNames.toString();
            JdbcSerDeHelper.columnTypeNames = colTypeNames.toString();

            tblProps
               .setProperty(Constants.LIST_COLUMNS, colNames.toString());
            tblProps
               .setProperty(Constants.LIST_COLUMN_TYPES, colTypeNames.toString());
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        String columnNameProperty = tblProps
                .getProperty(Constants.LIST_COLUMNS);
        String columnTypeProperty = tblProps
               .getProperty(Constants.LIST_COLUMN_TYPES);
        List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
        String[] columnTypes = columnTypeProperty.split(":");
        assert (columnTypes.length == columnNames.size()) : "columnNames: "
                + columnNames + ", columnTypes: "
                + Arrays.toString(columnTypes);

        int[] types = HiveJdbcBridgeUtils.hiveTypesToSqlTypes(columnTypes);
        this.cachedWritable = new DbRecordWritable(types);
        this.fieldCount = types.length;

        final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(
                columnTypes.length);
        for (int i = 0; i < types.length; i++) {
            ObjectInspector oi = HiveJdbcBridgeUtils.getObjectInspector(
                    types[i], columnTypes[i]);
            fieldOIs.add(oi);
        }
        this.objectInspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, fieldOIs);
        this.deserializeCache = new ArrayList<Object>(columnTypes.length);
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return DbRecordWritable.class;
    }

    /*
     * This method takes an object representing a row of data from Hive, and
     * uses the ObjectInspector to get the data for each column and serialize.
     */
    @Override
    public DbRecordWritable serialize(Object row, ObjectInspector inspector)
            throws SerDeException {
        final StructObjectInspector structInspector = (StructObjectInspector) inspector;
        final List<? extends StructField> fields = structInspector
                .getAllStructFieldRefs();
        if (fields.size() != fieldCount) {
            throw new SerDeException(String.format(
                    "Required %d columns, received %d.", fieldCount,
                    fields.size()));
        }

        cachedWritable.clear();

        for (int i = 0; i < fieldCount; i++) {
            StructField structField = fields.get(i);
            if (structField != null) {
                Object field = structInspector.getStructFieldData(row,
                        structField);
                ObjectInspector fieldOI = structField.getFieldObjectInspector();
                Object javaObject = HiveJdbcBridgeUtils.deparseObject(field,
                        fieldOI);
                cachedWritable.set(i, javaObject);
            }
        }

        return cachedWritable;
    }

    /**
     * This method does the work of deserializing a record into Java objects
     * that Hive can work with via the ObjectInspector interface.
     */
    @Override
    public Object deserialize(Writable record) throws SerDeException {
        if (!(record instanceof DbRecordWritable)) {
            throw new SerDeException("Expected DbTupleWritable, received "
                    + record.getClass().getName());
        }
        DbRecordWritable tuple = (DbRecordWritable) record;
        deserializeCache.clear();

        for (int i = 0; i < fieldCount; i++) {
            Object o = tuple.get(i);
            deserializeCache.add(o);
        }

        return deserializeCache;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // TODO Auto-generated method stub
        return null;
    }
}
