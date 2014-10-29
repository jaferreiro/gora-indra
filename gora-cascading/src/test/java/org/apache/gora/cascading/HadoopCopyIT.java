package org.apache.gora.cascading;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.gora.cascading.tap.hadoop.GoraScheme;
import org.apache.gora.cascading.tap.hadoop.GoraTap;
import org.apache.gora.cascading.test.storage.TestRow;
import org.apache.gora.cascading.util.ConfigurationUtil;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

public class HadoopCopyIT {

    public static final Logger LOG = LoggerFactory.getLogger(HadoopCopyIT.class);

    /** The configuration */
    protected static Configuration configuration;

    private static HBaseTestingUtility utility;
    
    public HadoopCopyIT() {
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        System.setProperty(HBaseTestingUtility.TEST_DIRECTORY_KEY, "build/test-data");
        Configuration localExecutionConfiguration = new Configuration() ;
        localExecutionConfiguration.setStrings("hadoop.log.dir", localExecutionConfiguration.get("hadoop.tmp.dir")) ;
        utility = new HBaseTestingUtility(localExecutionConfiguration);
        utility.startMiniCluster(1);
        utility.startMiniMapReduceCluster(1) ;
        configuration = utility.getConfiguration();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        utility.shutdownMiniCluster();
    }
    
    protected static void deleteTable(String tableName) throws IOException {
        HBaseAdmin hbase = new HBaseAdmin(new Configuration());
        if (hbase.tableExists(Bytes.toBytes(tableName))) {
            hbase.disableTable(Bytes.toBytes(tableName));
            hbase.deleteTable(Bytes.toBytes(tableName));
        }
    }

    protected void verifySink(Flow flow, int expects) throws IOException {
        int count = 0;

        TupleEntryIterator iterator = flow.openSink();

        while (iterator.hasNext()) {
            count++;
            LOG.debug("iterator.next() = {}", iterator.next());
        }

        iterator.close();

        Assert.assertEquals("wrong number of values in " + flow.getSink().toString(), expects, count);
    }

    protected void verify(String tableName, String family, String charCol, int expected) throws IOException {
        byte[] familyBytes = Bytes.toBytes(family);
        byte[] qulifierBytes = Bytes.toBytes(charCol);

        HTable table = new HTable(new Configuration(), tableName);
        ResultScanner scanner = table.getScanner(familyBytes, qulifierBytes);

        int count = 0;
        for (Result rowResult : scanner) {
            count++;
            LOG.debug("rowResult = {}", rowResult.getValue(familyBytes, qulifierBytes));
        }

        scanner.close();
        table.close();

        Assert.assertEquals("wrong number of rows", expected, count);
    }

    @Before
    public void before() throws GoraException {
        DataStore<String, TestRow> dataStore = DataStoreFactory.getDataStore(String.class, TestRow.class, HadoopCopyIT.configuration);
        TestRow t = dataStore.newPersistent();
        t.setDefaultLong1(2); // Campo obligatorio
        t.setDefaultStringEmpty("a"); //Campo obligatorio
        t.setColumnLong((long) 10);
        t.setDefaultLong1(2);
        dataStore.put("1", t);

        t.setDefaultLong1(3); // Campo obligatorio
        t.setDefaultStringEmpty("b"); //Campo obligatorio
        t.setColumnLong((long) 5);
        t.setDefaultLong1(7);
        dataStore.put("2", t);

        dataStore.flush();
    }

    @Test
    public void identityCopy() throws Exception {

        Properties properties = ConfigurationUtil.toProperties(HadoopCopyIT.configuration) ;
        AppProps.setApplicationJarPath(properties, "target/gora-cascading-0.4-indra-SNAPSHOT-test-jar-with-dependencies.jar") ;
        HadoopCopyIT.configuration = ConfigurationUtil.toConfiguration(properties) ;
        
        GoraScheme esquema = new GoraScheme() ;
        esquema.setSourceAsPersistent(true) ;
        esquema.setSinkAsPersistent(true) ;
        Tap<?, ?, ?> origen = new GoraTap(String.class, TestRow.class, esquema) ;
        Tap<?, ?, ?> destino = new GoraTap(String.class, TestRow.class, esquema) ;

        Pipe copyPipe = new Each("read", new Identity());
        FlowConnector flowConnector = new HadoopFlowConnector(properties) ;
        Flow copyFlow = flowConnector.connect(origen, destino, copyPipe);

        copyFlow.complete();

        verifySink(copyFlow, 2);
        verifyValuesCopy() ;

    }

    private void verifyValuesCopy() throws IOException, Exception {
        DataStore<String, TestRow> dataStore = DataStoreFactory.getDataStore(String.class, TestRow.class, HadoopCopyIT.configuration);
        org.apache.gora.query.Result<String,TestRow> resultDest = dataStore.newQuery().execute() ;
        
        int numResults = 0 ;
        while (resultDest.next()) {
            numResults++ ;
            LOG.debug("key = {}",resultDest.getKey().toString()) ;
            if (resultDest.getKey().equals("1")) {
                TestRow persistent = resultDest.get() ;
                assertEquals(2, persistent.getDefaultLong1()) ;
                assertEquals("a", persistent.getDefaultStringEmpty().toString()) ;
                assertEquals(10, persistent.getColumnLong().longValue()) ;
            }
        }

        assertEquals("Wrong number of records written", 2, numResults) ;
        
    }
    
    @Test
    public void incrementField() throws Exception {

        Properties properties = ConfigurationUtil.toProperties(HadoopCopyIT.configuration) ;
        AppProps.setApplicationJarPath(properties, "target/gora-cascading-0.4-indra-SNAPSHOT-test-jar-with-dependencies.jar") ;
        HadoopCopyIT.configuration = ConfigurationUtil.toConfiguration(properties) ;
        
        GoraScheme esquema = new GoraScheme() ;
        Tap<?, ?, ?> origen = new GoraTap(String.class, TestRow.class, esquema) ;
        Tap<?, ?, ?> destino = new GoraTap(String.class, TestRow.class, esquema) ;

        Pipe insertPipe = new Each("insert", new Fields("defaultLong1"), new IncrementField(new Fields("unionLong")), Fields.REPLACE) ;
        FlowConnector flowConnector = new HadoopFlowConnector(properties) ;
        Flow copyFlow = flowConnector.connect(origen, destino, insertPipe);

        copyFlow.complete();

        verifySink(copyFlow, 2);
        verifyValuesIncrement() ;
    }

    private void verifyValuesIncrement() throws IOException, Exception {
        DataStore<String, TestRow> dataStore = DataStoreFactory.getDataStore(String.class, TestRow.class, HadoopCopyIT.configuration);
        org.apache.gora.query.Result<String,TestRow> resultDest = dataStore.newQuery().execute() ;
        
        int numResults = 0 ;
        while (resultDest.next()) {
            numResults++ ;
            LOG.debug("key = {}",resultDest.getKey().toString()) ;
            if (resultDest.getKey().equals("1")) {
                TestRow persistent = resultDest.get() ;
                assertEquals(2, persistent.getDefaultLong1()) ;
                assertEquals("a", persistent.getDefaultStringEmpty().toString()) ;
                assertEquals(10, persistent.getColumnLong().longValue()) ;
                assertEquals(67, persistent.getUnionLong().longValue()) ;
            }
        }

        assertEquals("Wrong number of records written", 2, numResults) ;
        
    }
    
}