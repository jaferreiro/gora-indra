package org.apache.gora.cascading;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.apache.gora.cascading.tap.local.GoraLocalScheme;
import org.apache.gora.cascading.tap.local.GoraLocalTap;
import org.apache.gora.cascading.test.storage.TestRow;
import org.apache.gora.cascading.test.storage.TestRowDest;
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
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class LocalCopyTest {

    public static final Logger LOG = LoggerFactory.getLogger(LocalCopyTest.class);

    /** The configuration */
    protected static Configuration configuration;

    private static HBaseTestingUtility utility;

    public LocalCopyTest() {
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        System.setProperty(HBaseTestingUtility.TEST_DIRECTORY_KEY, "build/test-data");
        Configuration localExecutionConfiguration = new Configuration() ;
        utility = new HBaseTestingUtility(localExecutionConfiguration);
        utility.startMiniCluster(1);
        configuration = utility.getConfiguration();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        utility.shutdownMiniCluster();
    }

    protected static void deleteTable(Configuration configuration, String tableName) throws IOException {
        HBaseAdmin hbase = new HBaseAdmin(configuration);
        if (hbase.tableExists(Bytes.toBytes(tableName))) {
            hbase.disableTable(Bytes.toBytes(tableName));
            hbase.deleteTable(Bytes.toBytes(tableName));
        }
    }

    protected void verifySink(Flow<?> flow, int expects) throws Exception {

        DataStore<String, TestRowDest> dataStore = DataStoreFactory.getDataStore(String.class, TestRowDest.class, LocalCopyTest.configuration);
        org.apache.gora.query.Result<String,TestRowDest> resultDest = dataStore.newQuery().execute() ;
        
        int numResults = 0 ;
        while (resultDest.next()) {
            numResults++ ;
            LOG.debug("key = {}",resultDest.getKey().toString()) ;
            if (resultDest.getKey().equals("1")) {
                TestRowDest persistent = resultDest.get() ;
                assertEquals(2, persistent.getDefaultLong1()) ;
                assertEquals("a", persistent.getDefaultStringEmpty().toString()) ;
                assertEquals(10, persistent.getColumnLong().longValue()) ;
            }
        }

        assertEquals("Wrong number of records written", 2, numResults) ;

    }

    protected void verifyIncrementField(Flow<?> flow, int expects) throws Exception {

        DataStore<String, TestRowDest> dataStore = DataStoreFactory.getDataStore(String.class, TestRowDest.class, LocalCopyTest.configuration);
        org.apache.gora.query.Result<String,TestRowDest> resultDest = dataStore.newQuery().execute() ;
        
        int numResults = 0 ;
        while (resultDest.next()) {
            numResults++ ;
            LOG.debug("key = {}",resultDest.getKey().toString()) ;
            if (resultDest.getKey().equals("1")) {
                TestRowDest persistent = resultDest.get() ;
                assertEquals(2, persistent.getDefaultLong1()) ;
                assertEquals("a", persistent.getDefaultStringEmpty().toString()) ;
                assertEquals(10, persistent.getColumnLong().longValue()) ;
                assertEquals(67, persistent.getUnionLong().longValue()) ;
            }
        }

        assertEquals("Wrong number of records written", 2, numResults) ;

    }
    
    protected void verify(String tableName, String family, String charCol, int expected) throws IOException {
        byte[] familyBytes = Bytes.toBytes(family);
        byte[] qulifierBytes = Bytes.toBytes(charCol);

        HTable table = new HTable(configuration, tableName);
        ResultScanner scanner = table.getScanner(familyBytes, qulifierBytes);

        int count = 0;
        for (Result rowResult : scanner) {
            count++;
            LOG.info("rowResult = {}", rowResult.getValue(familyBytes, qulifierBytes));
        }

        scanner.close();
        table.close();

        assertEquals("wrong number of rows", expected, count);
    }

    @Before
    public void before() throws GoraException {
        DataStore<String, TestRow> dataStore = DataStoreFactory.getDataStore(String.class, TestRow.class, LocalCopyTest.configuration);
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
    public void copiar() throws Exception {

        Map<Object, Object> properties = ConfigurationUtil.toRawMap(LocalCopyTest.configuration) ;
        AppProps.setApplicationJarClass(properties, LocalCopyTest.class);
        LocalCopyTest.configuration = ConfigurationUtil.toConfiguration(properties) ;
        
        //deleteTable(configuration, "test");

        GoraLocalScheme esquema = new GoraLocalScheme() ;
        
        //esquema.setQueryStartKey(queryStartKey) ;
        
        Tap<?, ?, ?> origen = new GoraLocalTap(String.class, TestRow.class, esquema, LocalCopyTest.configuration) ;
        Tap<?, ?, ?> destino = new GoraLocalTap(String.class, TestRowDest.class, esquema, LocalCopyTest.configuration) ;

        Pipe copyPipe = new Each("read", new Identity());
        FlowConnector flowConnector = new LocalFlowConnector(properties) ;
        Flow<?> copyFlow = flowConnector.connect(origen, destino, copyPipe);

        copyFlow.complete();
        
        verifySink(copyFlow, 2);

    }

    @Test
    public void incrementField() throws Exception {

        Map<Object, Object> properties = ConfigurationUtil.toRawMap(LocalCopyTest.configuration) ;
        AppProps.setApplicationJarClass(properties, LocalCopyTest.class);
        LocalCopyTest.configuration = ConfigurationUtil.toConfiguration(properties) ;
        
        GoraLocalScheme esquema = new GoraLocalScheme() ;
        Tap<?, ?, ?> origen = new GoraLocalTap(String.class, TestRow.class, esquema) ;
        Tap<?, ?, ?> destino = new GoraLocalTap(String.class, TestRowDest.class, esquema) ;

        Pipe insertPipe = new Each("insert", new Fields("defaultLong1"), new IncrementField(new Fields("unionLong")), Fields.REPLACE) ;
        FlowConnector flowConnector = new LocalFlowConnector(properties) ;
        Flow copyFlow = flowConnector.connect(origen, destino, insertPipe);

        copyFlow.complete();

        verifyIncrementField(copyFlow, 2);

    }    
}
