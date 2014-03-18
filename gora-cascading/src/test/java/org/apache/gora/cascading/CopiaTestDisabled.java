package org.apache.gora.cascading;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.gora.cascading.tap.local.GoraLocalScheme;
import org.apache.gora.cascading.tap.local.GoraLocalTap;
import org.apache.gora.cascading.test.storage.TestRow;
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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryIterator;

import com.google.common.collect.Maps;

public class CopiaTestDisabled {

    /** The configuration. */
    protected static Configuration     configuration;

    private static HBaseTestingUtility utility;

    public CopiaTestDisabled() {
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        System.setProperty(HBaseTestingUtility.TEST_DIRECTORY_KEY, "build/test-data");
        utility = new HBaseTestingUtility();
        utility.startMiniCluster(1);
        configuration = utility.getConfiguration();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        utility.shutdownMiniCluster();
    }

    public FlowConnector createHadoopFlowConnector() {
        return createHadoopFlowConnector(Maps.newHashMap());
    }

    public FlowConnector createHadoopFlowConnector(Map<Object, Object> props) {
        Map<Object, Object> finalProperties = Maps.newHashMap(props);
        //finalProperties.put(HConstants.ZOOKEEPER_CLIENT_PORT, utility.getZkCluster().getClientPort());

        return new HadoopFlowConnector(finalProperties);
    }

    protected static void deleteTable(Configuration configuration, String tableName) throws IOException {
        HBaseAdmin hbase = new HBaseAdmin(configuration);
        if (hbase.tableExists(Bytes.toBytes(tableName))) {
            hbase.disableTable(Bytes.toBytes(tableName));
            hbase.deleteTable(Bytes.toBytes(tableName));
        }
        //hbase.close();
    }

    protected void verifySink(Flow flow, int expects) throws IOException {
        int count = 0;

        TupleEntryIterator iterator = flow.openSink();

        while (iterator.hasNext()) {
            count++;
            System.out.println("iterator.next() = " + iterator.next());
        }

        iterator.close();

        assertEquals("wrong number of values in " + flow.getSink().toString(), expects, count);
    }

    protected void verify(String tableName, String family, String charCol, int expected) throws IOException {
        byte[] familyBytes = Bytes.toBytes(family);
        byte[] qulifierBytes = Bytes.toBytes(charCol);

        HTable table = new HTable(configuration, tableName);
        ResultScanner scanner = table.getScanner(familyBytes, qulifierBytes);

        int count = 0;
        for (Result rowResult : scanner) {
            count++;
            System.out.println("rowResult = " + rowResult.getValue(familyBytes, qulifierBytes));
        }

        scanner.close();
        table.close();

        assertEquals("wrong number of rows", expected, count);
    }

    @Before
    public void before() throws GoraException {
        DataStore<String, TestRow> dataStore = DataStoreFactory.getDataStore(String.class, TestRow.class, new Configuration());
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
    public void copiar() throws IOException {

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, CopiaTestDisabled.class);

        deleteTable(configuration, "test");

        GoraLocalScheme esquema = new GoraLocalScheme() ;
        //esquema.setQuery(query) ;
        Tap<?, ?, ?> origen = new GoraLocalTap(String.class, TestRow.class, esquema) ;
        Tap<?, ?, ?> destino = new GoraLocalTap(String.class, TestRow.class, esquema) ;

        Pipe copyPipe = new Each("read", new Identity());
        FlowConnector flowConnector = new LocalFlowConnector() ;
        Flow copyFlow = flowConnector.connect(origen, destino, copyPipe);

        copyFlow.complete();

        verifySink(copyFlow, 5);

    }
}
