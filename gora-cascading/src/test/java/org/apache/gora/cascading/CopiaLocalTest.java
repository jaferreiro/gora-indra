package org.apache.gora.cascading;

import org.apache.gora.cascading.tap.GoraScheme;
import org.apache.gora.cascading.tap.GoraTap;
import org.apache.gora.cascading.test.storage.TestRow;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

public class CopiaLocalTest {

    public CopiaLocalTest() {
    }
    

    @Before
    public void before() throws GoraException {
        DataStore<String,TestRow> dataStore = DataStoreFactory.getDataStore(String.class, TestRow.class, new Configuration()) ;
        TestRow t = dataStore.newPersistent() ;
        t.setDefaultLong1(2) ; // Campo obligatorio
        t.setDefaultStringEmpty("a") ; //Campo obligatorio
        t.setColumnLong((long) 10) ;
        t.setDefaultLong1(2) ;
        dataStore.put("1", t) ;
        
        t.setDefaultLong1(3) ; // Campo obligatorio
        t.setDefaultStringEmpty("b") ; //Campo obligatorio
        t.setColumnLong((long) 5) ;
        t.setDefaultLong1(7) ;
        dataStore.put("2", t) ;
        
        dataStore.flush() ;
    }

    @Test
    public void copiar() {
        GoraScheme esquema = new GoraScheme(String.class, TestRow.class) ;
        //esquema.setQuery(query) ;
        Tap<?, ?, ?> origen = new GoraTap(esquema) ;
        Tap<?, ?, ?> destino = new GoraTap(esquema) ;
        
        Pipe pipe = new Pipe("copia sobre sí mismo") ;
        FlowDef flow = FlowDef.flowDef()
            .addSource(pipe, origen)
            .addTail(pipe)
            .addSink(pipe, destino) ;
        
        new LocalFlowConnector().connect(flow).complete() ;
        
    }
}
