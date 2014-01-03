package org.apache.gora.cascading.tap;

import java.io.IOException;

import org.apache.gora.store.DataStore;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

public class GoraTupleEntryCollector extends TupleEntryCollector {

    private GoraScheme scheme ;
    
    public GoraTupleEntryCollector(GoraScheme scheme) {
        this.scheme = scheme ;
    }

    public GoraTupleEntryCollector(Fields declared) {
        super(declared);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void collect(TupleEntry tupleEntry) throws IOException {
// TODO IMPLEMENTAR
        return ;
    }

}
