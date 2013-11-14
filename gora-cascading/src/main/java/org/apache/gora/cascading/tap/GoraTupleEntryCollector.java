package org.apache.gora.cascading.tap;

import java.io.IOException;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

public class GoraTupleEntryCollector extends TupleEntryCollector {

    public GoraTupleEntryCollector() {
        // TODO Auto-generated constructor stub
    }

    public GoraTupleEntryCollector(Fields declared) {
        super(declared);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void collect(TupleEntry tupleEntry) throws IOException {
        // TODO Auto-generated method stub

    }

}
